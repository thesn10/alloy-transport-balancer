//! Proactive JSON-RPC admission over rolling windows: per-endpoint request
//! rates counted in calls, and shared [`ComputeUnitBudget`](crate::ComputeUnitBudget)s
//! counted in compute units.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

pub(crate) const REQUEST_RATE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LimitKind {
    RequestRate,
    ComputeUnits,
}

impl LimitKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RequestRate => "request rate",
            Self::ComputeUnits => "compute-unit budget",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestRateReservation {
    Admitted,
    RetryAfter(Duration),
    PacketTooLarge,
}

#[derive(Debug)]
struct RequestRateWindow {
    admitted: VecDeque<(Instant, u32)>,
    used: u32,
}

#[derive(Debug)]
pub(crate) struct RequestRateLimiter {
    kind: LimitKind,
    limit: u32,
    period: Duration,
    window: Mutex<RequestRateWindow>,
    total_admitted_units: AtomicU64,
    total_admitted_rpc_calls: AtomicU64,
    total_capacity_rollovers: AtomicU64,
    total_capacity_waits: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequestRateSnapshot {
    pub(crate) limit: u32,
    pub(crate) used: u32,
    pub(crate) remaining: u32,
    pub(crate) total_admitted_units: u64,
    pub(crate) total_admitted_rpc_calls: u64,
    pub(crate) total_capacity_rollovers: u64,
    pub(crate) total_capacity_waits: u64,
}

/// Units one packet needs from one limiter.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Demand<'a> {
    pub(crate) limiter: &'a Arc<RequestRateLimiter>,
    pub(crate) units: u32,
}

/// The first limiter that could not admit a packet's demands.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Denial<'a> {
    pub(crate) limiter: &'a Arc<RequestRateLimiter>,
    pub(crate) units: u32,
    pub(crate) reservation: RequestRateReservation,
}

/// Reports the first demand its limiter cannot admit now, without reserving.
pub(crate) fn check_all<'a>(demands: &[Demand<'a>]) -> Option<Denial<'a>> {
    let now = Instant::now();
    demands.iter().find_map(|demand| {
        let mut window = demand.limiter.lock_window();
        demand.limiter.prune(&mut window, now);
        demand.limiter.deny(&window, now, *demand)
    })
}

/// Reserves every demand of a packet of `calls` JSON-RPC calls, or none of them.
pub(crate) fn try_reserve_all<'a>(demands: &[Demand<'a>], calls: u64) -> Option<Denial<'a>> {
    let now = Instant::now();
    // Callers list the endpoint's own limiter before the shared budget, which keeps the lock order acyclic.
    let mut windows: Vec<_> = demands
        .iter()
        .map(|demand| demand.limiter.lock_window())
        .collect();
    for (demand, window) in demands.iter().zip(windows.iter_mut()) {
        demand.limiter.prune(window, now);
    }
    if let Some(denial) = demands
        .iter()
        .zip(&windows)
        .find_map(|(demand, window)| demand.limiter.deny(window, now, *demand))
    {
        return Some(denial);
    }

    for (demand, window) in demands.iter().zip(windows.iter_mut()) {
        window.admitted.push_back((now, demand.units));
        window.used += demand.units;
        demand
            .limiter
            .total_admitted_units
            .fetch_add(u64::from(demand.units), Ordering::Relaxed);
        demand
            .limiter
            .total_admitted_rpc_calls
            .fetch_add(calls, Ordering::Relaxed);
    }
    None
}

#[derive(Debug, Default)]
pub(crate) struct RequestRateFallback {
    earliest_capacity: Option<(Duration, Arc<RequestRateLimiter>)>,
    denied: Vec<Arc<RequestRateLimiter>>,
    packets_too_large: usize,
}

impl RequestRateFallback {
    pub(crate) fn reject(&mut self, denial: &Denial<'_>) {
        self.denied.push(Arc::clone(denial.limiter));
        match denial.reservation {
            RequestRateReservation::Admitted => {}
            RequestRateReservation::RetryAfter(retry_after) => {
                if self
                    .earliest_capacity
                    .as_ref()
                    .is_none_or(|(current, _)| retry_after < *current)
                {
                    self.earliest_capacity = Some((retry_after, Arc::clone(denial.limiter)));
                }
            }
            RequestRateReservation::PacketTooLarge => {
                self.packets_too_large += 1;
            }
        }
    }

    pub(crate) fn record_rollovers(&mut self) {
        for limiter in self.denied.drain(..) {
            limiter.record_rollover();
        }
    }

    pub(crate) fn earliest_capacity(&self) -> Option<(Duration, Arc<RequestRateLimiter>)> {
        self.earliest_capacity.clone()
    }

    pub(crate) const fn packets_too_large(&self) -> usize {
        self.packets_too_large
    }
}

impl RequestRateLimiter {
    pub(crate) const fn new(kind: LimitKind, limit: u32, period: Duration) -> Self {
        Self {
            kind,
            limit,
            period,
            window: Mutex::new(RequestRateWindow {
                admitted: VecDeque::new(),
                used: 0,
            }),
            total_admitted_units: AtomicU64::new(0),
            total_admitted_rpc_calls: AtomicU64::new(0),
            total_capacity_rollovers: AtomicU64::new(0),
            total_capacity_waits: AtomicU64::new(0),
        }
    }

    pub(crate) const fn kind(&self) -> LimitKind {
        self.kind
    }

    pub(crate) const fn window(&self) -> Duration {
        self.period
    }

    fn lock_window(&self) -> MutexGuard<'_, RequestRateWindow> {
        self.window
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn deny<'a>(
        &self,
        window: &RequestRateWindow,
        now: Instant,
        demand: Demand<'a>,
    ) -> Option<Denial<'a>> {
        let reservation = self.evaluate(window, now, demand.units);
        (reservation != RequestRateReservation::Admitted).then_some(Denial {
            limiter: demand.limiter,
            units: demand.units,
            reservation,
        })
    }

    fn evaluate(
        &self,
        window: &RequestRateWindow,
        now: Instant,
        units: u32,
    ) -> RequestRateReservation {
        if units > self.limit {
            return RequestRateReservation::PacketTooLarge;
        }

        let available = self.limit.saturating_sub(window.used);
        if units <= available {
            return RequestRateReservation::Admitted;
        }

        let required = units - available;
        let mut expiring = 0_u32;
        let retry_after = window
            .admitted
            .iter()
            .find_map(|(admitted_at, admitted_units)| {
                expiring = expiring.saturating_add(*admitted_units);
                (expiring >= required)
                    .then(|| (*admitted_at + self.period).saturating_duration_since(now))
            })
            .unwrap_or(self.period);
        RequestRateReservation::RetryAfter(retry_after)
    }

    pub(crate) fn record_rollover(&self) {
        self.total_capacity_rollovers
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_wait(&self) {
        self.total_capacity_waits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RequestRateSnapshot {
        let now = Instant::now();
        let mut window = self.lock_window();
        self.prune(&mut window, now);
        RequestRateSnapshot {
            limit: self.limit,
            used: window.used,
            remaining: self.limit.saturating_sub(window.used),
            total_admitted_units: self.total_admitted_units.load(Ordering::Relaxed),
            total_admitted_rpc_calls: self.total_admitted_rpc_calls.load(Ordering::Relaxed),
            total_capacity_rollovers: self.total_capacity_rollovers.load(Ordering::Relaxed),
            total_capacity_waits: self.total_capacity_waits.load(Ordering::Relaxed),
        }
    }

    fn prune(&self, window: &mut RequestRateWindow, now: Instant) {
        while window
            .admitted
            .front()
            .is_some_and(|(admitted_at, _)| now.duration_since(*admitted_at) >= self.period)
        {
            if let Some((_, expired)) = window.admitted.pop_front() {
                window.used = window.used.saturating_sub(expired);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(kind: LimitKind, limit: u32, period: Duration) -> Arc<RequestRateLimiter> {
        Arc::new(RequestRateLimiter::new(kind, limit, period))
    }

    #[test]
    fn denied_demand_reserves_nothing_on_the_other_limiters() {
        let requests = limiter(LimitKind::RequestRate, 10, REQUEST_RATE_WINDOW);
        let budget = limiter(LimitKind::ComputeUnits, 5, REQUEST_RATE_WINDOW);
        let demands = [
            Demand {
                limiter: &requests,
                units: 1,
            },
            Demand {
                limiter: &budget,
                units: 6,
            },
        ];

        let denial = try_reserve_all(&demands, 1).expect("budget denies the packet");

        assert!(Arc::ptr_eq(denial.limiter, &budget));
        assert_eq!(denial.reservation, RequestRateReservation::PacketTooLarge);
        let requests = requests.snapshot();
        assert_eq!((requests.used, requests.total_admitted_rpc_calls), (0, 0));
    }

    #[test]
    fn admitted_demands_charge_every_limiter() {
        let requests = limiter(LimitKind::RequestRate, 10, REQUEST_RATE_WINDOW);
        let budget = limiter(LimitKind::ComputeUnits, 100, REQUEST_RATE_WINDOW);
        let demands = [
            Demand {
                limiter: &requests,
                units: 2,
            },
            Demand {
                limiter: &budget,
                units: 52,
            },
        ];

        assert!(try_reserve_all(&demands, 2).is_none());

        let requests = requests.snapshot();
        let budget = budget.snapshot();
        assert_eq!((requests.used, requests.total_admitted_rpc_calls), (2, 2));
        assert_eq!((budget.used, budget.total_admitted_units), (52, 52));
        assert_eq!(budget.total_admitted_rpc_calls, 2);
    }

    #[test]
    fn capacity_returns_after_the_configured_window() {
        let period = Duration::from_millis(50);
        let budget = limiter(LimitKind::ComputeUnits, 100, period);
        let demand = [Demand {
            limiter: &budget,
            units: 100,
        }];
        assert!(try_reserve_all(&demand, 1).is_none());

        let denial = check_all(&demand).expect("window is full");
        assert!(matches!(
            denial.reservation,
            RequestRateReservation::RetryAfter(retry_after) if retry_after <= period
        ));

        std::thread::sleep(period);
        assert!(check_all(&demand).is_none());
    }
}
