//! Compute-unit budgets: provider cost tables and the shared admission limit
//! that charges each call its compute units.

use crate::request_rate::{LimitKind, RequestRateLimiter, RequestRateSnapshot};
use std::{sync::Arc, time::Duration};

/// A provider's table of compute units per JSON-RPC method.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ComputeUnitCost {
    /// Alchemy's throughput compute units, which are what Alchemy meters
    /// against an account's CUPS limit.
    ///
    /// Methods missing from Alchemy's published table count as `eth_call`
    /// ([`ALCHEMY_UNLISTED_METHOD_UNITS`]).
    Alchemy,
}

/// Throughput compute units assumed for a method Alchemy does not list.
pub const ALCHEMY_UNLISTED_METHOD_UNITS: u32 = 26;

/// Alchemy evaluates CUPS over a 10-second rolling window: an account may
/// consume ten times its CUPS in any 10-second period.
pub const ALCHEMY_THROUGHPUT_WINDOW: Duration = Duration::from_secs(10);

impl ComputeUnitCost {
    /// Compute units one call of `method` consumes.
    pub fn units(self, method: &str) -> u32 {
        match self {
            Self::Alchemy => alchemy_throughput_units(method),
        }
    }

    /// Compute units a packet of calls to `methods` consumes, saturating at
    /// `u32::MAX`.
    pub fn packet_units<'a>(self, methods: impl IntoIterator<Item = &'a str>) -> u32 {
        let total = methods
            .into_iter()
            .map(|method| u64::from(self.units(method)))
            .sum::<u64>()
            .max(1);
        u32::try_from(total).unwrap_or(u32::MAX)
    }
}

/// A compute-unit-per-second budget shared by every endpoint and transport
/// that holds a clone, e.g. all endpoints of one Alchemy account.
///
/// Every call consumes its [`ComputeUnitCost`] units, a batch the sum of its
/// members. A packet is admitted only while the units admitted during the last
/// `window` stay within `units_per_second × window`.
#[derive(Clone, Debug)]
pub struct ComputeUnitBudget {
    cost: ComputeUnitCost,
    units_per_second: u32,
    limiter: Arc<RequestRateLimiter>,
}

/// Observable state of a [`ComputeUnitBudget`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ComputeUnitBudgetSnapshot {
    /// Configured sustained rate.
    pub units_per_second: u32,
    /// Rolling window the rate is evaluated over.
    pub window: Duration,
    /// Units admitted at most during one window.
    pub capacity: u32,
    /// Units admitted during the current window.
    pub used_in_window: u32,
    /// Units still available during the current window.
    pub remaining_in_window: u32,
    /// Total compute units admitted since construction.
    pub total_admitted_units: u64,
    /// Total JSON-RPC calls admitted since construction.
    pub total_admitted_rpc_calls: u64,
    /// Total packets redirected after an endpoint lacked budget.
    pub total_capacity_rollovers: u64,
    /// Total times callers waited for budget.
    pub total_capacity_waits: u64,
}

impl ComputeUnitBudget {
    /// A budget of `units_per_second` evaluated over `window`.
    ///
    /// # Panics
    ///
    /// Panics if `units_per_second` is zero or `window` is shorter than one
    /// millisecond.
    pub fn new(cost: ComputeUnitCost, units_per_second: u32, window: Duration) -> Self {
        assert!(units_per_second > 0, "compute-unit budget must be > 0");
        assert!(
            window >= Duration::from_millis(1),
            "compute-unit budget window must be at least 1 ms"
        );
        let capacity = u64::from(units_per_second) * window.as_millis() as u64 / 1_000;
        let capacity = u32::try_from(capacity.max(1)).unwrap_or(u32::MAX);
        Self {
            cost,
            units_per_second,
            limiter: Arc::new(RequestRateLimiter::new(
                LimitKind::ComputeUnits,
                capacity,
                window,
            )),
        }
    }

    /// An Alchemy account budget of `cups` compute units per second over
    /// [`ALCHEMY_THROUGHPUT_WINDOW`].
    ///
    /// # Panics
    ///
    /// Panics if `cups` is zero.
    pub fn alchemy(cups: u32) -> Self {
        Self::new(ComputeUnitCost::Alchemy, cups, ALCHEMY_THROUGHPUT_WINDOW)
    }

    /// The cost table this budget charges.
    pub const fn cost(&self) -> ComputeUnitCost {
        self.cost
    }

    /// Current usage and cumulative admission counters.
    pub fn snapshot(&self) -> ComputeUnitBudgetSnapshot {
        let RequestRateSnapshot {
            limit,
            used,
            remaining,
            total_admitted_units,
            total_admitted_rpc_calls,
            total_capacity_rollovers,
            total_capacity_waits,
        } = self.limiter.snapshot();
        ComputeUnitBudgetSnapshot {
            units_per_second: self.units_per_second,
            window: self.limiter.window(),
            capacity: limit,
            used_in_window: used,
            remaining_in_window: remaining,
            total_admitted_units,
            total_admitted_rpc_calls,
            total_capacity_rollovers,
            total_capacity_waits,
        }
    }

    pub(crate) const fn limiter(&self) -> &Arc<RequestRateLimiter> {
        &self.limiter
    }

    pub(crate) fn packet_units<'a>(&self, methods: impl IntoIterator<Item = &'a str>) -> u32 {
        self.cost.packet_units(methods)
    }

    #[cfg(test)]
    pub(crate) fn shares_limiter_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.limiter, &other.limiter)
    }
}

/// Alchemy's "Throughput CU" where its cost table lists one, otherwise its "CU",
/// for every JSON-RPC method an EVM endpoint serves, sorted by method.
const ALCHEMY_THROUGHPUT_UNITS: &[(&str, u32)] = &[
    ("alchemy_getAssetTransfers", 120),
    ("alchemy_getTokenAllowance", 20),
    ("alchemy_getTokenBalances", 20),
    ("alchemy_getTokenMetadata", 10),
    ("alchemy_getTransactionReceipts", 250),
    ("alchemy_requestFeePayer", 100),
    ("alchemy_requestGasAndPaymasterAndData", 125),
    ("alchemy_requestPaymasterAndData", 100),
    ("alchemy_requestPaymasterTokenQuote", 100),
    ("alchemy_simulateAssetChanges", 2500),
    ("alchemy_simulateAssetChangesBundle", 4500),
    ("alchemy_simulateExecution", 2500),
    ("alchemy_simulateExecutionBundle", 4500),
    ("alchemy_simulateUserOperationAssetChanges", 2500),
    ("arbtrace_block", 1000),
    ("arbtrace_call", 1000),
    ("arbtrace_callMany", 1000),
    ("arbtrace_filter", 1000),
    ("arbtrace_get", 1000),
    ("arbtrace_replayBlockTransactions", 3000),
    ("arbtrace_replayTransaction", 3000),
    ("arbtrace_transaction", 40),
    ("bor_getAuthor", 10),
    ("bor_getCurrentProposer", 10),
    ("bor_getCurrentValidators", 10),
    ("bor_getRootHash", 10),
    ("bor_getSignersAtHash", 10),
    ("chain_getFinalizedHead", 10),
    ("chain_getHeader", 10),
    ("citrea_getL2StatusHeightsByL1Height", 20),
    ("citrea_getLastCommittedL2Height", 20),
    ("citrea_getLastProvenL2Height", 20),
    ("citrea_sendRawDepositTransaction", 20),
    ("citrea_syncStatus", 20),
    ("debug_bundler_clearMempool", 10),
    ("debug_bundler_clearState", 10),
    ("debug_bundler_dumpMempool", 10),
    ("debug_bundler_dumpPaymasterBalances", 10),
    ("debug_bundler_dumpReputation", 10),
    ("debug_bundler_getStakeStatus", 10),
    ("debug_bundler_sendBundleNow", 10),
    ("debug_bundler_setBundlingMode", 10),
    ("debug_bundler_setReputation", 10),
    ("debug_dbAncient", 1000),
    ("debug_dbAncients", 1000),
    ("debug_dbGet", 1000),
    ("debug_executionWitness", 1000),
    ("debug_executionWitnessByHash", 1000),
    ("debug_getBadBlocks", 1000),
    ("debug_getRawBlock", 1000),
    ("debug_getRawHeader", 1000),
    ("debug_getRawReceipts", 1000),
    ("debug_getTrieFlushInterval", 1000),
    ("debug_storageRangeAt", 1000),
    ("debug_traceBlockByHash", 1000),
    ("debug_traceBlockByNumber", 1000),
    ("debug_traceCall", 1000),
    ("debug_traceCallMany", 1000),
    ("debug_traceTransaction", 1000),
    ("erigon_forks", 20),
    ("erigon_getHeaderByHash", 20),
    ("erigon_getHeaderByNumber", 20),
    ("erigon_getLogsByHash", 20),
    ("erigon_issuance", 20),
    ("eth_accounts", 10),
    ("eth_baseFee", 10),
    ("eth_bigBlockGasPrice", 20),
    ("eth_blobBaseFee", 10),
    ("eth_blockNumber", 10),
    ("eth_call", 26),
    ("eth_callBundle", 40),
    ("eth_callMany", 20),
    ("eth_chainId", 5),
    ("eth_createAccessList", 10),
    ("eth_estimateDiffSize", 20),
    ("eth_estimateGas", 20),
    ("eth_estimateUserOperationGas", 50),
    ("eth_feeHistory", 10),
    ("eth_fillTransaction", 20),
    ("eth_gasPrice", 20),
    ("eth_getAccount", 20),
    ("eth_getAccountInfo", 60),
    ("eth_getAssetBalance", 10),
    ("eth_getBalance", 20),
    ("eth_getBlobSidecars", 20),
    ("eth_getBlockByHash", 20),
    ("eth_getBlockByNumber", 20),
    ("eth_getBlockReceipts", 500),
    ("eth_getBlockReceiptsWithSystemTx", 20),
    ("eth_getBlockTransactionCountByHash", 20),
    ("eth_getBlockTransactionCountByNumber", 20),
    ("eth_getChainConfig", 10),
    ("eth_getCode", 20),
    ("eth_getFilterChanges", 20),
    ("eth_getFilterLogs", 60),
    ("eth_getFinalizedHeader", 20),
    ("eth_getLogs", 60),
    ("eth_getProof", 20),
    ("eth_getRawTransactionByHash", 20),
    ("eth_getRootHash", 20),
    ("eth_getStorageAt", 20),
    ("eth_getSystemTxsByBlockHash", 20),
    ("eth_getSystemTxsByBlockNumber", 20),
    ("eth_getTdByNumber", 20),
    ("eth_getTransactionByBlockHashAndIndex", 20),
    ("eth_getTransactionByBlockNumberAndIndex", 20),
    ("eth_getTransactionByHash", 20),
    ("eth_getTransactionCount", 20),
    ("eth_getTransactionReceipt", 20),
    ("eth_getTransactionReceiptsByBlock", 1500),
    ("eth_getUncleByBlockHashAndIndex", 20),
    ("eth_getUncleByBlockNumberAndIndex", 20),
    ("eth_getUncleCountByBlockHash", 20),
    ("eth_getUncleCountByBlockNumber", 20),
    ("eth_getUserOperationByHash", 20),
    ("eth_getUserOperationReceipt", 20),
    ("eth_getWithdrawalProof", 20),
    ("eth_maxPriorityFeePerGas", 10),
    ("eth_newBlockFilter", 20),
    ("eth_newFilter", 20),
    ("eth_newPendingTransactionFilter", 20),
    ("eth_protocolVersion", 5),
    ("eth_sendRawTransaction", 50),
    ("eth_sendRawTransactionSync", 125),
    ("eth_sendUserOperation", 100),
    ("eth_simulateV1", 40),
    ("eth_submitWork", 20),
    ("eth_subscribe", 10),
    ("eth_supportedEntryPoints", 10),
    ("eth_syncing", 5),
    ("eth_uninstallFilter", 10),
    ("eth_unsubscribe", 10),
    ("eth_usingBigBlocks", 20),
    ("ledger_getHeadL2Block", 20),
    ("ledger_getHeadL2BlockHeight", 20),
    ("ledger_getL2BlockByHash", 20),
    ("ledger_getL2BlockByNumber", 20),
    ("ledger_getL2BlockRange", 20),
    ("ledger_getL2GenesisStateRoot", 20),
    ("ledger_getLastScannedL1Height", 20),
    ("ledger_getLastVerifiedBatchProof", 20),
    ("ledger_getSequencerCommitmentByIndex", 20),
    ("ledger_getSequencerCommitmentsOnSlotByHash", 20),
    ("ledger_getSequencerCommitmentsOnSlotByNumber", 20),
    ("ledger_getVerifiedBatchProofsBySlotHeight", 20),
    ("linea_estimateGas", 20),
    ("linea_getProof", 20),
    ("linea_getTransactionExclusionStatusV1", 20),
    ("net_listening", 5),
    ("net_peerCount", 5),
    ("net_version", 5),
    ("optimism_outputAtBlock", 10),
    ("optimism_rollupConfig", 10),
    ("optimism_syncStatus", 10),
    ("optimism_version", 10),
    ("parlia_getSnapshot", 20),
    ("pm_getPaymasterData", 100),
    ("pm_getPaymasterStubData", 100),
    ("realtime_sendRawTransaction", 40),
    ("rundler_dropLocalUserOperation", 1000),
    ("rundler_getUserOperationGasPrice", 10),
    ("rundler_getUserOperationStatus", 50),
    ("rundler_maxPriorityFeePerGas", 10),
    ("trace_block", 20),
    ("trace_call", 40),
    ("trace_callMany", 3000),
    ("trace_filter", 40),
    ("trace_get", 20),
    ("trace_rawTransaction", 40),
    ("trace_replayBlockTransactions", 3000),
    ("trace_replayTransaction", 3000),
    ("trace_transaction", 40),
    ("txpool_content", 500),
    ("wallet_createAccount", 50),
    ("wallet_createSession", 50),
    ("wallet_formatSign", 15),
    ("wallet_getAssets", 200),
    ("wallet_getCallsStatus", 20),
    ("wallet_getCapabilities", 10),
    ("wallet_getCrossChainStatus_v0", 40),
    ("wallet_listAccounts", 10),
    ("wallet_prepareCalls", 50),
    ("wallet_prepareSign", 15),
    ("wallet_requestAccount", 50),
    ("wallet_requestQuote_v0", 50),
    ("wallet_sendPreparedCalls", 100),
    ("web3_clientVersion", 20),
    ("web3_sha3", 20),
    ("zks_L1BatchNumber", 10),
    ("zks_L1ChainId", 10),
    ("zks_estimateFee", 10),
    ("zks_estimateGasL1ToL2", 10),
    ("zks_gasPerPubdata", 10),
    ("zks_getAllAccountBalances", 10),
    ("zks_getBaseTokenL1Address", 10),
    ("zks_getBlockDetails", 10),
    ("zks_getBridgeContracts", 10),
    ("zks_getBridgehubContract", 10),
    ("zks_getBytecodeByHash", 10),
    ("zks_getConfirmedTokens", 10),
    ("zks_getFeeParams", 10),
    ("zks_getL1BatchBlockRange", 10),
    ("zks_getL1BatchDetails", 10),
    ("zks_getL1GasPrice", 10),
    ("zks_getL2ToL1LogProof", 10),
    ("zks_getL2ToL1MsgProof", 10),
    ("zks_getMainContract", 10),
    ("zks_getProof", 10),
    ("zks_getProtocolVersion", 10),
    ("zks_getRawBlockTransactions", 10),
    ("zks_getTestnetPaymaster", 10),
    ("zks_getTransactionDetails", 10),
    ("zks_sendRawTransactionWithDetailedOutput", 20),
];

fn alchemy_throughput_units(method: &str) -> u32 {
    ALCHEMY_THROUGHPUT_UNITS
        .binary_search_by(|(listed, _)| (*listed).cmp(method))
        .map_or(ALCHEMY_UNLISTED_METHOD_UNITS, |index| {
            ALCHEMY_THROUGHPUT_UNITS[index].1
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn alchemy_uses_throughput_units_where_listed() {
        let cost = ComputeUnitCost::Alchemy;
        assert_eq!(cost.units("eth_chainId"), 5);
        assert_eq!(cost.units("eth_blockNumber"), 10);
        assert_eq!(cost.units("eth_getStorageAt"), 20);
        assert_eq!(cost.units("eth_call"), 26);
        assert_eq!(cost.units("eth_getLogs"), 60);
        assert_eq!(cost.units("eth_sendRawTransaction"), 50);
        assert_eq!(cost.units("eth_getBlockReceipts"), 500);
        assert_eq!(cost.units("debug_traceCall"), 1000);
        assert_eq!(cost.units("trace_replayTransaction"), 3000);
        assert_eq!(cost.units("eth_sendUserOperation"), 100);
    }

    #[test]
    fn alchemy_counts_unlisted_methods_as_eth_call() {
        assert_eq!(
            ComputeUnitCost::Alchemy.units("anvil_mine"),
            ALCHEMY_UNLISTED_METHOD_UNITS
        );
    }

    #[test]
    fn alchemy_batch_sums_its_members() {
        assert_eq!(
            ComputeUnitCost::Alchemy.packet_units([
                "eth_call",
                "eth_call",
                "eth_getLogs",
                "eth_blockNumber"
            ]),
            122
        );
    }

    #[test]
    fn empty_packet_still_consumes_one_unit() {
        assert_eq!(ComputeUnitCost::Alchemy.packet_units([]), 1);
    }

    #[test]
    fn alchemy_budget_holds_ten_seconds_of_cups() {
        let snapshot = ComputeUnitBudget::alchemy(8_000).snapshot();
        assert_eq!(snapshot.units_per_second, 8_000);
        assert_eq!(snapshot.window, ALCHEMY_THROUGHPUT_WINDOW);
        assert_eq!(snapshot.capacity, 80_000);
        assert_eq!(snapshot.remaining_in_window, 80_000);
    }

    #[test]
    fn sub_second_window_scales_capacity() {
        let budget =
            ComputeUnitBudget::new(ComputeUnitCost::Alchemy, 1_000, Duration::from_millis(250));
        assert_eq!(budget.snapshot().capacity, 250);
    }

    #[test]
    fn capacity_saturates_at_u32_max() {
        let budget = ComputeUnitBudget::new(
            ComputeUnitCost::Alchemy,
            u32::MAX,
            ALCHEMY_THROUGHPUT_WINDOW,
        );
        assert_eq!(budget.snapshot().capacity, u32::MAX);
    }

    #[test]
    fn clones_share_one_budget() {
        let budget = ComputeUnitBudget::alchemy(100);
        let clone = budget.clone();
        assert!(budget.shares_limiter_with(&clone));
        assert!(!budget.shares_limiter_with(&ComputeUnitBudget::alchemy(100)));
    }

    #[test]
    #[should_panic(expected = "compute-unit budget must be > 0")]
    fn zero_budget_is_rejected() {
        ComputeUnitBudget::alchemy(0);
    }

    #[test]
    #[should_panic(expected = "compute-unit budget window must be at least 1 ms")]
    fn zero_window_is_rejected() {
        ComputeUnitBudget::new(ComputeUnitCost::Alchemy, 100, Duration::ZERO);
    }

    #[test]
    fn alchemy_table_is_sorted_and_unique() {
        assert!(
            ALCHEMY_THROUGHPUT_UNITS
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0)
        );
    }

    #[test]
    fn alchemy_lists_chain_specific_methods() {
        let cost = ComputeUnitCost::Alchemy;
        assert_eq!(cost.units("zks_getProof"), 10);
        assert_eq!(cost.units("eth_getSystemTxsByBlockNumber"), 20);
        assert_eq!(cost.units("wallet_sendPreparedCalls"), 100);
    }

    /// Alchemy's cost tables for the products an EVM JSON-RPC endpoint serves;
    /// REST products (NFT, Portfolio, Prices, Signer) and non-EVM chains are left out.
    const ALCHEMY_EVM_PRODUCTS: &[&str] = &[
        "evm",
        "debug",
        "trace",
        "arbtrace",
        "token",
        "transact",
        "transfers",
        "utility",
        "gas-manager-bundler",
        "wallet",
        "polygon-pos",
        "zksync",
        "citrea",
        "linea",
        "megaeth",
        "astar",
        "optimism",
        "hyperevm",
    ];

    const ALCHEMY_COST_PAGE: &str = "https://www.alchemy.com/docs/reference/compute-unit-costs.md";

    #[derive(Debug, PartialEq, Eq)]
    enum PublishedCost {
        Units(u32),
        SumOfMembers,
    }

    /// Rows of one generated table, delimited on the page by
    /// `{/* cu:auto product="…" */}` and `{/* cu:auto end */}`.
    fn product_table<'a>(page: &'a str, product: &str) -> &'a str {
        let marker = format!("{{/* cu:auto product=\"{product}\" */}}");
        let start = page
            .find(&marker)
            .unwrap_or_else(|| panic!("cost page lost product table {product}"))
            + marker.len();
        let end = page[start..]
            .find("{/* cu:auto end */}")
            .expect("product table is terminated");
        &page[start..start + end]
    }

    /// Method and throughput units of one table row; a blank "Throughput CU"
    /// cell means the "CU" value applies.
    fn published_row(row: &str) -> (String, PublishedCost) {
        let cells: Vec<&str> = row
            .trim()
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect();
        let method = cells[0].replace("\\_", "_");
        let units = cells
            .get(2)
            .filter(|throughput| !throughput.is_empty())
            .unwrap_or(&cells[1]);
        let cost = units.parse().map_or_else(
            |_| {
                assert_eq!(*units, "CU of method # times called", "row {row}");
                PublishedCost::SumOfMembers
            },
            PublishedCost::Units,
        );
        (method, cost)
    }

    fn published_costs(page: &str) -> Vec<(String, PublishedCost)> {
        ALCHEMY_EVM_PRODUCTS
            .iter()
            .flat_map(|product| {
                product_table(page, product)
                    .lines()
                    .filter(|line| line.starts_with('|'))
                    .skip(2)
                    .map(published_row)
            })
            .collect()
    }

    #[tokio::test]
    #[ignore = "fetches Alchemy's published cost table"]
    async fn alchemy_table_matches_published_costs() {
        let page = crate::reqwest::get(ALCHEMY_COST_PAGE)
            .await
            .and_then(crate::reqwest::Response::error_for_status)
            .expect("cost page is reachable")
            .text()
            .await
            .unwrap();
        let published = published_costs(&page);

        assert!(
            published
                .iter()
                .any(|(method, cost)| method == "batch" && *cost == PublishedCost::SumOfMembers),
            "Alchemy no longer documents batches as the sum of their members"
        );
        let listed: Vec<(&str, u32)> = published
            .iter()
            .filter_map(|(method, cost)| match cost {
                PublishedCost::Units(units) => Some((method.as_str(), *units)),
                PublishedCost::SumOfMembers => None,
            })
            .collect();
        let expected: BTreeMap<&str, u32> = listed.iter().copied().collect();
        let ours: BTreeMap<&str, u32> = ALCHEMY_THROUGHPUT_UNITS.iter().copied().collect();

        let conflicting = listed
            .iter()
            .filter(|(method, units)| expected[method] != *units)
            .map(|(method, units)| {
                format!(
                    "{method}: published with both {units} and {}",
                    expected[method]
                )
            });
        let changed = expected
            .iter()
            .filter(|(method, units)| ours.get(*method) != Some(*units))
            .map(|(method, units)| {
                format!("{method}: published {units}, ours {:?}", ours.get(method))
            });
        let dropped = ours
            .keys()
            .filter(|method| !expected.contains_key(*method))
            .map(|method| format!("{method}: no longer published"));
        let differences: Vec<String> = conflicting.chain(changed).chain(dropped).collect();

        let published_rows: String = expected
            .iter()
            .map(|(method, units)| format!("    (\"{method}\", {units}),\n"))
            .collect();
        assert!(
            differences.is_empty(),
            "ALCHEMY_THROUGHPUT_UNITS differs from {ALCHEMY_COST_PAGE}:\n{}\n\npublished rows:\n{published_rows}",
            differences.join("\n")
        );
    }
}
