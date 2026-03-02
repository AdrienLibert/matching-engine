## Matching-Engine Test Audit (Step 1)

Scope audited:
- `src/matching-engine/orderbook_test.go`
- `src/matching-engine/engine_test.go`
- `src/matching-engine/models_test.go`

Classification invariants:
- **Price priority**
- **FIFO (same price level)**
- **Schema contract**
- **Serialization / parsing**

### `orderbook_test.go`

- `TestMinHeapPop` → **Price priority primitive** (ask-side min ordering).
- `TestMinHeapPeak` → **Price priority primitive** (stable best-ask visibility).
- `TestMaxHeapPop` → **Price priority primitive** (bid-side max ordering).
- `TestMaxHeapPeak` → **Price priority primitive** (stable best-bid visibility).
- `TestOrderBookAddOrder` → **Price priority / top-of-book insertion** at a single price level on both sides.

Current gaps in this file:
- No explicit multi-price best-level regression (e.g., higher bid inserted later becoming best bid, lower ask inserted later becoming best ask).
- No explicit FIFO assertions within one price bucket.

### `engine_test.go`

- `TestMachingEngineProcessIterative` → **Scenario integration** with mixed outcomes:
  - no-match insertion,
  - partial fill,
  - full fill,
  - top-of-book updates.

Invariant mapping:
- **Price priority:** partially covered via best bid/ask checks.
- **FIFO:** not explicitly asserted (single resting order at each compared price in main flow).
- **Serialization / parsing:** not covered.
- **Schema contract:** not covered.

Assertion style risk noted:
- Uses pointer/slice identity comparisons (`&[]*Order{&...}`), which are brittle and do not validate value-level execution invariants.

### `models_test.go`

- `TestOrderAgainstContract` → **Schema contract** (`order.json` ↔ `Order`).
- `TestTradeAgainstContract` → **Schema contract** (`trade.json` ↔ `Trade`).
- `TestPricePointAgainstContract` → **Schema contract** (`pricepoint.json` ↔ `PricePoint`).

Invariant mapping:
- **Schema contract:** covered at struct-field/type level.
- **Serialization / parsing:** not directly tested (`messageToOrder`, `toJSON`, malformed payload paths not covered here).
- **Price priority / FIFO:** not applicable.

### Coverage Summary Matrix

| File | Price Priority | FIFO | Schema Contract | Serialization/Parsing |
|---|---|---|---|---|
| `orderbook_test.go` | Partial (heap + single-level add) | Missing | N/A | N/A |
| `engine_test.go` | Partial (iterative scenario) | Missing explicit checks | N/A | Missing |
| `models_test.go` | N/A | N/A | Present | Missing explicit runtime parsing/encoding checks |

### Step-1 Outcome

- Existing tests are now classified by invariant.
- Main exposed risks for next steps:
  - missing explicit FIFO validation,
  - missing multi-level best-price regressions,
  - missing serialization/malformed-input coverage in model-level tests.
