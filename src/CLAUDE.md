# Project
Local trading-system simulator: Kubernetes + Kafka event-driven stack for a limit order book (LOB) matching engine.

# Components
- `kafka_init` (Python): creates topics and bootstrap config. Run as k8s Jobs.
- `matching-engine` (Go): in-memory LOB, deterministic matching. Run as k8s Deployment.
- `traderpool` (Go): publishes buy/sell limit orders. Run as k8s Deployment.
- `scripts`(Python): utilities to read / produce to queues during testing.

# Data Flow
Traders publish orders → matching engine updates book and matches orders → emits `trade` events and `midprice` updates via Kafka.

# Current Scope
Implemented: limit-order handling, matching logic, midprice feed.

# Goals
1. Deterministic, correct matching behavior (data-structure rigor).
2. Strong observability for replay/demo (Kafka + local K8s).

# TODO
- Order cancellation
- L1/L2 market depth output