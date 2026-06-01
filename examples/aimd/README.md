# AIMD Adaptive Concurrency Demo

This demo shows the AIMD (Additive Increase Multiplicative Decrease) adaptive concurrency control in the batch processor. You will observe the per-endpoint concurrency limit dropping under sustained 429 failures, then recovering when failures clear.

## What is AIMD?

The batch processor uses AIMD to dynamically adjust how many concurrent inference requests it sends to each endpoint:

| Parameter | Value | Description |
| --- | --- | --- |
| `perEndpoint` | 10 | Initial concurrency limit per endpoint |
| `min` | 5 | Floor — AIMD never drops below this |
| `backoffFactor` | 0.5 | Multiplicative decrease on 429/5xx (e.g., 10 → 5) |
| `additiveIncrease` | 1 | Slots added per successful window (e.g., 5 → 6 → 7 → ... → 10) |

**Decrease triggers**: 429 (rate limit), 5xx (server error), 503 (capacity retry).
**Increase trigger**: Successful request completions (200).

The asymmetry — fast decrease, slow increase — is intentional: the system backs off quickly under pressure but probes capacity gradually to avoid oscillation.

## Prerequisites

1. **Deploy the dev cluster**:

   ```bash
   make dev-deploy
   ```

2. **Install the VS Code REST Client extension** (Ctrl+Shift+X / Cmd+Shift+X), or use `curl` manually.

3. **Port-forward the simulator** (needed for runtime failure toggling):

   ```bash
   kubectl port-forward svc/vllm-sim-aimd 8888:8000 -n default
   ```

4. **Open Grafana** in your browser: <http://localhost:3000>
   - Navigate to the **Processor** dashboard (UID: `batch-gateway-processor`)
   - Locate the two AIMD panels:
     - **AIMD Concurrency Limit per Endpoint** — shows the live limit
     - **AIMD Signals (decreases / increases)** — shows decrease/increase events

## Files

- **batch_input_aimd.jsonl**: 50 inference requests targeting `sim-model-aimd`.
- **demo.http**: REST Client file with step-by-step sequences.

## Demo Flow

### Phase 1: AIMD Drops Under Failures

The `sim-model-aimd` simulator is deployed with **100% `rate_limit` failure injection**. Every inference request returns HTTP 429.

1. Open `demo.http` in VS Code
2. Run **Sequence 1**: upload the input file, create a batch
3. Watch the **AIMD Concurrency Limit** panel in Grafana

**Expected behavior**:

- Concurrency drops rapidly: 10 → 5 (each 429 triggers `limit = limit × 0.5`, floored at 5)
- The **AIMD Signals** panel shows `rate_limit` decrease events
- The batch will eventually fail (all requests return 429)

### Phase 2: Clear Failures and Observe Recovery

Use the simulator's runtime admin API to disable failure injection instantly (no pod restart):

```bash
curl -X POST http://localhost:8888/admin/config \
  -H 'Content-Type: application/json' \
  -d '{"failure-injection-rate": 0}'
```

Or run the "Disable Failure Injection" request in **Sequence 2** of `demo.http`.

Then upload a new input file and create a batch.

**Expected behavior**:

- Requests start succeeding (200)
- Concurrency climbs linearly: 5 → 6 → 7 → ... → 10 (one slot added per successful window)
- The **AIMD Signals** panel shows increase events replacing the previous decrease events
- The batch completes successfully

### Phase 3: Reset

Restore failure injection for the next demo run:

```bash
curl -X POST http://localhost:8888/admin/config \
  -H 'Content-Type: application/json' \
  -d '{"failure-injection-rate": 100, "failure-types": ["rate_limit"]}'
```

Or run the "Restore 100% Failure Injection" request in **Sequence 3** of `demo.http`.

## Monitoring

### Grafana

Open <http://localhost:3000/d/batch-gateway-processor> for the Processor dashboard with AIMD panels.

### Prometheus Metrics

Query directly via the Prometheus UI at <http://localhost:9091>:

- `batch_processor_aimd_concurrency_limit` — current limit per endpoint
- `batch_processor_aimd_decreases_total` — decrease counter by endpoint and signal
- `batch_processor_aimd_increases_total` — increase counter by endpoint

### Raw Metrics

- Processor metrics endpoint: <http://localhost:9090/metrics>
- Filter for AIMD: `curl -s http://localhost:9090/metrics | grep aimd`

## Troubleshooting

### AIMD Limit Not Changing

- Verify the simulator is running: `kubectl get pods -l app=vllm-sim-aimd -n default`
- Check processor logs: `kubectl logs -l app.kubernetes.io/component=processor -n default | grep -i aimd`
- Confirm AIMD is enabled in processor config: the default `make dev-deploy` enables it

### Batch Completes Too Fast to See AIMD

- The 50-request input file with 10ms latency processes quickly. If needed, submit multiple batches in rapid succession to sustain traffic.

### Admin Config Not Taking Effect

- Verify the port-forward is running: `kubectl port-forward svc/vllm-sim-aimd 8888:8000 -n default`
- Check current config: `curl http://localhost:8888/admin/config`
- Verify the simulator is running: `kubectl get pods -l app=vllm-sim-aimd -n default`

### Grafana Not Showing Data

- Verify Grafana is running: <http://localhost:3000>
- Check that Prometheus is scraping: <http://localhost:9091/targets>
- AIMD metrics only appear after the processor handles at least one inference request
