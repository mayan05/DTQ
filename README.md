# Distributed Task Queue (DTQ) - Microservices Architecture

[![CI/CD Pipeline](https://github.com/mayan-sequeira/DTQ/actions/workflows/ci.yml/badge.svg)](https://github.com/mayan-sequeira/DTQ/actions)
![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)
![Redis](https://img.shields.io/badge/Redis-7.4-DC382D.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5.svg)
![Tests](https://img.shields.io/badge/Tests-17%20Passed-brightgreen.svg)

A production-grade, distributed asynchronous task processing system designed around a decoupled microservice architecture. It features non-blocking job ingestion, dedicated worker node isolation, resilient retry backoff with jitter, dead-letter queuing, and container orchestration with Docker Compose and Kubernetes.

---

## 🏗️ High-Level System Architecture

```mermaid
flowchart TD
    Client["Client / Load Generator / Locust"]
    
    subgraph Ingestion_Layer ["Job Service (FastAPI Microservice)"]
        API["REST API (Port 8000)"]
        SQLite[("Jobs DB (WAL Mode)")]
        API <-->|Read / Write State| SQLite
    end

    subgraph Message_Broker ["Redis Message Broker (Port 6379)"]
        QHigh[("queue:high (VIP Priority)")]
        QDef[("queue:default (Standard)")]
        QDLQ[("queue:dlq (Dead Letter Queue)")]
    end

    subgraph Worker_Fleet ["Worker Fleet (Processing Microservices)"]
        WorkerHigh["Dedicated VIP Worker Node\n(processing-service-high)"]
        WorkerDef["Default Worker Node\n(processing-service)"]
    end

    Client -->|1. POST /jobs (HTTP 202 Accepted)| API
    Client -.->|Poll GET /jobs/:id| API
    
    API -->|2a. Push High Priority Event| QHigh
    API -->|2b. Push Default Priority Event| QDef

    QHigh -->|Strict High-Only BLPOP| WorkerHigh
    QDef -->|Default-Only BLPOP| WorkerDef

    WorkerHigh & WorkerDef -->|3. PUT /internal/jobs/:id/status| API
    WorkerHigh & WorkerDef -.->|Exhausted Retries (Poison Pill)| QDLQ
```

---

## 💡 Core Engineering Principles & Microservice Design

### 1. Decoupled Ingestion vs. Compute Execution
- **Job Service (Producer & State Manager):** A high-throughput, low-latency I/O-bound FastAPI service. It asynchronously accepts compute tasks, returns `HTTP 202 Accepted` with a `Location: /jobs/{job_id}` header, persists initial state in SQLite with Write-Ahead Logging (WAL), and delegates execution to Redis.
- **Processing Service (Consumer & Execution Engine):** Compute-bound workers decoupled from the HTTP ingestion lifecycle. Workers scale independently based on queue depth, completely isolated from traffic spikes.

### 2. Physical Worker Fleet Isolation (High vs. Default)
- **Dedicated VIP Worker Node (`processing-service-high`):** Monopolizes the `queue:high` queue. Critical or VIP tasks never experience head-of-line blocking or delay, even if standard traffic spikes by 100x.
- **Standard Worker Node (`processing-service`):** Exclusively monitors and drains `queue:default`.

### 3. Production Reliability: Retries, Exponential Backoff, and Jitter
Located in `services/processing_service/app/worker.py` under `handle_retry_or_dlq()`:
- **Exponential Backoff:** When transient errors occur, the worker calculates an exponentially increasing delay:
  $$\text{Delay} = \text{BACKOFF\_BASE}^{\text{attempt}} + \text{jitter}$$
- **Jitter (`random.uniform(0.1, 0.5)`):** Adds randomness to prevent the **thundering herd problem** (where concurrent failed workers retry simultaneously and overwhelm downstream services).
- **Dead-Letter Queue (DLQ):** Poison-pill payloads that repeatedly fail exceed `MAX_RETRIES` (default: 3) and are routed to `queue:dlq` alongside full failure stack traces, protecting the fleet from infinite retry loops.
- **Graceful Shutdown:** Workers capture `SIGTERM` and `SIGINT` signals, allowing in-flight tasks to finish before exiting.

---

## 🔌 API Reference & Endpoints

| Method | Endpoint | Description | Status Code |
| :--- | :--- | :--- | :--- |
| `POST` | `/jobs` | Enqueue a task asynchronously (`priority`: `"high"` or `"default"`) | `202 Accepted` |
| `GET` | `/jobs/{job_id}` | Query execution state & result | `200 OK` / `404 Not Found` |
| `GET` | `/jobs` | List tasks (supports `limit`, `offset`, `status`, `priority`) | `200 OK` |
| `PUT` | `/internal/jobs/{job_id}/status` | Internal worker status callback | `200 OK` |
| `GET` | `/health` | Liveness & Readiness probe (DB + Redis check) | `200 OK` / `503 Unavailable` |
| `GET` | `/docs` | Interactive Swagger / OpenAPI documentation | `200 OK` |

### Sample Request: Submit Job
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "Render video clip #4029",
    "delay_seconds": 3,
    "priority": "high"
  }'
```

### Sample Response (`202 Accepted`)
```json
{
  "job_id": "76ec2a6e-41d8-4f8e-a9b0-96f30a90e3eb",
  "status": "PENDING",
  "priority": "high",
  "created_at": "2026-11-19T05:00:00.000000Z",
  "message": "Job accepted and queued for processing"
}
```

---

## 🚀 Quickstart with Docker Compose

Spin up the entire microservices ecosystem (Redis, Job Service, Default Worker, High-Priority Worker) with a single command:

```bash
docker compose up --build
```

### Infrastructure Lifecycle:
- Workers depend solely on **Redis** being healthy (`service_healthy`) to start consuming.
- Workers communicate with the **Job Service** via REST callback (`PUT /internal/jobs/{id}/status`) to synchronize task states.
- The state layer persists across container restarts via the `job-data` volume.

---

## ☸️ Kubernetes Deployment

The `k8s/` directory contains standard declarative production manifests with resource requests/limits, liveness/readiness probes, and service discovery.

```bash
# 1. Apply all manifests
kubectl apply -f k8s/

# 2. Inspect pods and services
kubectl get pods
kubectl get services

# 3. Port-forward or access LoadBalancer
kubectl port-forward service/job-service 8000:8000
```

---

## 🧪 Automated Testing & CI/CD

The project features a complete automated test suite covering unit operations, data validation, Redis dispatching, exponential backoff, and DLQ routing.

```bash
# Run tests locally
pytest tests/ -v
```

---

## 📊 Load Testing with Locust

The Locust benchmark file ([`locustfile.py`](locustfile.py)) is focused strictly on stress-testing the business endpoints under heavy concurrency:
- **`POST /jobs` (Weight: 10):** High-volume asynchronous task submission.
- **`GET /jobs/{job_id}` (Weight: 3):** User status polling.
- **`GET /jobs` (Weight: 1):** Paginated database querying.

```bash
locust -f locustfile.py --host http://localhost:8000
```
Open `http://localhost:8089` to initiate the benchmark.

---

## 🎯 Tech Recruiter & Interview Cheatsheet (Architecture Q&A)

<details>
<summary><strong>Q1: How do you handle priority tasks and prevent queue starvation?</strong></summary>

> **Answer:** We use physical worker fleet isolation:
> 1. `processing-service` exclusively consumes `queue:default`.
> 2. `processing-service-high` exclusively consumes `queue:high`.
> Even if a million default-priority jobs arrive, the high-priority worker pool remains completely unaffected and ready to process critical workloads instantly.
</details>

<details>
<summary><strong>Q2: Why did workers have a connection to Job Service in Docker Compose?</strong></summary>

> **Answer:** The workers report job state transitions (`PROCESSING`, `RETRYING`, `SUCCESS`, `FAILED`) back to the persistence layer via an HTTP callback (`PUT /internal/jobs/:id/status`). At startup, the workers only depend on the Redis broker (`condition: service_healthy`), allowing true decoupling at the infrastructure level while using lightweight HTTP callbacks for state synchronization.
</details>

<details>
<summary><strong>Q3: Where and how are exponential backoff, jitter, and DLQ implemented?</strong></summary>

> **Answer:** In `services/processing_service/app/worker.py` within `handle_retry_or_dlq()`:
> - Exponential backoff calculates delay as `BACKOFF_BASE ** attempt`.
> - Jitter adds `random.uniform(0.1, 0.5)` to break synchronization across concurrent failing workers.
> - When `attempt >= MAX_RETRIES`, the message is safely diverted to `queue:dlq` (Dead-Letter Queue).
</details>
