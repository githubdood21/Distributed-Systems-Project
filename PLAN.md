# Distributed OCR System — Project Plan

## 1. Overview

A distributed image processing (OCR) pipeline where:
- A minimal web frontend uploads images and displays extracted text results.
- A .NET backend orchestrates the flow, stores results, and exposes a REST API.
- **Apache Kafka** acts as the message backbone — distributing work to workers and carrying results back.
- One or more **Python worker nodes** run on separate machines, each hosting an OCR model. They consume tasks from Kafka, process images, and publish results back.

### Course Goals Demonstrated
| Concept | Where it shows up |
|---------|-------------------|
| Message queuing / pub-sub | Kafka topics (`ocr-tasks`, `ocr-results`, `worker-metrics`) |
| Load distribution | Kafka consumer groups — each task processed by exactly one worker |
| Decoupling | Frontend ↔ API ↔ Workers are fully async; no direct node-to-node HTTP for work |
| Fault tolerance | If a worker dies, its tasks are re-assigned to another consumer in the group |
| Metrics / observability | Workers publish CPU/RAM/queue-depth metrics to a dedicated topic |
| Multi-language distributed system | C# (orchestrator) + Python (workers) communicating via a broker |

---

## 2. Architecture

```mermaid
graph TB
    subgraph Client
        FE[Frontend<br/>React or Angular<br/>Upload + Results UI]
    end

    subgraph Server "Server (single machine)"
        API[.NET Web API<br/>REST + Kafka producer/consumer]
        DB[(SQLite / Postgres<br/>Results store)]
    end

    subgraph Kafka Cluster
        KT[Topic: ocr-tasks]
        KR[Topic: ocr-results]
        KM[Topic: worker-metrics]
    end

    subgraph Workers "Worker Nodes (separate machines)"
        W1[Worker 1<br/>Python + OCR model<br/>Kafka consumer]
        W2[Worker 2<br/>Python + OCR model<br/>Kafka consumer]
        WN[Worker N ...]
    end

    FE -->|POST /api/images (upload)| API
    FE -->|GET /api/results/{id}| API
    API -->|publish task| KT
    KT -->|consume| W1
    KT -->|consume| W2
    KT -->|consume| WN
    W1 -->|publish result| KR
    W2 -->|publish result| KR
    WN -->|publish result| KR
    KR -->|consume + store| API
    API --> DB
    W1 -->|publish metrics| KM
    W2 -->|publish metrics| KM
    KM -->|consume (optional dashboard)| API
```

### Key Architectural Decision: Pull, Not Push

> **Your original idea:** .NET "dispatches requests to nodes" that listen on a port.
>
> **Revised approach:** Workers are Kafka *consumers*. They connect **out** to the broker and pull tasks. No inbound ports needed on worker machines, no central dispatcher tracking who's free, and if a worker crashes Kafka automatically reassigns its partition to another consumer.

This is simpler, more secure (firewall: workers only need outbound access to the Kafka broker), and scales naturally — just add another consumer to the group.

---

## 3. Components in Detail

### 3.1 Frontend (React or Angular)

**Scope:** Deliberately minimal.
- Upload form (drag-drop or file picker, supports PNG/JPG/TIFF/PDF).
- Shows a list of submitted jobs with status: `queued → processing → done / failed`.
- Displays extracted text when result is ready.
- Polls `GET /api/jobs/{id}` every 2–3 s (or use SSE/WebSocket if you want to be fancy).

**Recommendation:** **React + Vite** — faster setup, smaller bundle, easier to keep "very minimal." Angular is fine too but has more boilerplate for a single-page form.

**Tech:** React 18+, TypeScript, plain CSS or Tailwind. No router needed (single page).

### 3.2 .NET Backend (the orchestrator)

**Role:** The only component the frontend talks to. It:
1. Accepts image uploads (`POST /api/images`).
2. Stores the image (see §4 "Image Distribution").
3. Publishes a task message to Kafka topic `ocr-tasks`.
4. Consumes from `ocr-results` topic, stores results in DB.
5. Exposes `GET /api/jobs/{id}` and `GET /api/jobs` for the frontend.
6. (Optional) Consumes `worker-metrics` and exposes a simple `/api/metrics` endpoint for a dashboard view.

**Tech:** .NET 8, ASP.NET Core Web API.
- Kafka client: **Confluent.Kafka** (official librdkafka binding).
- DB: **SQLite** (zero-config, fine for a course project) or PostgreSQL if you want to show off.
- Image storage: local disk under a known path, OR base64-in-message (see §4).

### 3.3 Apache Kafka

**Role:** The message backbone. Three topics:

| Topic | Producer | Consumer | Message shape |
|-------|----------|----------|---------------|
| `ocr-tasks` | .NET API | Python workers (consumer group `ocr-workers`) | `{ job_id, image_path_or_base64, options }` |
| `ocr-results` | Python workers | .NET API | `{ job_id, status, text?, error? }` |
| `worker-metrics` | Python workers | .NET API (optional) | `{ worker_id, cpu%, ram%, queue_depth, timestamp }` |

**Setup:** Run via **Docker Compose** in KRaft mode (no ZooKeeper since Kafka 3.x). Single broker is fine for the course; you can spin up 3 later to demo replication.

```yaml
# docker-compose.yml (sketch)
services:
  kafka:
    image: bitnami/kafka:3.7
    ports: ["9092:9092"]
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
```

### 3.4 Python Worker Nodes

**Role:** Each worker is a small Python process that:
1. Connects to Kafka as a consumer in group `ocr-workers`.
2. Polls for messages from `ocr-tasks`.
3. Downloads/reads the image, runs OCR, publishes result to `ocr-results`.
4. Every N seconds, publishes its system metrics to `worker-metrics`.

**Structure (per worker):**
```
worker/
├── main.py          # Kafka consumer loop + OCR dispatch
├── ocr_engine.py    # Black-box OCR interface (Tesseract 5 under the hood)
├── metrics.py       # psutil-based system usage reporter
├── config.py        # Broker address, topic names, worker_id
└── requirements.txt # confluent-kafka, pytesseract, psutil
```

**Resource constraints (per worker):**
- **OS:** Linux (any distro; Ubuntu 22.04 LTS recommended for Tesseract package availability).
- **CPU:** ≤ 2 logical cores, **AVX2 required** (Tesseract's LSTM inference benefits; also ensures the image is not emulated on older hardware).
- **RAM:** ≤ 2 GB.
- These limits are enforced by the VM/container cgroup or Docker `--cpus` / `--memory` flags. The worker should be a single Python process + Tesseract library — no heavy frameworks.

**Deployment model:**
- Each worker runs on its own **Linux VM** (e.g., Hetzner, DigitalOcean, AWS t3-small) **or** as a **Docker container** on a host for local dev.
- Docker containers make it trivial to scale up/down: `docker run --cpus=2 --memory=2g ocr-worker ...`.
- For the course demo, 2–4 workers is plenty. Spin them up/down to show the system adapting.

**Networking (Tailscale + Cloudflared):**
- All machines (server + workers) join a **Tailscale mesh network** → gives every node a stable private IP regardless of NAT/ISP/firewall. Workers connect to the Kafka broker over the Tailscale IP, no port-forwarding needed.
- **Cloudflared** (Cloudflare Tunnel) exposes the .NET Web API + frontend publicly for the live demo without opening inbound firewall ports. The tunnel terminates TLS at Cloudflare and forwards to the local server on `localhost`.
- Net effect: zero inbound ports exposed on any machine. Workers reach Kafka via Tailscale; viewers reach the UI via the Cloudflare Tunnel URL.

**Key properties:**
- Listens on **no inbound port**. It only makes outbound connections to the Kafka broker (via Tailscale IP).
- Each worker sets a unique consumer instance ID so Kafka tracks liveness.
- If a worker crashes, its assigned partition is rebalanced to another consumer within `session.timeout.ms`.

**OCR engine — black-box interface:**
The OCR engine is wrapped behind a single function so it can be swapped without touching the rest of the worker:
```python
# ocr_engine.py
from abc import ABC, abstractmethod

class OcrEngine(ABC):
    @abstractmethod
    def extract(self, image_bytes: bytes) -> str:
        """Accept raw image bytes (PNG/JPG/TIFF), return extracted text."""

class TesseractEngine(OcrEngine):
    def __init__(self, lang: str = "eng"):
        self._lang = lang

    def extract(self, image_bytes: bytes) -> str:
        img = Image.open(io.BytesIO(image_bytes))
        return pytesseract.image_to_string(img, lang=self._lang)
```
The worker calls `engine.extract(bytes)` and doesn't care which engine is behind it. This makes it trivial to swap Tesseract for EasyOCR/PaddleOCR later without changing the Kafka protocol or the .NET side.

---

## 4. Image Distribution (how workers get the image)

This is the one part your plan didn't cover yet. Options:

| Option | How | Trade-off |
|--------|-----|-----------|
| **A. Base64 in Kafka message** | .NET reads file → base64-encodes → puts in JSON message body | Simplest. Fine for images < ~5 MB. Wastes bandwidth but zero extra infra. |
| **B. Shared object store (MinIO)** | .NET uploads to MinIO, message contains a URL/key. Workers pull from MinIO. | Cleaner at scale. Adds one more Docker container. S3-compatible API. |
| **C. HTTP file endpoint on .NET** | .NET serves uploaded files at `GET /files/{id}`. Workers download via HTTP. | Simple, but now workers need an inbound route to the .NET server (defeats the "no inbound" benefit). |

**Recommendation:** Start with **Option A** (base64) for simplicity. If you have time, upgrade to **Option B** (MinIO in Docker Compose) as a stretch goal — it demonstrates object storage and keeps Kafka messages small.

---

## 5. Data Flow (end-to-end)

```
1. User drops an image on the frontend.
2. Frontend POSTs multipart/form-data to .NET API: POST /api/images
3. .NET saves image, creates a job record (status=queued), publishes to `ocr-tasks`:
   { "job_id": "abc123", "image_b64": "...", "lang": "eng" }
4. A Python worker (consumer) picks up the message from `ocr-tasks`.
5. Worker decodes image, runs OCR, gets text.
6. Worker publishes to `ocr-results`:
   { "job_id": "abc123", "status": "done", "text": "Hello world..." }
7. .NET consumes the result, updates DB (status=done, text=...).
8. Frontend polls GET /api/jobs/abc123 → gets status=done + text → renders it.
```

Failure path: if OCR throws, worker publishes `{ job_id, status: "failed", error: "..." }`. .NET marks job as failed. Frontend shows the error.

---

## 6. Metrics & Observability (stretch / bonus)

Each worker runs a background thread that every 5 s publishes:
```json
{ "worker_id": "node-1", "cpu_percent": 42.3, "ram_percent": 67.1, "queue_depth": 0, "ts": 1719999999 }
```

.NET can consume these and expose `GET /api/metrics/workers` → frontend shows a tiny live table or sparkline. This demonstrates the metrics/observability aspect of distributed systems without pulling in Prometheus/Grafana (which would be overkill for scope).

**Stretch goal:** If you want to impress, add a simple Grafana + Prometheus sidecar that scrapes the .NET `/metrics` endpoint. But the Kafka-metrics-topic approach is sufficient and more "distributed-systems native."

---

## 7. Corrections & Adjustments to Your Original Plan

| Your idea | Verdict | Adjustment |
|-----------|---------|------------|
| Web interface: React or Angular, minimal upload + results | ✅ Confirmed | **React + Vite** (light overhead, single page, no router). |
| .NET as the server base managing the app | ✅ Good | .NET 8 ASP.NET Core. It's the single entry point for the frontend and the Kafka producer/consumer for tasks/results. |
| "Load balancing and dispatching with just Kafka" | ⚠️ Partially | Kafka does **load distribution via consumer groups**, not push-based dispatching. Workers pull, not receive. This is actually *better* — no central dispatcher to fail, no port listening on workers. |
| Workers listen on all IPs + specific port | ❌ Not needed | With the pull model, workers make **outbound** connections to Kafka only (via Tailscale). No inbound ports anywhere. |
| Python scripts hosting a model | ✅ Good | Small Python process per VM/container. OCR engine behind a black-box interface (`OcrEngine.extract(bytes) -> str`). Add `psutil` for metrics. |
| Reporting system usage for metrics | ✅ Good | Publish to a dedicated Kafka topic. .NET can display it. |
| Workers on separate VMs or Docker containers, ≤ 2 cores (AVX2), 2 GB RAM | ✅ Added | Enforced via cgroup/Docker flags. Tesseract is well within these limits. AVX2 ensures fast LSTM inference. |
| Tailscale + Cloudflared for networking/demo | ✅ Added | Tailscale = private mesh (workers → Kafka). Cloudflared = public demo URL (viewers → UI). Zero inbound ports on any machine. |

---

## 8. Technology Stack (final)

| Layer | Tech | Why |
|-------|------|-----|
| Frontend | React 18 + Vite + TypeScript | Minimal overhead, single page, fast build |
| Backend / API | .NET 8, ASP.NET Core Web API | Solid REST + async I/O, single entry point |
| Kafka client (.NET) | Confluent.Kafka | Official librdkafka binding, well-supported |
| Kafka client (Python) | `confluent-kafka` | More performant than `kafka-python`, same C library |
| Message broker | Apache Kafka 3.7 (KRaft mode, Docker) | No ZooKeeper, single container for dev |
| Image storage | Base64 in message (v1) → MinIO (stretch) | Start simple, upgrade if messages get large |
| DB | SQLite (via Microsoft.Data.Sqlite) | Zero-config, fine for course scope |
| OCR engine | **Tesseract 5** via `pytesseract` | CPU-only, fits in 2 GB RAM, black-box swappable interface |
| Worker system metrics | `psutil` (Python) | CPU, RAM, disk — published to Kafka topic |
| Worker runtime | Linux VM or Docker container, ≤ 2 vCPU (AVX2), 2 GB RAM | Enforced via cgroup / `--cpus` / `--memory` |
| Private networking | **Tailscale** | Mesh network across VMs/containers, stable IPs, no NAT/port-forwarding |
| Public demo access | **Cloudflared** (Cloudflare Tunnel) | Exposes UI publicly without opening inbound ports |
| Containerization | Docker Compose for Kafka (+ MinIO optional) | One-command infra setup |

---

## 9. Implementation Phases

### Phase 0 — Scaffolding & Environment Setup (FIRST STEP)
Goal: every component initialized as a runnable stub in the repo, all dependencies installed, and a README that makes setup trivial.

- [x] **Docker Compose:** Kafka broker (KRaft mode) defined in `docker-compose.yml`.
- [x] **.NET backend scaffolded:** `backend/` — ASP.NET Core Web API (.NET 8), Confluent.Kafka + SQLite packages referenced, stub controller + producer/consumer classes.
- [x] **Python worker scaffolded:** `worker/` — `main.py`, `ocr_engine.py` (black-box interface + Tesseract impl stub), `metrics.py`, `config.py`, `requirements.txt`.
- [x] **React frontend scaffolded:** `frontend/` — Vite + React + TS, stub `App.tsx` with an upload form placeholder.
- [ ] Install all dependencies: `dotnet restore`, `pip install -r worker/requirements.txt`, `npm install` in `frontend/`.
- [ ] Start Kafka via `docker compose up -d`; create topics `ocr-tasks`, `ocr-results`, `worker-metrics`.
- [ ] Verify each stub boots: .NET API responds, Python worker connects to Kafka, React dev server serves the page.
- [ ] (Networking, when moving to real VMs) Set up **Tailscale** on all machines + **Cloudflared** tunnel for the public demo URL.

### Phase 1 — .NET Backend (orchestrator)
- [ ] Implement `POST /api/images` — accept multipart upload, save file, create job in SQLite, publish task to Kafka (`ocr-tasks`).
- [ ] Implement Kafka consumer for `ocr-results` — update job status + text in DB.
- [ ] Implement `GET /api/jobs/{id}` and `GET /api/jobs` (list with pagination).
- [ ] (Optional) Consume `worker-metrics` topic, expose `GET /api/metrics/workers`.
- [ ] Integration test: publish a fake result message, verify DB row updates.

### Phase 2 — Python Worker
- [ ] Write `main.py`: Kafka consumer loop on `ocr-tasks` (group `ocr-workers`).
- [ ] Implement `ocr_engine.py` with the black-box `OcrEngine` interface + `TesseractEngine` implementation.
- [ ] On message: decode base64 image → call `engine.extract()` → publish result to `ocr-results`.
- [ ] Add `metrics.py` background thread publishing CPU/RAM/queue-depth to `worker-metrics` every 5 s.
- [ ] Containerize as a Docker image (`Dockerfile` + `docker-compose` entry) with `--cpus=2 --memory=2g`.
- [ ] Test: run 2 worker containers, submit images, verify both process tasks in parallel.

### Phase 3 — Frontend (React)
- [ ] Vite + React + TypeScript scaffold.
- [ ] Upload form with drag-drop (accepts PNG/JPG/TIFF).
- [ ] Job list with auto-polling (2–3 s), status badges (`queued` / `processing` / `done` / `failed`), result text display.
- [ ] (Optional) Small metrics panel showing live worker CPU/RAM from `/api/metrics/workers`.

### Phase 4 — Integration & Demo
- [ ] End-to-end: upload → queue → process across workers → result displayed in UI.
- [ ] Kill a worker mid-task → verify Kafka rebalancing, task re-assigned, no data loss.
- [ ] Submit N images concurrently → verify parallel processing, observe per-worker load in metrics.
- [ ] Scale test: add/remove a worker container live, show throughput change.
- [ ] Demo via Cloudflare Tunnel URL (public access without exposing ports).
- [ ] Record a short demo video / write up the architecture decisions for the course submission.

### Phase 5 — Stretch Goals (if time permits)
- [ ] MinIO object storage instead of base64-in-message (keeps Kafka messages small).
- [ ] Multi-partition `ocr-tasks` topic to demonstrate partition-level parallelism and per-partition ordering.
- [ ] Dead-letter queue for permanently failed messages (e.g., corrupt image, OOM).
- [ ] Grafana dashboard consuming the metrics topic (or Prometheus sidecar on .NET).
- [ ] Swap Tesseract → EasyOCR behind the same `OcrEngine` interface to prove the black-box design.
- [ ] Horizontal scale: run 4+ workers, capture a throughput-vs-worker-count chart.

---

## 10. Decisions (locked) & Remaining Open Items

### Locked ✅
| # | Decision | Choice |
|---|----------|--------|
| 1 | Frontend framework | **React + Vite** (light overhead, single page) |
| 2 | OCR engine | **Tesseract 5** via `pytesseract`, behind a black-box `OcrEngine` interface (swappable) |
| 3 | Worker runtime | Linux VMs or Docker containers, ≤ 2 vCPU (AVX2), 2 GB RAM each |
| 4 | Networking / demo access | **Tailscale** (private mesh) + **Cloudflared** (public tunnel for the UI) |
| 5 | DB | SQLite (zero-config, swap to Postgres if needed) |
| 6 | Image delivery | Base64-in-message for v1; MinIO as stretch goal |
| 7 | Kafka topology | Single broker (KRaft mode) for the course; multi-broker as stretch |
| 8 | Worker count for demo | 2–4 workers (spin up/down to show scaling) |

### Still open (low urgency)
| # | Question | Notes |
|---|----------|-------|
| A | Which cloud/provider for the VMs? | Hetzner / DigitalOcean / AWS — pick whichever you have an account on. 2× small VMs ≈ $5–10/mo. |
| B | Do you want the metrics panel in the UI, or is a terminal `curl` enough for the demo? | Nice-to-have; doesn't block anything. |
| C | Multi-partition `ocr-tasks` (e.g., 3 partitions) vs single partition? | Single partition is simplest. Multiple partitions show off Kafka's parallelism better in the write-up. Decide when you build Phase 0. |

---

## 11. Project Structure (monorepo sketch)

```
Distributed-Systems-Project/
├── PLAN.md                  ← this file
├── docker-compose.yml       ← Kafka (+ optional MinIO)
├── backend/                 ← .NET 8 Web API
│   ├── Backend.csproj
│   ├── Program.cs
│   ├── Controllers/
│   │   └── JobsController.cs
│   ├── Kafka/
│   │   ├── TaskProducer.cs
│   │   └── ResultConsumer.cs
│   ├── Data/
│   │   └── AppDbContext.cs  (or raw SQLite via Microsoft.Data.Sqlite)
│   └── Models/
│       └── Job.cs
├── worker/                  ← Python OCR worker
│   ├── main.py
│   ├── ocr_engine.py
│   ├── metrics.py
│   ├── config.py
│   └── requirements.txt
├── frontend/                ← React + Vite
│   ├── src/
│   │   ├── App.tsx
│   │   ├── components/
│   │   │   ├── UploadForm.tsx
│   │   │   ├── JobList.tsx
│   │   │   └── MetricsPanel.tsx
│   │   └── api.ts
│   ├── package.json
│   └── vite.config.ts
└── docs/
    └── writeup.md           ← final course write-up
```

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Kafka setup eats time | Use the `bitnami/kafka` image in KRaft mode — single container, no ZooKeeper. Test producer/consumer in Phase 0 before building anything else. |
| Workers can't reach the broker across machines | Ensure firewall allows outbound to broker:9092. For dev, run everything in one Docker Compose network first. |
| OCR is slow and results take too long to demo | Use small test images (a few KB). Tesseract on a 200×100 image is <1 s. |
| Base64 messages too large for Kafka default `message.max.bytes` (1 MB) | Set `message.max.bytes=5242880` (5 MB) on broker, or switch to MinIO early. |
| Consumer group rebalancing causes duplicate processing during demo | Acknowledge it in the write-up as at-least-once semantics. Make the OCR step idempotent (it is — same input → same output). |
