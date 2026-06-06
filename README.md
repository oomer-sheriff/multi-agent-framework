# DMAF (Distributed Multi-Agent Framework)

DMAF is an enterprise-grade, event-driven multi-agent framework built to break down complex tasks and execute them concurrently at scale. Powered by LangGraph, FastAPI, Redis Streams, and PostgreSQL, DMAF uses a master-worker architecture to dynamically build and resolve Directed Acyclic Graphs (DAGs) of AI-driven subtasks.

## 🌟 Key Features

- **Distributed Master-Worker Architecture**: An **Orchestrator** agent plans tasks and maps out dependencies, while horizontally scalable **Agent Workers** execute the LLM workloads via Redis Streams.
- **Race-Condition Free State Management**: DAG resolution and downstream queuing are protected by pessimistic row-level locking (`FOR UPDATE`) in PostgreSQL, guaranteeing deterministic execution across any number of concurrent pods.
- **Hybrid Context Management**: Context explosion is prevented by summarizing all raw subtask outputs using a fast, cheap LLM (e.g., Gemini 1.5 Flash). Summaries are injected into downstream prompts, while the full raw data is stored in **MinIO (S3)** and dynamically accessible via an MCP Tool (`read_task_output`).
- **Resilient Fault Tolerance**: Both Orchestrator and Agent workers feature automated **Dead Letter Queue (DLQ)** loops. If a Kubernetes pod crashes, is preempted, or OOMs mid-execution, the system automatically detects the abandoned task via Redis `XPENDING` and gracefully re-queues it using `XCLAIM`.
- **Kubernetes Autoscaling**: Built for the cloud, DMAF includes full manifests for **MicroK8s** and features a **KEDA ScaledObject** that automatically scales the `agent-worker` Deployment from 0 to N based strictly on the length of the Redis queue.

## 🏗️ Architecture Stack

- **API Layer**: FastAPI (Uvicorn)
- **Agent Logic**: LangChain & LangGraph
- **Message Broker**: Redis (Streams)
- **Database**: PostgreSQL (pgvector)
- **Blob Storage**: MinIO
- **Tooling Engine**: Model Context Protocol (MCP) Server
- **Frontend**: Vite + React
- **Infrastructure**: Docker Compose & Kubernetes (KEDA)

---

## 🚀 Getting Started (Local Development)

The easiest way to run the full stack locally is via Docker Compose.

### 1. Prerequisites
- Docker & Docker Desktop (or Docker Engine)
- Git

### 2. Environment Variables
Create a `.env` file in the root directory (alongside `docker-compose.yml`) and add your LLM API keys:
```env
# Example using Gemini
GEMINI_API_KEY=your_api_key_here
LLM_MODEL=gemini/gemini-1.5-flash-latest

# (Optional) If you want to use OpenAI or Anthropic:
# OPENAI_API_KEY=your_api_key_here
# ANTHROPIC_API_KEY=your_api_key_here
```

### 3. Spin up the cluster
Run the following command to build the images and start the services:
```bash
docker-compose up -d --build
```

### 4. Access the Services
Once booted, the following services will be available:
- **Web Frontend**: [http://localhost:5173](http://localhost:5173)
- **FastAPI Backend**: [http://localhost:8000](http://localhost:8000) (Swagger UI at `/docs`)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) *(Login: `minioadmin` / `minioadminpassword`)*

---

## ☸️ Kubernetes Deployment (MicroK8s)

DMAF is designed for dynamic horizontal scaling. The `k8s/` directory contains all necessary manifests.

1. **Enable required MicroK8s addons:**
   ```bash
   microk8s enable registry keda
   ```

2. **Build and push images to local registry:**
   Build the backend, frontend, and mcp-server images and tag them for your local MicroK8s registry (`localhost:32000/dmaf-api:latest`, etc.), then push them.

3. **Apply the Infrastructure & Application Manifests:**
   ```bash
   kubectl apply -f k8s/infrastructure.yaml
   kubectl apply -f k8s/apps.yaml
   ```

4. **Enable Event-Driven Autoscaling (KEDA):**
   ```bash
   kubectl apply -f k8s/keda-scaledobject.yaml
   ```
   *The `agent-worker` pods will now automatically scale based on the volume of subtasks in the Redis queue!*

---

## 📂 Project Structure

```text
DMAF/
├── backend/
│   └── app/
│       ├── api/          # FastAPI Routes
│       ├── core/         # DB Connections & Config
│       ├── models/       # SQLAlchemy Domain Models
│       ├── agent/        # LangGraph State Machine Logic
│       ├── workers/      # Redis Stream Consumers (DLQ logic)
│       ├── llm/          # Multi-provider LLM wrappers
│       └── scripts/      # DB Initialization 
├── frontend/             # Vite + React UI
├── mcp-tools/            # Independent MCP Tool Server
├── k8s/                  # Kubernetes & KEDA Manifests
└── docker-compose.yml    # Local Orchestration
```
