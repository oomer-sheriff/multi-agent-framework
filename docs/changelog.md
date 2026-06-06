# Changelog: Distributed Microservices Migration

This document outlines the detailed timeline and execution order for migrating the monolithic LangGraph Orchestrator into an enterprise-grade, event-driven Kubernetes architecture.

## Phase 1: Decoupling the Monolithic Queue
**Objective:** Split the single worker into distinct services so Orchestration and Execution can scale independently.
1. **Queue Segregation:** Introduce two distinct Redis streams: `orchestrator_tasks` and `worker_tasks`.
2. **API Updates:** Modify the FastAPI `main.py` entrypoint to submit user requests to the `orchestrator_tasks` queue.
3. **Split Workers:** Retire `queue_worker.py`. 
   - Create `orchestrator_worker.py` to handle graph checkpointing, planning, and dispatching.
   - Create `agent_worker.py` to handle LLM execution, MCP tools, and DAG state management.
4. **Local Integration:** Update `docker-compose.yml` to launch these as isolated containers.

## Phase 2: Concurrency & Distributed State Management
**Objective:** Prevent race conditions when dynamically spawned worker pods attempt to mutate the same DAG state.
1. **Distributed Locking:** Implement pessimistic locking (via Postgres `FOR UPDATE`) inside `agent_worker.py`. When a subtask finishes, only one pod at a time may evaluate the DAG to determine if downstream tasks should queue.
2. **Cross-Service Resumption:** Prevent `agent_worker.py` from executing Orchestrator logic. When a DAG is fully resolved, the worker will push a `{"action": "resume"}` payload back to the `orchestrator_tasks` queue.

## Phase 3: Reliability & Fault Tolerance
**Objective:** Ensure no tasks are lost if a Kubernetes pod is pre-empted, crashes, or OOMs.
1. **Dead Letter Recovery:** Implement a Redis `XPENDING` background loop in both worker services.
2. **Task Reassignment:** Automatically detect unacknowledged messages older than 10 minutes and re-queue them.

## Phase 4: Kubernetes & KEDA Deployment
**Objective:** Define the infrastructure-as-code for dynamic cluster deployments.
1. **Stateful Resources:** Define K8s deployments and persistent volume claims for Postgres, Redis, and MinIO.
2. **Networking:** Expose the MCP tools via internal ClusterIP services and update database profiles to utilize cluster DNS (`mcp-service.default.svc.cluster.local`).
3. **KEDA Autoscaling:** Write the `ScaledObject` CRDs to map the `worker_tasks` Redis stream length to the replica count of the `agent_worker` Deployment, enabling true 0-to-N autoscaling based on task volume.
