import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis.asyncio as redis
from queue_worker import process_tasks

app = FastAPI(title="LangGraph Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL)

STREAM_NAME = "agent_tasks"

class TaskRequest(BaseModel):
    prompt: str

class TaskResponse(BaseModel):
    task_id: str

@app.on_event("startup")
async def startup_event():
    # Start the background worker
    asyncio.create_task(process_tasks())

@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: TaskRequest):
    task_id = str(uuid.uuid4())
    
    # Initialize task status FIRST to prevent race condition
    await redis_client.hset(
        f"task_results:{task_id}",
        mapping={"status": "pending", "result": ""}
    )
    
    # Push to redis stream
    await redis_client.xadd(
        STREAM_NAME,
        {"task_id": task_id, "prompt": request.prompt}
    )
    
    return {"task_id": task_id}

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    data = await redis_client.hgetall(f"task_results:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "task_id": task_id,
        "status": data.get(b"status").decode("utf-8"),
        "result": data.get(b"result").decode("utf-8")
    }

@app.get("/")
def health_check():
    return {"status": "ok"}
