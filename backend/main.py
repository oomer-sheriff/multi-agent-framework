import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis.asyncio as redis
from db import get_db
from models import Profile
from init_db import init_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

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
    profile_name: str = "default"

class TaskResponse(BaseModel):
    task_id: str

@app.on_event("startup")
async def startup_event():
    # Initialize Database
    await init_db()

@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: TaskRequest):
    task_id = str(uuid.uuid4())
    
    # Initialize task status FIRST to prevent race condition
    await redis_client.hset(
        f"task_results:{task_id}",
        mapping={"status": "pending", "result": ""}
    )
    
    # Publish to Redis Stream for Orchestrator
    await redis_client.xadd(
        "orchestrator_tasks",
        {"task_id": task_id, "prompt": request.prompt, "profile_name": request.profile_name}
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

class ProfileCreate(BaseModel):
    name: str
    system_prompt: str
    mcp_servers: list[str] = []
    workflow_config: dict

@app.post("/profiles")
async def create_profile(profile: ProfileCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Profile).where(Profile.name == profile.name))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Profile already exists")
    
    db_profile = Profile(
        name=profile.name,
        system_prompt=profile.system_prompt,
        mcp_servers=profile.mcp_servers,
        workflow_config=profile.workflow_config
    )
    db.add(db_profile)
    await db.commit()
    return {"status": "success", "profile": profile.name}

@app.get("/profiles")
async def get_profiles(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Profile))
    profiles = result.scalars().all()
    return [{"name": p.name, "workflow_config": p.workflow_config} for p in profiles]

