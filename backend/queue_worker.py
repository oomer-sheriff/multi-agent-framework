import asyncio
import os
import redis.asyncio as redis
from redis.exceptions import ResponseError
import json
import logging
from agent import invoke_agent, setup_agent
from db import AsyncSessionLocal
from models import Profile, SubtaskItem
from sqlalchemy import select, update
from minio import Minio
import io
from langgraph.types import Command

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadminpassword",
    secure=False
)

def ensure_bucket():
    if not minio_client.bucket_exists("agent-outputs"):
        minio_client.make_bucket("agent-outputs")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL)

STREAM_NAME = "agent_tasks"
GROUP_NAME = "agent_group"
CONSUMER_NAME = "worker_1"

async def setup_redis():
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating group: {e}")

async def process_tasks():
    await setup_redis()
    logger.info("Started Queue Worker...")
    
    while True:
        try:
            # Read messages from the stream
            response = await redis_client.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=2000
            )
            
            if response:
                for stream, messages in response:
                    for message_id, message_data in messages:
                        try:
                            # Process the message
                            task_id = message_data.get(b"task_id").decode("utf-8")
                            prompt = message_data.get(b"prompt").decode("utf-8")
                            profile_name = message_data.get(b"profile_name", b"default").decode("utf-8")
                            parent_task_id = message_data.get(b"parent_task_id", b"").decode("utf-8") if b"parent_task_id" in message_data else None
                            subtask_id = message_data.get(b"subtask_id", b"").decode("utf-8") if b"subtask_id" in message_data else None
                            
                            logger.info(f"Processing task {task_id}: {prompt} with profile {profile_name}")
                            
                            # Fetch Profile from DB
                            async with AsyncSessionLocal() as session:
                                result = await session.execute(select(Profile).where(Profile.name == profile_name))
                                profile = result.scalars().first()
                                
                            if not profile:
                                raise ValueError(f"Profile {profile_name} not found")
                            
                            try:
                                # Invoke LangGraph agent
                                result = await invoke_agent(prompt, profile=profile, task_id=task_id)
                                
                                if parent_task_id and subtask_id:
                                    ensure_bucket()
                                    obj_name = f"{parent_task_id}/{subtask_id}.txt"
                                    result_bytes = result.encode('utf-8')
                                    minio_client.put_object(
                                        "agent-outputs",
                                        obj_name,
                                        io.BytesIO(result_bytes),
                                        len(result_bytes)
                                    )
                                    s3_url = f"s3://agent-outputs/{obj_name}"
                                    
                                    async with AsyncSessionLocal() as session:
                                        await session.execute(
                                            update(SubtaskItem)
                                            .where(SubtaskItem.id == subtask_id)
                                            .values(status="complete", s3_url=s3_url)
                                        )
                                        await session.commit()
                                        
                            except Exception as sub_e:
                                logger.error(f"Error executing subtask {task_id}: {sub_e}")
                                result = str(sub_e)
                                if parent_task_id and subtask_id:
                                    async with AsyncSessionLocal() as session:
                                        await session.execute(
                                            update(SubtaskItem)
                                            .where(SubtaskItem.id == subtask_id)
                                            .values(status="failed", s3_url="")
                                        )
                                        await session.commit()
                                else:
                                    raise sub_e
                            
                            # Check if we need to resume the orchestrator
                            if parent_task_id and subtask_id:
                                async with AsyncSessionLocal() as session:
                                    res = await session.execute(
                                        select(SubtaskItem).where(SubtaskItem.parent_task_id == parent_task_id)
                                    )
                                    siblings = res.scalars().all()
                                    
                                    # LOG EXACT STATUSES
                                    status_counts = {}
                                    for s in siblings:
                                        status_counts[s.status] = status_counts.get(s.status, 0) + 1
                                    logger.info(f"Siblings status for {parent_task_id}: {status_counts}")
                                    
                                    all_finished = all(s.status in ["complete", "failed"] for s in siblings)
                                    
                                if all_finished:
                                    logger.info(f"All subtasks finished for {parent_task_id}. Resuming orchestrator.")
                                    async with AsyncSessionLocal() as session:
                                        res = await session.execute(select(Profile).where(Profile.name == "orchestrator"))
                                        orch_profile = res.scalars().first()
                                        
                                    executor = await setup_agent(orch_profile)
                                    final_state = await executor.ainvoke(
                                        Command(resume="workers_done"), 
                                        config={"configurable": {"thread_id": parent_task_id}}
                                    )
                                    
                                    # Extract the final result from the orchestrator's state
                                    final_result_message = final_state['messages'][-1]
                                    final_result_content = final_result_message.content if hasattr(final_result_message, 'content') else str(final_result_message)
                                    
                                    if isinstance(final_result_content, list):
                                        text_parts = []
                                        for part in final_result_content:
                                            if isinstance(part, dict) and "text" in part:
                                                text_parts.append(part["text"])
                                            elif isinstance(part, str):
                                                text_parts.append(part)
                                            else:
                                                text_parts.append(str(part))
                                        final_result = "\n".join(text_parts)
                                    elif not isinstance(final_result_content, str):
                                        final_result = str(final_result_content)
                                    else:
                                        final_result = final_result_content
                                    
                                    # Write final result to Redis hash
                                    await redis_client.hset(
                                        f"task_results:{parent_task_id}",
                                        mapping={"status": "completed", "result": final_result}
                                    )
                                    logger.info(f"Orchestrator task {parent_task_id} fully completed.")
                            
                            # Save result to Redis Hash
                            if profile_name == "orchestrator":
                                # Don't mark as completed, it's just paused.
                                await redis_client.hset(
                                    f"task_results:{task_id}",
                                    mapping={"status": "processing_subtasks", "result": result}
                                )
                            else:
                                await redis_client.hset(
                                    f"task_results:{task_id}",
                                    mapping={"status": "completed", "result": result}
                                )
                            
                            # Acknowledge the message
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                            logger.info(f"Completed task {task_id}")
                            
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")
                            if 'task_id' in locals():
                                await redis_client.hset(
                                    f"task_results:{task_id}",
                                    mapping={"status": "failed", "result": str(e)}
                                )
        except Exception as e:
            logger.error(f"Queue error: {e}")
            await asyncio.sleep(1)

async def start_worker():
    await process_tasks()

if __name__ == "__main__":
    asyncio.run(start_worker())
