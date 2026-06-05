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

STREAM_NAME = "worker_tasks"
GROUP_NAME = "worker_group"
CONSUMER_NAME = "agent_worker_1"

async def setup_redis():
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating group: {e}")

async def process_tasks():
    await setup_redis()
    logger.info("Started Agent Worker...")
    
    while True:
        try:
            response = await redis_client.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=2000
            )
            
            if response:
                for stream, messages in response:
                    for message_id, message_data in messages:
                        try:
                            task_id = message_data.get(b"task_id").decode("utf-8")
                            prompt = message_data.get(b"prompt").decode("utf-8")
                            profile_name = message_data.get(b"profile_name", b"default").decode("utf-8")
                            parent_task_id = message_data.get(b"parent_task_id", b"").decode("utf-8") if b"parent_task_id" in message_data else None
                            subtask_id = message_data.get(b"subtask_id", b"").decode("utf-8") if b"subtask_id" in message_data else None
                            
                            logger.info(f"Processing subtask {task_id}: {prompt} with profile {profile_name}")
                            
                            async with AsyncSessionLocal() as session:
                                result = await session.execute(select(Profile).where(Profile.name == profile_name))
                                profile = result.scalars().first()
                                
                            try:
                                result = await invoke_agent(prompt, profile=profile, task_id=task_id)
                                
                                if parent_task_id and subtask_id:
                                    ensure_bucket()
                                    obj_name = f"{parent_task_id}/{subtask_id}.txt"
                                    result_bytes = result.encode('utf-8')
                                    minio_client.put_object("agent-outputs", obj_name, io.BytesIO(result_bytes), len(result_bytes))
                                    s3_url = f"s3://agent-outputs/{obj_name}"
                                    
                                    async with AsyncSessionLocal() as session:
                                        await session.execute(update(SubtaskItem).where(SubtaskItem.id == subtask_id).values(status="complete", s3_url=s3_url))
                                        await session.commit()
                                        
                            except Exception as sub_e:
                                logger.error(f"Error executing subtask {task_id}: {sub_e}")
                                result = str(sub_e)
                                if parent_task_id and subtask_id:
                                    async with AsyncSessionLocal() as session:
                                        await session.execute(update(SubtaskItem).where(SubtaskItem.id == subtask_id).values(status="failed", s3_url=""))
                                        await session.commit()
                                else:
                                    raise sub_e
                            
                            # DAG Resolution Loop with Pessimistic Locking
                            if parent_task_id and subtask_id:
                                async with AsyncSessionLocal() as session:
                                    # Use with_for_update() to lock the rows so other workers don't race
                                    res = await session.execute(
                                        select(SubtaskItem)
                                        .where(SubtaskItem.parent_task_id == parent_task_id)
                                        .with_for_update()
                                    )
                                    siblings = res.scalars().all()
                                    sibling_map = {s.id: s for s in siblings}
                                    
                                    newly_ready = []
                                    changed = True
                                    while changed:
                                        changed = False
                                        for s in siblings:
                                            if s.status == "waiting" and s.dependencies:
                                                dep_statuses = [sibling_map[d].status for d in s.dependencies if d in sibling_map]
                                                if "failed" in dep_statuses:
                                                    s.status = "failed"
                                                    await session.execute(update(SubtaskItem).where(SubtaskItem.id == s.id).values(status="failed"))
                                                    changed = True
                                                elif all(status == "complete" for status in dep_statuses):
                                                    s.status = "queued"
                                                    await session.execute(update(SubtaskItem).where(SubtaskItem.id == s.id).values(status="queued"))
                                                    newly_ready.append(s)
                                                    changed = True
                                                    
                                    all_finished = all(s.status in ["complete", "failed"] for s in siblings)
                                    await session.commit()
                                    
                                    # Queue newly ready tasks
                                    for task in newly_ready:
                                        context_texts = []
                                        for d in task.dependencies:
                                            dep_task = sibling_map.get(d)
                                            if dep_task and dep_task.s3_url:
                                                obj_name = dep_task.s3_url.replace("s3://agent-outputs/", "")
                                                try:
                                                    resp = minio_client.get_object("agent-outputs", obj_name)
                                                    context_texts.append(f"--- Output of prerequisite task '{dep_task.description}': ---\n{resp.read().decode('utf-8')}")
                                                except Exception as e:
                                                    pass
                                                finally:
                                                    try:
                                                        resp.close()
                                                        resp.release_conn()
                                                    except:
                                                        pass
                                        
                                        enriched_prompt = task.description
                                        if context_texts:
                                            enriched_prompt += "\n\nContext from prerequisite tasks:\n" + "\n\n".join(context_texts)
                                            
                                        await redis_client.xadd("worker_tasks", {
                                            "task_id": task.id,
                                            "parent_task_id": parent_task_id,
                                            "subtask_id": task.id,
                                            "prompt": enriched_prompt,
                                            "profile_name": task.profile_name
                                        })
                                        
                                if all_finished:
                                    logger.info(f"All subtasks finished for {parent_task_id}. Dispatching resume action.")
                                    await redis_client.xadd("orchestrator_tasks", {
                                        "task_id": parent_task_id,
                                        "action": "resume"
                                    })
                            
                            # Workers don't update parent task status, only their own subtask results (handled above)
                            # But we log it anyway
                            await redis_client.hset(f"task_results:{task_id}", mapping={"status": "completed", "result": result})
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                            
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")
                            if 'task_id' in locals():
                                await redis_client.hset(f"task_results:{task_id}", mapping={"status": "failed", "result": str(e)})
        except Exception as e:
            logger.error(f"Queue error: {e}")
            await asyncio.sleep(1)

async def start_worker():
    await process_tasks()

if __name__ == "__main__":
    asyncio.run(start_worker())
