import asyncio
import os
import redis.asyncio as redis
from redis.exceptions import ResponseError
import json
import logging
from agent import invoke_agent, setup_agent
from db import AsyncSessionLocal
from models import Profile
from langgraph.types import Command

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL)

STREAM_NAME = "orchestrator_tasks"
GROUP_NAME = "orchestrator_group"
CONSUMER_NAME = "orch_worker_1"

async def setup_redis():
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating group: {e}")

async def process_tasks():
    await setup_redis()
    logger.info("Started Orchestrator Worker...")
    
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
                            action = message_data.get(b"action", b"").decode("utf-8") if b"action" in message_data else None
                            
                            if action == "resume":
                                logger.info(f"Resuming orchestrator for task {task_id}")
                                async with AsyncSessionLocal() as session:
                                    from sqlalchemy import select
                                    res = await session.execute(select(Profile).where(Profile.name == "orchestrator"))
                                    orch_profile = res.scalars().first()
                                    
                                executor = await setup_agent(orch_profile)
                                final_state = await executor.ainvoke(
                                    Command(resume="workers_done"), 
                                    config={"configurable": {"thread_id": task_id}}
                                )
                                
                                # Extract final result
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
                                
                                await redis_client.hset(
                                    f"task_results:{task_id}",
                                    mapping={"status": "completed", "result": final_result}
                                )
                                logger.info(f"Orchestrator task {task_id} fully completed.")
                            else:
                                prompt = message_data.get(b"prompt").decode("utf-8")
                                profile_name = message_data.get(b"profile_name", b"orchestrator").decode("utf-8")
                                
                                logger.info(f"Processing orchestrator task {task_id}")
                                
                                async with AsyncSessionLocal() as session:
                                    from sqlalchemy import select
                                    result = await session.execute(select(Profile).where(Profile.name == profile_name))
                                    profile = result.scalars().first()
                                    
                                result_text = await invoke_agent(prompt, profile=profile, task_id=task_id)
                                
                                await redis_client.hset(
                                    f"task_results:{task_id}",
                                    mapping={"status": "processing_subtasks", "result": result_text}
                                )
                            
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
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
