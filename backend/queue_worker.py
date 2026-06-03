import asyncio
import os
import redis.asyncio as redis
from redis.exceptions import ResponseError
import json
import logging
from agent import invoke_agent

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
                            
                            logger.info(f"Processing task {task_id}: {prompt}")
                            
                            # Invoke LangGraph agent
                            result = await invoke_agent(prompt)
                            
                            # Save result to Redis Hash
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
