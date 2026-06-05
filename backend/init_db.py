import asyncio
import os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from db import engine, Base, AsyncSessionLocal
from models import Profile

DEFAULT_WORKFLOW = {
    "entry_point": "agent",
    "nodes": [
        {"name": "agent", "type": "llm"},
        {"name": "tools", "type": "tool_node"}
    ],
    "edges": [
        {"from": "tools", "to": "agent"}
    ],
    "conditional_edges": [
        {"from": "agent", "condition": "should_continue"}
    ]
}

ORCHESTRATOR_WORKFLOW = {
    "entry_point": "planner",
    "nodes": [
        {"name": "planner", "type": "planner"},
        {"name": "dispatcher", "type": "dispatcher"},
        {"name": "reviewer", "type": "reviewer"}
    ],
    "edges": [
        {"from": "planner", "to": "dispatcher"},
        {"from": "dispatcher", "to": "reviewer"}
    ],
    "conditional_edges": [
        {"from": "reviewer", "condition": "should_replan"}
    ]
}

DEFAULT_PROFILES = [
    {
        "name": "default",
        "system_prompt": "You are a helpful AI assistant. You have access to various tools. Use them to answer the user's questions.",
        "mcp_servers": [os.environ.get("MCP_SERVER_URL", "http://mcp-server:8001/sse")],
        "workflow_config": DEFAULT_WORKFLOW
    },
    {
        "name": "researcher",
        "system_prompt": "You are an expert researcher. You focus on finding accurate information from the web and summarizing it clearly.",
        "mcp_servers": [os.environ.get("MCP_SERVER_URL", "http://mcp-server:8001/sse")],
        "workflow_config": DEFAULT_WORKFLOW
    },
    {
        "name": "orchestrator",
        "system_prompt": "You are the Orchestrator. Your job is to break down complex tasks into subtasks and delegate them to other specialized agents. You must build a DAG (Directed Acyclic Graph) of subtasks by assigning local IDs to each subtask, and specifying which other subtasks they depend on in the 'dependencies' array. If a task requires the output of another task to function correctly (e.g., formatting data retrieved by another task), you MUST list the prerequisite task's ID in the dependencies. Independent tasks should have empty dependencies so they run in parallel. You will review their work and ensure the final user request is met.",
        "mcp_servers": [],
        "workflow_config": ORCHESTRATOR_WORKFLOW
    }
]

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        
    async with AsyncSessionLocal() as session:
        for p_data in DEFAULT_PROFILES:
            # Check if exists
            result = await session.execute(select(Profile).where(Profile.name == p_data["name"]))
            if not result.scalars().first():
                profile = Profile(**p_data)
                session.add(profile)
        await session.commit()
        print("Database initialized with default profiles.")

if __name__ == "__main__":
    asyncio.run(init_db())
