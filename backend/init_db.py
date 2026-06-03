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

DEFAULT_PROFILES = [
    {
        "name": "default",
        "system_prompt": "You are a helpful AI assistant. You have access to various tools. Use them to answer the user's questions.",
        "mcp_servers": [os.environ.get("MCP_SERVER_URL", "http://mcp-server:8001/mcp")],
        "workflow_config": DEFAULT_WORKFLOW
    },
    {
        "name": "researcher",
        "system_prompt": "You are an expert researcher. You focus on finding accurate information from the web and summarizing it clearly.",
        "mcp_servers": [os.environ.get("MCP_SERVER_URL", "http://mcp-server:8001/mcp")],
        "workflow_config": DEFAULT_WORKFLOW
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
