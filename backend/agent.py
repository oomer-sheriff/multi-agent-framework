import os
import asyncio
from typing import TypedDict, Annotated, Sequence
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_litellm import ChatLiteLLM
from langgraph.graph import StateGraph, END
from models import Profile
from langgraph.prebuilt import ToolNode
import operator
import logging
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from pydantic import BaseModel, Field
from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool
import uuid
import redis.asyncio as redis

redis_client = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))

logger = logging.getLogger(__name__)

class Subtask(BaseModel):
    description: str = Field(description="The task to perform")
    profile: str = Field(description="The agent profile to use (e.g., 'researcher' or 'default')")

class Plan(BaseModel):
    subtasks: list[Subtask] = Field(description="List of subtasks to complete the goal")

class Review(BaseModel):
    is_complete: bool = Field(description="Whether the original request has been fully satisfied")
    feedback: str = Field(description="Feedback on what is missing or incorrect, if any")

# State for the LangGraph
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    plan: list[dict]
    results: list[dict]
    review: dict

# Determine the model from env, default to gemini
MODEL_NAME = os.environ.get("LLM_MODEL", "gemini/gemma-4-26b-a4b-it")
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/mcp")

# Initialize ChatLiteLLM
llm = ChatLiteLLM(model=MODEL_NAME, temperature=0.7, max_tokens=4096)

# Keep global references to prevent garbage collection
_mcp_clients = {}
_agent_executors = {}

async def setup_agent(profile: Profile):
    global _mcp_clients, _agent_executors
    if profile.name in _agent_executors:
        return _agent_executors[profile.name]

    tools = []
    
    # Connect to MCP servers based on profile
    for mcp_url in profile.mcp_servers:
        logger.info(f"Connecting to MCP server at {mcp_url} for profile {profile.name}")
        mcp_config = {
            f"mcp-server-{profile.name}": {
                "transport": "sse",
                "url": mcp_url,
            }
        }
        try:
            client = MultiServerMCPClient(mcp_config)
            server_tools = await client.get_tools()
            tools.extend(server_tools)
            _mcp_clients[profile.name] = client # Keep reference
            logger.info(f"Loaded MCP tools for {profile.name}: {[t.name for t in server_tools]}")
        except Exception as e:
            logger.error(f"Failed to load MCP tools from {mcp_url}: {e}")

    if tools:
        agent_llm = llm.bind_tools(tools)
    else:
        agent_llm = llm

    # Handlers for dynamic nodes
    async def run_agent(state: AgentState):
        messages = state['messages']
        logger.info(f"[run_agent] Input messages: {[m.content for m in messages if hasattr(m, 'content')]}")
        response = await agent_llm.ainvoke(messages)
        logger.info(f"[run_agent] Output response: {response.content if hasattr(response, 'content') else str(response)}")
        return {"messages": [response]}
        
    async def run_planner(state: AgentState):
        messages = state['messages']
        planner_llm = agent_llm.with_structured_output(Plan)
        prompt = messages[-1].content
        logger.info(f"[run_planner] Creating plan for prompt: {prompt}")
        plan = await planner_llm.ainvoke(messages)
        logger.info(f"[run_planner] Generated plan: {plan}")
        return {"plan": [s.model_dump() for s in plan.subtasks]}
        
    async def run_dispatcher(state: AgentState, config: RunnableConfig):
        from db import AsyncSessionLocal
        from models import SubtaskItem, Profile
        from sqlalchemy import select
        
        task_id = config.get("configurable", {}).get("thread_id")
        if not task_id:
            logger.error("No thread_id found in config. Cannot dispatch subtasks.")
            return {"results": []}
            
        plan = state.get('plan', [])
        if not plan:
            logger.warning("[run_dispatcher] No plan found in state!")
            return {"messages": [AIMessage(content="I couldn't create a plan.")]}
            
        logger.info(f"[run_dispatcher] Dispatching {len(plan)} subtasks for {task_id}")
        
        # Check if we already have subtasks for this task_id
        async with AsyncSessionLocal() as session:
            res = await session.execute(select(SubtaskItem).where(SubtaskItem.parent_task_id == task_id))
            existing_subtasks = res.scalars().all()
            
        if not existing_subtasks:
            async with AsyncSessionLocal() as session:
                for subtask in plan:
                    subtask_id = str(uuid.uuid4())
                    
                    res = await session.execute(select(Profile).where(Profile.name == subtask['profile']))
                    worker_profile = res.scalars().first()
                    p_name = worker_profile.name if worker_profile else "default"
                    
                    item = SubtaskItem(
                        id=subtask_id,
                        parent_task_id=task_id,
                        description=subtask['description'],
                        profile_name=p_name
                    )
                    session.add(item)
                    
                    await redis_client.xadd("agent_tasks", {
                        "task_id": subtask_id,
                        "parent_task_id": task_id,
                        "subtask_id": subtask_id,
                        "prompt": subtask['description'],
                        "profile_name": p_name
                    })
                await session.commit()
                
            interrupt("Waiting for workers to finish")
            
        # Fetch results
        results = []
        async with AsyncSessionLocal() as session:
            res = await session.execute(select(SubtaskItem).where(SubtaskItem.parent_task_id == task_id))
            all_subtasks = res.scalars().all()
            
            from minio import Minio
            minio_client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadminpassword", secure=False)
            
            for s in all_subtasks:
                if s.status == "complete" and s.s3_url:
                    obj_name = s.s3_url.replace("s3://agent-outputs/", "")
                    try:
                        resp = minio_client.get_object("agent-outputs", obj_name)
                        result_text = resp.read().decode('utf-8')
                        results.append({
                            "task": s.description,
                            "profile": s.profile_name,
                            "result": result_text
                        })
                    except Exception as e:
                        logger.error(f"Error fetching from MinIO: {e}")
                    finally:
                        try:
                            resp.close()
                            resp.release_conn()
                        except:
                            pass
                            
        return {"results": results}
        
    async def run_reviewer(state: AgentState):
        messages = state['messages']
        results = state.get('results', [])
        
        # Format results for the reviewer
        results_str = "\\n".join([f"Task ({r['profile']}): {r['task']}\\nResult: {r['result']}" for r in results])
        review_prompt = f"Original Request:\\n{messages[0].content}\\n\\nExecution Results:\\n{results_str}\\n\\nReview the results. Have we fully satisfied the original request?"
        
        reviewer_llm = agent_llm.with_structured_output(Review)
        review = await reviewer_llm.ainvoke([HumanMessage(content=review_prompt)])
        
        if review.is_complete:
            # Generate final response
            final_prompt = f"The tasks are complete. Based on these results:\\n{results_str}\\n\\nProvide the final answer to the user."
            response = await agent_llm.ainvoke([HumanMessage(content=final_prompt)])
            return {"review": review.model_dump(), "messages": [response]}
        else:
            # Return feedback to trigger replan
            feedback_msg = AIMessage(content=f"Review Feedback: {review.feedback}. We need to adjust our plan and continue.")
            return {"review": review.model_dump(), "messages": [feedback_msg]}
        
    def should_continue(state: AgentState):
        messages = state['messages']
        last_message = messages[-1]
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            return "tools"
        return END
        
    def should_replan(state: AgentState):
        review = state.get('review', {})
        if review.get('is_complete'):
            return END
        return "planner"

    # Define the graph
    workflow = StateGraph(AgentState)
    workflow_config = profile.workflow_config
    
    # 1. Build Nodes
    for node in workflow_config.get("nodes", []):
        node_name = node["name"]
        node_type = node["type"]
        
        if node_type == "llm":
            workflow.add_node(node_name, run_agent)
        elif node_type == "tool_node":
            if tools:  # Only add tool node if tools exist
                workflow.add_node(node_name, ToolNode(tools))
        elif node_type == "planner":
            workflow.add_node(node_name, run_planner)
        elif node_type == "dispatcher":
            workflow.add_node(node_name, run_dispatcher)
        elif node_type == "reviewer":
            workflow.add_node(node_name, run_reviewer)
    
    # 2. Build Conditional Edges
    for c_edge in workflow_config.get("conditional_edges", []):
        from_node = c_edge["from"]
        condition = c_edge["condition"]
        
        if condition == "should_continue":
            workflow.add_conditional_edges(from_node, should_continue)
        elif condition == "should_replan":
            workflow.add_conditional_edges(from_node, should_replan)
            
    # 3. Build Standard Edges
    for edge in workflow_config.get("edges", []):
        from_node = edge["from"]
        to_node = edge["to"]
        
        if to_node == "END":
            workflow.add_edge(from_node, END)
        else:
            # If the destination is tools but we have no tools, don't add the explicit edge
            # (though in our default topology tools only routes back to agent, which is fine to skip)
            if to_node == "tools" and not tools:
                continue
            if from_node == "tools" and not tools:
                continue
            workflow.add_edge(from_node, to_node)
            
    # If no conditional edges were added for the agent and tools don't exist, we must add an edge to END
    if not tools and not workflow_config.get("conditional_edges", []):
        workflow.add_edge("agent", END)

    workflow.set_entry_point(workflow_config.get("entry_point", "agent"))

    # Setup Checkpointer
    db_url = os.environ.get("DATABASE_URL", "postgresql://admin:password@db:5432/agent_db")
    pool = AsyncConnectionPool(
        db_url.replace("postgresql+asyncpg", "postgresql"), 
        kwargs={"autocommit": True}, 
        open=False
    )
    await pool.open()
    checkpointer = AsyncPostgresSaver(pool)
    await checkpointer.setup()

    # Compile the graph
    executor = workflow.compile(checkpointer=checkpointer)
    _agent_executors[profile.name] = executor
    return executor


async def invoke_agent(prompt: str, history: list = None, profile: Profile = None, task_id: str = None) -> str:
    """Invokes the agent with a prompt and optional history."""
    if profile is None:
        raise ValueError("Profile is required")
        
    executor = await setup_agent(profile)
    
    if not task_id:
        task_id = str(uuid.uuid4())
        
    config = {"configurable": {"thread_id": task_id}}
    
    messages = history or []
    # Ensure system prompt is first
    if not messages or not isinstance(messages[0], SystemMessage):
        messages.insert(0, SystemMessage(content=profile.system_prompt))
        
    messages.append(HumanMessage(content=prompt))
    
    # Run the graph
    logger.info(f"Invoking graph with task_id: {task_id}")
    result = await executor.ainvoke({"messages": messages}, config=config)
    logger.info(f"Graph result keys: {result.keys() if result else 'None'}")
    
    if not result or 'messages' not in result or not result['messages']:
        logger.error(f"Graph result was empty or had no messages: {result}")
        return "Error: Graph did not return any messages. The AI model may have failed or hit a rate limit."
        
    # The last message is from the AI
    last_message = result['messages'][-1]
    
    # If the execution was paused by an interrupt, the last message is just the user prompt
    if profile.name == "orchestrator" and getattr(last_message, 'type', '') == 'human':
        return "Processing subtasks..."
        
    ai_response = last_message.content if hasattr(last_message, 'content') else str(last_message)
    
    # Langchain message content can sometimes be a list of blocks (e.g., Gemini)
    if isinstance(ai_response, list):
        text_parts = []
        for part in ai_response:
            if isinstance(part, dict) and "text" in part:
                text_parts.append(part["text"])
            elif isinstance(part, str):
                text_parts.append(part)
            else:
                text_parts.append(str(part))
        ai_response = "\n".join(text_parts)
    elif not isinstance(ai_response, str):
        ai_response = str(ai_response)
        
    return ai_response
