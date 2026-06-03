import os
import asyncio
from typing import TypedDict, Annotated, Sequence
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_litellm import ChatLiteLLM
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import operator
import logging
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools

logger = logging.getLogger(__name__)

# State for the LangGraph
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]

# Determine the model from env, default to gemini
MODEL_NAME = os.environ.get("LLM_MODEL", "gemini/gemini-3.5-flash")
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/sse")

# Initialize ChatLiteLLM
llm = ChatLiteLLM(model=MODEL_NAME, temperature=0.7, max_tokens=4096)

# Keep global references to prevent garbage collection
_mcp_client = None
_agent_executor = None

async def setup_agent():
    global _mcp_client, _agent_executor
    if _agent_executor is not None:
        return _agent_executor

    logger.info(f"Connecting to MCP server at {MCP_SERVER_URL}")
    mcp_config = {
        "mcp-server": {
            "transport": "sse",
            "url": MCP_SERVER_URL,
        }
    }
    
    try:
        _mcp_client = MultiServerMCPClient(mcp_config)
        await _mcp_client.__aenter__()
        tools = await load_mcp_tools(_mcp_client)
        logger.info(f"Loaded MCP tools: {[t.name for t in tools]}")
    except Exception as e:
        logger.error(f"Failed to load MCP tools: {e}")
        tools = []

    if tools:
        agent_llm = llm.bind_tools(tools)
    else:
        agent_llm = llm

    # The agent node
    async def run_agent(state: AgentState):
        messages = state['messages']
        response = await agent_llm.ainvoke(messages)
        return {"messages": [response]}
        
    def should_continue(state: AgentState):
        messages = state['messages']
        last_message = messages[-1]
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            return "tools"
        return END

    # Define the graph
    workflow = StateGraph(AgentState)
    workflow.add_node("agent", run_agent)
    
    if tools:
        tool_node = ToolNode(tools)
        workflow.add_node("tools", tool_node)
        workflow.add_conditional_edges("agent", should_continue)
        workflow.add_edge("tools", "agent")
    else:
        workflow.add_edge("agent", END)

    workflow.set_entry_point("agent")

    # Compile the graph
    _agent_executor = workflow.compile()
    return _agent_executor


async def invoke_agent(prompt: str, history: list = None) -> str:
    """Invokes the agent with a prompt and optional history."""
    executor = await setup_agent()
    
    messages = history or []
    messages.append(HumanMessage(content=prompt))
    
    # Run the graph
    result = await executor.ainvoke({"messages": messages})
    
    # The last message is from the AI
    ai_response = result['messages'][-1].content
    return ai_response
