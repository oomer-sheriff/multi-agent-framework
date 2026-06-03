import os
import asyncio
from typing import TypedDict, Annotated, Sequence
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_litellm import ChatLiteLLM
from langgraph.graph import StateGraph, END
from models import Profile
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
MODEL_NAME = os.environ.get("LLM_MODEL", "gemini/gemini-3.1-flash")
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
    
    # 2. Build Conditional Edges
    for c_edge in workflow_config.get("conditional_edges", []):
        from_node = c_edge["from"]
        condition = c_edge["condition"]
        
        if condition == "should_continue":
            workflow.add_conditional_edges(from_node, should_continue)
            
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

    # Compile the graph
    executor = workflow.compile()
    _agent_executors[profile.name] = executor
    return executor


async def invoke_agent(prompt: str, history: list = None, profile: Profile = None) -> str:
    """Invokes the agent with a prompt and optional history."""
    if profile is None:
        raise ValueError("Profile is required")
        
    executor = await setup_agent(profile)
    
    messages = history or []
    # Ensure system prompt is first
    if not messages or not isinstance(messages[0], SystemMessage):
        messages.insert(0, SystemMessage(content=profile.system_prompt))
        
    messages.append(HumanMessage(content=prompt))
    
    # Run the graph
    result = await executor.ainvoke({"messages": messages})
    
    # The last message is from the AI
    ai_response = result['messages'][-1].content
    return ai_response
