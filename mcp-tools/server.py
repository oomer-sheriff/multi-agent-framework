import requests
from fastmcp import FastMCP
from duckduckgo_search import DDGS
import ast
import os
from minio import Minio

# Initialize FastMCP
mcp = FastMCP("Basic Tools Server")

@mcp.tool()
def search_web(query: str) -> str:
    """Searches the web using DuckDuckGo for the given query."""
    print(f"[MCP Tool Called] search_web with query: {query}", flush=True)
    try:
        results = DDGS().text(query, max_results=3)
        return "\n\n".join([f"Title: {r['title']}\nSnippet: {r['body']}\nURL: {r['href']}" for r in results])
    except Exception as e:
        return f"Error searching the web: {e}"

@mcp.tool()
def fetch_url(url: str) -> str:
    """Fetches the content of a URL."""
    print(f"[MCP Tool Called] fetch_url with url: {url}", flush=True)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text[:2000] # Return first 2000 chars
    except Exception as e:
        return f"Error fetching URL: {e}"

@mcp.tool()
def calculate(expression: str) -> str:
    """Safely evaluates a basic mathematical expression."""
    try:
        # Very simple safe eval for math
        allowed_names = {"__builtins__": {}}
        node = ast.parse(expression, mode='eval')
        return str(eval(compile(node, '<string>', 'eval'), allowed_names))
    except Exception as e:
        return f"Error evaluating math: {e}"

@mcp.tool()
def read_task_output(parent_task_id: str, subtask_id: str) -> str:
    """Reads the full raw text output of a previously executed subtask from MinIO."""
    print(f"[MCP Tool Called] read_task_output for {parent_task_id}/{subtask_id}", flush=True)
    try:
        minio_client = Minio(
            os.environ.get("MINIO_URL", "minio:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadminpassword"),
            secure=False
        )
        obj_name = f"{parent_task_id}/{subtask_id}.txt"
        resp = minio_client.get_object("agent-outputs", obj_name)
        data = resp.read().decode('utf-8')
        return data
    except Exception as e:
        return f"Error reading task output from MinIO: {e}"
    finally:
        try:
            resp.close()
            resp.release_conn()
        except:
            pass

if __name__ == "__main__":
    # Run over HTTP with Server-Sent Events on port 8001
    mcp.run(transport="sse", host="0.0.0.0", port=8001)
