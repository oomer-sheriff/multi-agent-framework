# Evaluating Context Window Management for Multi-Agent DAGs

As the complexity of parent tasks increases, passing the full raw text output of prerequisite tasks into downstream agents (and back to the Orchestrator) will inevitably cause context window explosion, token limit errors, and increased latency/cost. 

Here is an evaluation of the feasible architectural solutions to solve this.

---

## Option A: The "RAG / Search Tool" Approach (Your Suggestion)
**How it works:** Downstream agents receive a very brief metadata injection (e.g., *"Prerequisite Task A finished. Output saved to MinIO. Summary: It found 15 sources on gold prices."*). The agent is provided with a `search_previous_output(task_id, query)` tool that performs a semantic/vector search against the MinIO document to return relevant chunks.

- **Pros:** Highly scalable. Context windows stay pristine and tiny.
- **Cons:** High engineering overhead. Requires setting up an embedding model (like `pgvector` which we already have in the DB) to chunk and store outputs before the next task runs. Sometimes LLMs get "lazy" and guess instead of actively querying the tool.

## Option B: The "Read File / Pagination" Tool Approach
**How it works:** Similar to Option A, but instead of full semantic RAG, we simply give agents a `read_output(task_id, start_line, end_line)` tool. We tell the agent: *"Task A generated 5,000 lines of data. Use the read tool to paginate through it."*

- **Pros:** Very easy to implement. Agents are highly accustomed to using file-reading tools.
- **Cons:** If the agent blindly requests `start=0, end=5000`, the context window will still explode.

## Option C: The LLM "Map-Reduce" Summarization Approach
**How it works:** Before a worker saves its final output to MinIO, we force the text through a cheap, ultra-fast LLM pass (using a strict prompt like *"Extract only the core numerical data and conclusions from this text"*). Only this condensed summary is passed to the downstream dependencies.

- **Pros:** Zero tool-calling required by downstream agents. Fast execution.
- **Cons:** Loss of fidelity. If the summarizer deems a specific data point "unimportant", the downstream agent loses access to it forever.

## Option D: Native Context Caching / File APIs (The Gemini Way)
**How it works:** Models like Gemini 1.5 Flash natively support massive context windows (1M-2M tokens) and feature [Context Caching / File APIs]. Instead of injecting text into the prompt string, we upload the prerequisite MinIO outputs directly to the Gemini API as a `File`, and pass the `File URI` to the downstream agent. 

- **Pros:** Zero data loss. No RAG infrastructure needed. Extremely cheap if using Gemini Context Caching.
- **Cons:** Locks the framework into Gemini-specific APIs, breaking compatibility with OpenAI/Anthropic if you plan to support them later.

---

### Recommendation

I recommend a hybrid of **Option B and Option C**. 

1. **The Summarizer:** When a worker finishes, it saves the FULL raw data to MinIO, but generates a 200-word **Index/Summary** to pass into the prompt of downstream dependencies.
2. **The Retrieval Tool:** We write a simple FastMCP tool called `read_task_output` that allows downstream agents (and the Orchestrator) to explicitly read the full raw data from MinIO if their task requires granular details missing from the summary.

**Is this the direction you'd like to go? If so, I can draft an implementation plan to build the Summarizer and the MCP Tool.**
