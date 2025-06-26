# Spark History Server MCP Server

Welcome to the Spark History Server MCP Server! This tool bridges Apache Spark's history data with LLM-powered analysis through the Model Context Protocol (MCP).

This MCP server exposes most Apache History Server REST APIs as tools, allowing you to analyze Spark job performance data through natural language interactions. Since Apache Spark History server supports read operations only, this tool is likewise focused on data retrieval and analysis rather than modifications.

In addition to standard REST APIs, we've included enhanced analytics capabilities such as identifying the n slowest jobs, pinpointing bottleneck stages, and generating comprehensive executor metric summaries.

Some use cases for this include:
- Investigating job failure scenarios with natural language queries
- Identifying performance bottlenecks in complex Spark applications
- Optimizing jobs based on historical execution data
- Extracting insights from Spark metrics without writing custom queries

This MCP server was tested with Qwen3 32B and 235B, but should work with other LLMs that support the Model Context Protocol.

# Getting started

## Prerequisites

- Podman or Docker
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

Python version and dependencies are available in [`.python-version`](./.python-version) and [`pyproject.toml`](./pyproject.toml).


## Usage
### Setting Up Your Environment

**Step 1: Prepare the Spark History Server**

This MCP server requires a running Spark History Server to connect to. For your convenience:
- We've included example Spark event data in the repo
- Location: `examples/basic/events` directory
- These sample events will help you test the setup

Run the following command to launch a Spark History Server with our sample data:


```bash
# This command:
# - Uses Docker to run Apache Spark 3.5.5
# - Mounts your local examples directory
# - Exposes port 18080 for the History Server
# - Configures the server using our example configuration

docker run -it \
  -v $(pwd)/examples/basic:/mnt/data \
  -p 18080:18080 \
  docker.io/apache/spark:3.5.5 \
  /opt/java/openjdk/bin/java \
  -cp '/opt/spark/conf:/opt/spark/jars/*' \
  -Xmx1g \
  org.apache.spark.deploy.history.HistoryServer \
  --properties-file /mnt/data/history-server.conf
```

Once running, the History Server will be available at http://localhost:18080 

**Step 2: Launch the MCP Server**

With the History Server running, start the MCP server in a new terminal:

```bash
# Make sure uv is installed before running this command
uv run main.py
```
 
The MCP server will start on port 18888 by default. 

**Step 3: Run the MCP Inspector Tool**

MCP Inspector is a debugging tool that helps you interact with MCP-enabled services: 

```bash
# Note: Disabling auth is only recommended for local development
DANGEROUSLY_OMIT_AUTH=true npx @modelcontextprotocol/inspector
```

⚠️ Security Note: The above command disables authentication for testing. Never use this setting in production environments. 

**Step 4: Connect to the Inspector Interface**

Open your browser and navigate to: http://localhost:6274  
 
Configure the connection: 
1. In the Transport Type dropdown, select Streamable HTTP
2. Set the URL field to: http://localhost:18888/mcp
3. Click the Connect button


**Step 5: Explore Available Tools**

Once connected:
1. Navigate to the Tools tab
2. Click the List Tools button to see available operations
3. You should see a list of tools for interacting with Spark History data
     

**Step 6: Try a Sample Query**

Let's test the setup by retrieving application details: 

1. Select the get_application tool from the list
2. In the application_id field, enter: spark-bcec39f6201b42b9925124595baad260
3. Click Run Tool

You should see a JSON response containing details about the Spark application. 

### Configuration Options

Configuration is done through a configuration file called [`config.yaml`](./config.yaml).
