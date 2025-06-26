
# Spark History Server MCP Server

This is a MCP server that exposes most Apache History Server REST APIs as tools. Apache Spark History server supports read operations only so ths tool can only do read operations.

In addition to REST APIs, it has additional operations available such as getting the n slowest jobs, n slowest stages, and a summary of excutor metrics.

This MCP server was tested with Qwen3 32B and 235B.

## Getting started

### Basic Demo

Let's look at a basic example application. There's are Spark events from an example app in the `examples/basic/events` directory.

1. Run Spark History Server by mounting the example directory.

    ```bash
    docker run -it -v $(pwd)/examples/basic:/mnt/data  -p 18080:18080 docker.io/apache/spark:3.5.5 /opt/java/openjdk/bin/java -cp '/opt/spark/conf:/opt/spark/jars/*' -Xmx1g org.apache.spark.deploy.history.HistoryServer --properties-file /mnt/data/history-server.conf
    ```
2. Run the MCP server

    Be sure to install uv before running this command.

    ```bash
    uv run main.py
    ```
3. Run MCP Inspector

    Note that we are disabling auth for this example. Do not do this for non-local environments.

    ```bash
    DANGEROUSLY_OMIT_AUTH=true npx @modelcontextprotocol/inspector
    ```
4. Open [`http://localhost:6274`](http://localhost:6274)
6. In the `Transport Type` dropdown, select "Streamable HTTP".
5. In the URL field, set it to be `http://localhost:18888/mcp`
7. Click Connect
8. Go to the "Toos" tab, then click "List Tools"
9. Select the `get_application` tool.
10. In the application_id field, enter `spark-bcec39f6201b42b9925124595baad260`, then click "Run Tool"
11. You should see a json payload in the output field.


