#!/usr/bin/env python3
"""
KDB+ PyKX MCP Server

An MCP (Model Context Protocol) server for interacting with KDB+ databases
using PyKX. This server provides tools for querying tables, getting metadata,
and executing safe q queries against a KDB+ server.

Usage:
    python kdb_mcp_server.py --host localhost --port 5001

Or configure in your MCP client settings with args:
    ["--host", "localhost", "--port", "5001"]
"""

import os
import re
import argparse
import logging
from typing import Any, Optional
from contextlib import asynccontextmanager

# Set PyKX to unlicensed mode for IPC-only usage
os.environ['PYKX_UNLICENSED'] = 'true'

import pykx as kx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global connection parameters
KDB_HOST = "localhost"
KDB_PORT = 5001
KDB_USERNAME: Optional[str] = None
KDB_PASSWORD: Optional[str] = None
KDB_TIMEOUT = 10.0

# Dangerous operations that should be blocked for safety
DANGEROUS_PATTERNS = [
    r'\bdrop\b',           # DROP table
    r'\bdelete\s+from\b',  # DELETE FROM (without where clause detection)
    r'\\\\',               # Exit q session
    r'\\l\s+/',            # Load from root path
    r'\bexit\b',           # Exit command
    r'\bvalue\s*"',        # Dynamic code execution
    r'\bsystem\b',         # System commands
    r'\bhclose\b',         # Close handles
    r'\bhdel\b',           # Delete files
    r'`:.*/',              # File path operations to root
]

# Compile patterns for efficiency
DANGEROUS_REGEX = [re.compile(p, re.IGNORECASE) for p in DANGEROUS_PATTERNS]


def is_dangerous_query(query: str) -> tuple[bool, str]:
    """
    Check if a query contains potentially dangerous operations.

    Args:
        query: The q query string to check

    Returns:
        Tuple of (is_dangerous, reason)
    """
    query_lower = query.lower().strip()

    for i, pattern in enumerate(DANGEROUS_REGEX):
        if pattern.search(query):
            return True, f"Query contains dangerous pattern: {DANGEROUS_PATTERNS[i]}"

    return False, ""


def get_connection() -> kx.SyncQConnection:
    """
    Create a connection to the KDB+ server.

    Returns:
        SyncQConnection instance
    """
    kwargs = {
        'host': KDB_HOST,
        'port': KDB_PORT,
        'timeout': KDB_TIMEOUT,
    }

    if KDB_USERNAME and KDB_PASSWORD:
        kwargs['username'] = KDB_USERNAME
        kwargs['password'] = KDB_PASSWORD

    return kx.SyncQConnection(**kwargs)


def format_result(result: Any) -> str:
    """
    Format a PyKX result for display.

    Args:
        result: PyKX result object

    Returns:
        Formatted string representation
    """
    try:
        # Try to convert to pandas for better formatting
        if hasattr(result, 'pd'):
            df = result.pd()
            return df.to_string()
        elif hasattr(result, 'py'):
            return str(result.py())
        else:
            return str(result)
    except Exception as e:
        return str(result)


# Create the MCP server
app = Server("kdb-pykx-mcp-server")


@app.list_tools()
async def list_tools():
    """List available tools for the MCP client."""
    return [
        Tool(
            name="kdb_list_tables",
            description="List all tables available in the connected KDB+ session",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="kdb_table_schema",
            description="Get the schema (column names and types) of a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to get schema for"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="kdb_table_count",
            description="Get the number of rows in a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to count rows for"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="kdb_table_sample",
            description="Get a sample of rows from a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to sample from"
                    },
                    "num_rows": {
                        "type": "integer",
                        "description": "Number of rows to retrieve (default: 10, max: 100)",
                        "default": 10
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="kdb_query",
            description="Execute a q query against the KDB+ database. Use for SELECT queries and data analysis. Dangerous operations (DROP, DELETE without WHERE, system commands) are blocked for safety.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The q query to execute (e.g., 'select from stocks where symbol=\"AAPL\"')"
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 100, max: 10000)",
                        "default": 100
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="kdb_column_stats",
            description="Get statistics for a specific column in a table (min, max, avg, count, distinct count)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Name of the column to analyze"
                    }
                },
                "required": ["table_name", "column_name"]
            }
        ),
        Tool(
            name="kdb_distinct_values",
            description="Get distinct values for a column (useful for categorical columns like symbols)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Name of the column to get distinct values for"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of distinct values to return (default: 50)",
                        "default": 50
                    }
                },
                "required": ["table_name", "column_name"]
            }
        ),
        Tool(
            name="kdb_connection_info",
            description="Get information about the current KDB+ connection",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls from the MCP client."""

    try:
        if name == "kdb_list_tables":
            with get_connection() as conn:
                result = conn('tables[]')
                tables = result.py()
                if not tables:
                    return [TextContent(type="text", text="No tables found in the current session.")]

                table_list = "\n".join([f"  - {t}" for t in tables])
                return [TextContent(
                    type="text",
                    text=f"Available tables ({len(tables)}):\n{table_list}"
                )]

        elif name == "kdb_table_schema":
            table_name = arguments.get("table_name")
            if not table_name:
                return [TextContent(type="text", text="Error: table_name is required")]

            # Validate table name (alphanumeric and underscore only)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]

            with get_connection() as conn:
                # Check if table exists
                tables = conn('tables[]').py()
                if table_name not in tables:
                    return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

                result = conn(f'meta {table_name}')
                schema_str = format_result(result)
                return [TextContent(
                    type="text",
                    text=f"Schema for table '{table_name}':\n{schema_str}"
                )]

        elif name == "kdb_table_count":
            table_name = arguments.get("table_name")
            if not table_name:
                return [TextContent(type="text", text="Error: table_name is required")]

            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]

            with get_connection() as conn:
                tables = conn('tables[]').py()
                if table_name not in tables:
                    return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

                result = conn(f'count {table_name}')
                count = result.py()
                return [TextContent(
                    type="text",
                    text=f"Table '{table_name}' has {count:,} rows"
                )]

        elif name == "kdb_table_sample":
            table_name = arguments.get("table_name")
            num_rows = min(arguments.get("num_rows", 10), 100)  # Cap at 100

            if not table_name:
                return [TextContent(type="text", text="Error: table_name is required")]

            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]

            with get_connection() as conn:
                tables = conn('tables[]').py()
                if table_name not in tables:
                    return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

                result = conn(f'{num_rows}#{table_name}')
                sample_str = format_result(result)
                return [TextContent(
                    type="text",
                    text=f"Sample of {num_rows} rows from '{table_name}':\n{sample_str}"
                )]

        elif name == "kdb_query":
            query = arguments.get("query", "").strip()
            max_rows = min(arguments.get("max_rows", 100), 10000)  # Cap at 10000

            if not query:
                return [TextContent(type="text", text="Error: query is required")]

            # Safety check
            is_dangerous, reason = is_dangerous_query(query)
            if is_dangerous:
                return [TextContent(
                    type="text",
                    text=f"Error: Query blocked for safety. {reason}\n\nPlease use SELECT queries only."
                )]

            with get_connection() as conn:
                # Execute the query
                result = conn(query)

                # If result is a table, limit rows
                try:
                    if hasattr(result, '__len__') and len(result) > max_rows:
                        result = conn(f'{max_rows}#{query}')
                        result_str = format_result(result)
                        return [TextContent(
                            type="text",
                            text=f"Query result (limited to {max_rows} rows):\n{result_str}"
                        )]
                except:
                    pass

                result_str = format_result(result)
                return [TextContent(
                    type="text",
                    text=f"Query result:\n{result_str}"
                )]

        elif name == "kdb_column_stats":
            table_name = arguments.get("table_name")
            column_name = arguments.get("column_name")

            if not table_name or not column_name:
                return [TextContent(type="text", text="Error: table_name and column_name are required")]

            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
                return [TextContent(type="text", text="Error: Invalid column name format")]

            with get_connection() as conn:
                tables = conn('tables[]').py()
                if table_name not in tables:
                    return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

                # Get column metadata first
                meta = conn(f'meta {table_name}')
                meta_df = meta.pd()

                if column_name not in meta_df.index:
                    return [TextContent(type="text", text=f"Error: Column '{column_name}' not found in table '{table_name}'")]

                col_type = meta_df.loc[column_name, 't']

                # Build stats query based on column type
                stats_parts = [f"count: count {column_name}"]
                stats_parts.append(f"nulls: sum null {column_name}")
                stats_parts.append(f"distinct_count: count distinct {column_name}")

                # Numeric types support min/max/avg
                if col_type in ['i', 'j', 'h', 'e', 'f', 'n', 'p', 'z', 'd', 't']:
                    stats_parts.append(f"min_val: min {column_name}")
                    stats_parts.append(f"max_val: max {column_name}")
                    if col_type in ['i', 'j', 'h', 'e', 'f']:
                        stats_parts.append(f"avg_val: avg {column_name}")
                        stats_parts.append(f"sum_val: sum {column_name}")

                stats_query = f"select {'; '.join(stats_parts)} from {table_name}"
                result = conn(stats_query)
                stats_str = format_result(result)

                return [TextContent(
                    type="text",
                    text=f"Statistics for '{table_name}.{column_name}' (type: {col_type}):\n{stats_str}"
                )]

        elif name == "kdb_distinct_values":
            table_name = arguments.get("table_name")
            column_name = arguments.get("column_name")
            limit = min(arguments.get("limit", 50), 500)  # Cap at 500

            if not table_name or not column_name:
                return [TextContent(type="text", text="Error: table_name and column_name are required")]

            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
                return [TextContent(type="text", text="Error: Invalid column name format")]

            with get_connection() as conn:
                tables = conn('tables[]').py()
                if table_name not in tables:
                    return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

                # Get distinct values with count
                query = f"select count_rows: count i by {column_name} from {table_name}"
                result = conn(query)

                # Limit results
                if hasattr(result, '__len__') and len(result) > limit:
                    result = conn(f'{limit}#{query}')
                    truncated = True
                else:
                    truncated = False

                result_str = format_result(result)
                msg = f"Distinct values for '{table_name}.{column_name}':\n{result_str}"
                if truncated:
                    msg += f"\n\n(Results truncated to {limit} values)"

                return [TextContent(type="text", text=msg)]

        elif name == "kdb_connection_info":
            return [TextContent(
                type="text",
                text=f"KDB+ Connection Info:\n"
                     f"  Host: {KDB_HOST}\n"
                     f"  Port: {KDB_PORT}\n"
                     f"  Timeout: {KDB_TIMEOUT}s\n"
                     f"  Auth: {'Yes' if KDB_USERNAME else 'No'}"
            )]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except kx.QError as e:
        return [TextContent(type="text", text=f"KDB+ Error: {str(e)}")]
    except ConnectionError as e:
        return [TextContent(
            type="text",
            text=f"Connection Error: Could not connect to KDB+ at {KDB_HOST}:{KDB_PORT}\n"
                 f"Make sure the KDB+ server is running with: q -p {KDB_PORT}"
        )]
    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def main():
    """Main entry point for the MCP server."""
    global KDB_HOST, KDB_PORT, KDB_USERNAME, KDB_PASSWORD, KDB_TIMEOUT

    parser = argparse.ArgumentParser(description="KDB+ PyKX MCP Server")
    parser.add_argument("--host", default="localhost", help="KDB+ server host (default: localhost)")
    parser.add_argument("--port", type=int, default=5001, help="KDB+ server port (default: 5001)")
    parser.add_argument("--username", default=None, help="Username for authentication")
    parser.add_argument("--password", default=None, help="Password for authentication")
    parser.add_argument("--timeout", type=float, default=10.0, help="Connection timeout in seconds")

    args = parser.parse_args()

    KDB_HOST = args.host
    KDB_PORT = args.port
    KDB_USERNAME = args.username
    KDB_PASSWORD = args.password
    KDB_TIMEOUT = args.timeout

    logger.info(f"Starting KDB+ PyKX MCP Server (connecting to {KDB_HOST}:{KDB_PORT})")

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
