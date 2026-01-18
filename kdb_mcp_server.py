#!/usr/bin/env python3
"""
KDB+ PyKX MCP Server (Native Mode)

An MCP (Model Context Protocol) server for interacting with KDB+ databases
using PyKX in native/embedded mode. This server provides tools for querying
tables, getting metadata, and executing safe q queries.

Usage:
    python kdb_mcp_server.py --data-dir /path/to/data

Or configure in your MCP client settings with args:
    ["--data-dir", "/path/to/kdb/tables"]
"""

import os
import re
import argparse
import logging
from typing import Any, Optional
from pathlib import Path

import pykx as kx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global configuration
DATA_DIR: Optional[str] = None
LOADED_TABLES: list[str] = []

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
    r'`:/',                # File path operations to root
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
    for i, pattern in enumerate(DANGEROUS_REGEX):
        if pattern.search(query):
            return True, f"Query contains dangerous pattern: {DANGEROUS_PATTERNS[i]}"
    return False, ""


def load_tables_from_directory(data_dir: str) -> list[str]:
    """
    Load splayed tables from a directory into the q session.

    Args:
        data_dir: Path to directory containing splayed tables

    Returns:
        List of loaded table names
    """
    loaded = []
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return loaded

    # Look for splayed tables (directories with .d file)
    for item in data_path.iterdir():
        if item.is_dir() and (item / '.d').exists():
            table_name = item.name
            try:
                # Load splayed table
                kx.q(f'{table_name}: get`:{item}')
                loaded.append(table_name)
                count = kx.q(f'count {table_name}').py()
                logger.info(f"Loaded table '{table_name}' with {count:,} rows")
            except Exception as e:
                logger.error(f"Failed to load table '{table_name}': {e}")

    return loaded


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
    except Exception:
        return str(result)


# Create the MCP server
app = Server("kdb-pykx-mcp-server")


@app.list_tools()
async def list_tools():
    """List available tools for the MCP client."""
    return [
        Tool(
            name="kdb_list_tables",
            description="List all tables available in the KDB+ session",
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
                        "description": "The q query to execute (e.g., 'select from stocks where symbol=`AAPL')"
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
            name="kdb_load_table",
            description="Load a splayed table from disk into the KDB+ session",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_path": {
                        "type": "string",
                        "description": "Path to the splayed table directory"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name to assign to the table (optional, defaults to directory name)"
                    }
                },
                "required": ["table_path"]
            }
        ),
        Tool(
            name="kdb_server_info",
            description="Get information about the KDB+/PyKX session",
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
            result = kx.q('tables[]')
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

            # Check if table exists
            tables = kx.q('tables[]').py()
            if table_name not in tables:
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            result = kx.q(f'meta {table_name}')
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

            tables = kx.q('tables[]').py()
            if table_name not in tables:
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            result = kx.q(f'count {table_name}')
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

            tables = kx.q('tables[]').py()
            if table_name not in tables:
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            result = kx.q(f'{num_rows}#{table_name}')
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

            # Execute the query
            result = kx.q(query)

            # If result is a table, limit rows
            try:
                if hasattr(result, '__len__') and len(result) > max_rows:
                    result = kx.q(f'{max_rows}#', result)
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

            tables = kx.q('tables[]').py()
            if table_name not in tables:
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            # Get column metadata first
            meta = kx.q(f'meta {table_name}')
            meta_df = meta.pd()

            if column_name not in meta_df.index:
                return [TextContent(type="text", text=f"Error: Column '{column_name}' not found in table '{table_name}'")]

            col_type = meta_df.loc[column_name, 't']

            # Build stats query based on column type
            stats_parts = [f"cnt: count {column_name}"]
            stats_parts.append(f"nulls: sum null {column_name}")
            stats_parts.append(f"distinct_count: count distinct {column_name}")

            # Numeric types support min/max/avg
            if col_type in ['i', 'j', 'h', 'e', 'f', 'n', 'p', 'z', 'd', 't']:
                stats_parts.append(f"min_val: min {column_name}")
                stats_parts.append(f"max_val: max {column_name}")
                if col_type in ['i', 'j', 'h', 'e', 'f']:
                    stats_parts.append(f"avg_val: avg {column_name}")
                    stats_parts.append(f"sum_val: sum {column_name}")

            stats_query = f"select {', '.join(stats_parts)} from {table_name}"
            result = kx.q(stats_query)
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

            tables = kx.q('tables[]').py()
            if table_name not in tables:
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            # Get distinct values with count
            query = f"select count_rows: count i by {column_name} from {table_name}"
            result = kx.q(query)

            # Limit results
            truncated = False
            if hasattr(result, '__len__') and len(result) > limit:
                result = kx.q(f'{limit}#', result)
                truncated = True

            result_str = format_result(result)
            msg = f"Distinct values for '{table_name}.{column_name}':\n{result_str}"
            if truncated:
                msg += f"\n\n(Results truncated to {limit} values)"

            return [TextContent(type="text", text=msg)]

        elif name == "kdb_load_table":
            table_path = arguments.get("table_path")
            table_name = arguments.get("table_name")

            if not table_path:
                return [TextContent(type="text", text="Error: table_path is required")]

            path = Path(table_path)
            if not path.exists():
                return [TextContent(type="text", text=f"Error: Path '{table_path}' does not exist")]

            if not (path / '.d').exists():
                return [TextContent(type="text", text=f"Error: '{table_path}' is not a valid splayed table (missing .d file)")]

            # Use directory name if table_name not provided
            if not table_name:
                table_name = path.name

            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]

            try:
                kx.q(f'{table_name}: get`:{path}')
                count = kx.q(f'count {table_name}').py()
                return [TextContent(
                    type="text",
                    text=f"Successfully loaded table '{table_name}' with {count:,} rows"
                )]
            except Exception as e:
                return [TextContent(type="text", text=f"Error loading table: {str(e)}")]

        elif name == "kdb_server_info":
            tables = kx.q('tables[]').py()
            table_counts = {}
            for t in tables:
                try:
                    table_counts[t] = kx.q(f'count {t}').py()
                except:
                    table_counts[t] = "N/A"

            table_info = "\n".join([f"    {t}: {c:,} rows" if isinstance(c, int) else f"    {t}: {c}"
                                   for t, c in table_counts.items()])

            return [TextContent(
                type="text",
                text=f"KDB+/PyKX Server Info:\n"
                     f"  PyKX Version: {kx.__version__}\n"
                     f"  PyKX Licensed: {kx.licensed}\n"
                     f"  Data Directory: {DATA_DIR or 'Not set'}\n"
                     f"  Loaded Tables ({len(tables)}):\n{table_info if table_info else '    (none)'}"
            )]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except kx.exceptions.QError as e:
        return [TextContent(type="text", text=f"KDB+ Error: {str(e)}")]
    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def main():
    """Main entry point for the MCP server."""
    global DATA_DIR, LOADED_TABLES

    parser = argparse.ArgumentParser(description="KDB+ PyKX MCP Server (Native Mode)")
    parser.add_argument("--data-dir", default=None,
                       help="Directory containing splayed tables to load at startup")

    args = parser.parse_args()

    logger.info(f"Starting KDB+ PyKX MCP Server (Native Mode)")
    logger.info(f"PyKX Version: {kx.__version__}")
    logger.info(f"PyKX Licensed: {kx.licensed}")

    if not kx.licensed:
        logger.warning("PyKX is running in unlicensed mode. Some features may be limited.")

    # Load tables from data directory if specified
    if args.data_dir:
        DATA_DIR = args.data_dir
        logger.info(f"Loading tables from: {DATA_DIR}")
        LOADED_TABLES = load_tables_from_directory(DATA_DIR)
        logger.info(f"Loaded {len(LOADED_TABLES)} table(s): {LOADED_TABLES}")

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
