#!/usr/bin/env python3
"""
KDB+ PyKX MCP Server

An MCP (Model Context Protocol) server for interacting with KDB+ databases
using PyKX in native/embedded mode. This server provides comprehensive tools
for querying tables, analyzing stock data, and executing safe q queries.

Usage:
    python kdb_mcp_server.py --data-dir /path/to/data

Tools are organized into categories:
- Basic: Table info, schema, sample data
- Discovery: Symbols, distributions, date ranges
- Price Analysis: Averages, ranges, volatility, highs/lows
- Volume Analysis: Volume stats, top volume records
- Filtering: By symbol, date, price thresholds
- Advanced: OHLC aggregation, custom queries
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
    r'\bdelete\s+from\b',  # DELETE FROM
    r'\\\\',               # Exit q session
    r'\\l\s+/',            # Load from root path
    r'\bexit\b',           # Exit command
    r'\bvalue\s*"',        # Dynamic code execution
    r'\bsystem\b',         # System commands
    r'\bhclose\b',         # Close handles
    r'\bhdel\b',           # Delete files
    r'`:/',                # File path operations to root
]

DANGEROUS_REGEX = [re.compile(p, re.IGNORECASE) for p in DANGEROUS_PATTERNS]


def is_dangerous_query(query: str) -> tuple[bool, str]:
    """Check if a query contains potentially dangerous operations."""
    for i, pattern in enumerate(DANGEROUS_REGEX):
        if pattern.search(query):
            return True, f"Query contains dangerous pattern: {DANGEROUS_PATTERNS[i]}"
    return False, ""


def load_tables_from_directory(data_dir: str) -> list[str]:
    """Load splayed tables from a directory into the q session."""
    loaded = []
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return loaded

    for item in data_path.iterdir():
        if item.is_dir() and (item / '.d').exists():
            table_name = item.name
            try:
                kx.q(f'{table_name}: get`:{item}')
                loaded.append(table_name)
                count = kx.q(f'count {table_name}').py()
                logger.info(f"Loaded table '{table_name}' with {count:,} rows")
            except Exception as e:
                logger.error(f"Failed to load table '{table_name}': {e}")

    return loaded


def format_result(result: Any, max_width: int = 120) -> str:
    """Format a PyKX result for display."""
    try:
        result_str = str(result)
        # Truncate very long lines
        lines = result_str.split('\n')
        formatted_lines = []
        for line in lines[:100]:  # Limit to 100 lines
            if len(line) > max_width:
                formatted_lines.append(line[:max_width] + '...')
            else:
                formatted_lines.append(line)
        if len(lines) > 100:
            formatted_lines.append(f'... ({len(lines) - 100} more rows)')
        return '\n'.join(formatted_lines)
    except Exception:
        return str(result)


def validate_table_name(name: str) -> bool:
    """Validate table name format."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))


def validate_column_name(name: str) -> bool:
    """Validate column name format."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the session."""
    tables = kx.q('tables[]').py()
    return table_name in tables


# Create the MCP server
app = Server("kdb-pykx-mcp-server")


@app.list_tools()
async def list_tools():
    """List available tools for the MCP client."""
    return [
        # =====================================================================
        # CATEGORY A: BASIC TABLE INFORMATION (Tools 1-5)
        # =====================================================================
        Tool(
            name="list_tables",
            description="List all tables available in the KDB+ session",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),
        Tool(
            name="table_schema",
            description="Get the schema (column names, types, attributes) of a table using meta",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="table_count",
            description="Get the number of rows in a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="table_sample",
            description="Get sample rows from a table (first N rows)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "num_rows": {"type": "integer", "description": "Number of rows (default: 10, max: 100)", "default": 10}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="column_names",
            description="Get the list of column names in a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"}
                },
                "required": ["table_name"]
            }
        ),

        # =====================================================================
        # CATEGORY B: DATA DISCOVERY (Tools 6-10)
        # =====================================================================
        Tool(
            name="distinct_values",
            description="Get distinct values in a column (useful for symbols, categories)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "column_name": {"type": "string", "description": "Name of the column"},
                    "limit": {"type": "integer", "description": "Max values to return (default: 50)", "default": 50}
                },
                "required": ["table_name", "column_name"]
            }
        ),
        Tool(
            name="count_by_group",
            description="Get row counts grouped by a column (distribution)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "group_column": {"type": "string", "description": "Column to group by"}
                },
                "required": ["table_name", "group_column"]
            }
        ),
        Tool(
            name="date_range",
            description="Get the min and max dates/timestamps in a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "date_column": {"type": "string", "description": "Date/timestamp column name", "default": "timestamp"}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="data_points_per_day",
            description="Count data points per day for time series analysis",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "date_column": {"type": "string", "description": "Date/timestamp column", "default": "timestamp"},
                    "limit": {"type": "integer", "description": "Number of days to show (default: 10)", "default": 10}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="column_stats",
            description="Get basic statistics for a numeric column (count, nulls, distinct count)",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table"},
                    "column_name": {"type": "string", "description": "Name of the column"}
                },
                "required": ["table_name", "column_name"]
            }
        ),

        # =====================================================================
        # CATEGORY C: PRICE ANALYSIS (Tools 11-15)
        # =====================================================================
        Tool(
            name="average_price_by_symbol",
            description="Calculate average price (close) for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column name", "default": "close"}
                },
                "required": []
            }
        ),
        Tool(
            name="price_range_by_symbol",
            description="Get min, max, and price range for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column name", "default": "close"}
                },
                "required": []
            }
        ),
        Tool(
            name="highest_prices",
            description="Get the highest (max) price for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column name", "default": "close"}
                },
                "required": []
            }
        ),
        Tool(
            name="price_volatility",
            description="Calculate price volatility (standard deviation) for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column name", "default": "close"}
                },
                "required": []
            }
        ),
        Tool(
            name="price_statistics",
            description="Get comprehensive price stats: avg, median, std dev for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column name", "default": "close"}
                },
                "required": []
            }
        ),

        # =====================================================================
        # CATEGORY D: VOLUME ANALYSIS (Tools 16-18)
        # =====================================================================
        Tool(
            name="average_volume_by_symbol",
            description="Calculate average trading volume for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"}
                },
                "required": []
            }
        ),
        Tool(
            name="total_volume_by_symbol",
            description="Calculate total trading volume for each symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"}
                },
                "required": []
            }
        ),
        Tool(
            name="top_volume_records",
            description="Get records with the highest trading volume",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "limit": {"type": "integer", "description": "Number of records (default: 10)", "default": 10}
                },
                "required": []
            }
        ),

        # =====================================================================
        # CATEGORY E: FILTERING & SELECTION (Tools 19-22)
        # =====================================================================
        Tool(
            name="filter_by_symbol",
            description="Get data for a specific stock symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., AAPL, NVDA)"},
                    "limit": {"type": "integer", "description": "Max rows to return (default: 100)", "default": 100}
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="filter_by_price_threshold",
            description="Get records where price exceeds a threshold",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "price_column": {"type": "string", "description": "Price column", "default": "close"},
                    "threshold": {"type": "number", "description": "Price threshold"},
                    "operator": {"type": "string", "description": "Comparison: gt, lt, gte, lte", "default": "gt"}
                },
                "required": ["threshold"]
            }
        ),
        Tool(
            name="filter_by_date",
            description="Get data from a specific year or date range",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "year": {"type": "integer", "description": "Year to filter (e.g., 2025)"},
                    "start_date": {"type": "string", "description": "Start date (YYYY.MM.DD format)"},
                    "end_date": {"type": "string", "description": "End date (YYYY.MM.DD format)"}
                },
                "required": []
            }
        ),
        Tool(
            name="symbol_summary",
            description="Get a summary for a specific symbol: count, avg price, avg volume",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "symbol": {"type": "string", "description": "Stock symbol (e.g., NVDA)"}
                },
                "required": ["symbol"]
            }
        ),

        # =====================================================================
        # CATEGORY F: ADVANCED ANALYTICS (Tools 23-25)
        # =====================================================================
        Tool(
            name="daily_ohlc",
            description="Get daily OHLC (Open, High, Low, Close) aggregation for a symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"},
                    "symbol": {"type": "string", "description": "Stock symbol"},
                    "limit": {"type": "integer", "description": "Number of days (default: 10)", "default": 10}
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="price_change_analysis",
            description="Analyze daily price ranges and spread percentages by symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Name of the table", "default": "stocks"}
                },
                "required": []
            }
        ),
        Tool(
            name="execute_query",
            description="Execute a custom q query. Use for complex queries not covered by other tools. Dangerous operations are blocked.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The q query to execute"},
                    "max_rows": {"type": "integer", "description": "Max rows to return (default: 100)", "default": 100}
                },
                "required": ["query"]
            }
        ),

        # =====================================================================
        # CATEGORY G: SERVER & TABLE MANAGEMENT (Tools 26-27)
        # =====================================================================
        Tool(
            name="server_info",
            description="Get information about the KDB+/PyKX session and loaded tables",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),
        Tool(
            name="load_table",
            description="Load a splayed table from disk into the session",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_path": {"type": "string", "description": "Path to the splayed table directory"},
                    "table_name": {"type": "string", "description": "Name for the table (optional)"}
                },
                "required": ["table_path"]
            }
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls from the MCP client."""

    try:
        # =====================================================================
        # CATEGORY A: BASIC TABLE INFORMATION
        # =====================================================================
        if name == "list_tables":
            result = kx.q('tables[]')
            tables = result.py()
            if not tables:
                return [TextContent(type="text", text="No tables found in the current session.")]
            table_list = "\n".join([f"  - {t}" for t in tables])
            return [TextContent(type="text", text=f"Available tables ({len(tables)}):\n{table_list}")]

        elif name == "table_schema":
            table_name = arguments.get("table_name")
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'meta {table_name}')
            return [TextContent(type="text", text=f"Schema for '{table_name}':\n{format_result(result)}")]

        elif name == "table_count":
            table_name = arguments.get("table_name")
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            count = kx.q(f'count {table_name}').py()
            return [TextContent(type="text", text=f"Table '{table_name}' has {count:,} rows")]

        elif name == "table_sample":
            table_name = arguments.get("table_name")
            num_rows = min(arguments.get("num_rows", 10), 100)
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{num_rows}#{table_name}')
            return [TextContent(type="text", text=f"Sample ({num_rows} rows) from '{table_name}':\n{format_result(result)}")]

        elif name == "column_names":
            table_name = arguments.get("table_name")
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'cols {table_name}')
            return [TextContent(type="text", text=f"Columns in '{table_name}':\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY B: DATA DISCOVERY
        # =====================================================================
        elif name == "distinct_values":
            table_name = arguments.get("table_name")
            column_name = arguments.get("column_name")
            limit = min(arguments.get("limit", 50), 500)
            if not validate_table_name(table_name) or not validate_column_name(column_name):
                return [TextContent(type="text", text="Error: Invalid table or column name")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{limit}#distinct {table_name}`{column_name}')
            return [TextContent(type="text", text=f"Distinct values in '{table_name}.{column_name}':\n{format_result(result)}")]

        elif name == "count_by_group":
            table_name = arguments.get("table_name")
            group_column = arguments.get("group_column")
            if not validate_table_name(table_name) or not validate_column_name(group_column):
                return [TextContent(type="text", text="Error: Invalid table or column name")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select cnt: count i by {group_column} from {table_name}')
            return [TextContent(type="text", text=f"Count by '{group_column}':\n{format_result(result)}")]

        elif name == "date_range":
            table_name = arguments.get("table_name")
            date_column = arguments.get("date_column", "timestamp")
            if not validate_table_name(table_name) or not validate_column_name(date_column):
                return [TextContent(type="text", text="Error: Invalid table or column name")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select min_date: min {date_column}, max_date: max {date_column} from {table_name}')
            return [TextContent(type="text", text=f"Date range in '{table_name}':\n{format_result(result)}")]

        elif name == "data_points_per_day":
            table_name = arguments.get("table_name")
            date_column = arguments.get("date_column", "timestamp")
            limit = arguments.get("limit", 10)
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{limit}#select cnt: count i by dt: `date${date_column} from {table_name}')
            return [TextContent(type="text", text=f"Data points per day:\n{format_result(result)}")]

        elif name == "column_stats":
            table_name = arguments.get("table_name")
            column_name = arguments.get("column_name")
            if not validate_table_name(table_name) or not validate_column_name(column_name):
                return [TextContent(type="text", text="Error: Invalid table or column name")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select cnt: count {column_name}, nulls: sum null {column_name}, distinct_cnt: count distinct {column_name} from {table_name}')
            return [TextContent(type="text", text=f"Stats for '{table_name}.{column_name}':\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY C: PRICE ANALYSIS
        # =====================================================================
        elif name == "average_price_by_symbol":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select avg_{price_column}: avg {price_column} by symbol from {table_name}')
            return [TextContent(type="text", text=f"Average {price_column} by symbol:\n{format_result(result)}")]

        elif name == "price_range_by_symbol":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select min_{price_column}: min {price_column}, max_{price_column}: max {price_column}, price_range: (max {price_column}) - min {price_column} by symbol from {table_name}')
            return [TextContent(type="text", text=f"Price range by symbol:\n{format_result(result)}")]

        elif name == "highest_prices":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select max_{price_column}: max {price_column} by symbol from {table_name}')
            return [TextContent(type="text", text=f"Highest {price_column} by symbol:\n{format_result(result)}")]

        elif name == "price_volatility":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select volatility: dev {price_column} by symbol from {table_name}')
            return [TextContent(type="text", text=f"Price volatility (std dev) by symbol:\n{format_result(result)}")]

        elif name == "price_statistics":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select avg_{price_column}: avg {price_column}, median_{price_column}: med {price_column}, std_{price_column}: dev {price_column} by symbol from {table_name}')
            return [TextContent(type="text", text=f"Price statistics by symbol:\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY D: VOLUME ANALYSIS
        # =====================================================================
        elif name == "average_volume_by_symbol":
            table_name = arguments.get("table_name", "stocks")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select avg_volume: avg volume by symbol from {table_name}')
            return [TextContent(type="text", text=f"Average volume by symbol:\n{format_result(result)}")]

        elif name == "total_volume_by_symbol":
            table_name = arguments.get("table_name", "stocks")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select total_volume: sum volume by symbol from {table_name}')
            return [TextContent(type="text", text=f"Total volume by symbol:\n{format_result(result)}")]

        elif name == "top_volume_records":
            table_name = arguments.get("table_name", "stocks")
            limit = arguments.get("limit", 10)
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{limit} sublist `volume xdesc select symbol, timestamp, volume from {table_name}')
            return [TextContent(type="text", text=f"Top {limit} highest volume records:\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY E: FILTERING & SELECTION
        # =====================================================================
        elif name == "filter_by_symbol":
            table_name = arguments.get("table_name", "stocks")
            symbol = arguments.get("symbol", "").upper()
            limit = min(arguments.get("limit", 100), 1000)
            if not symbol:
                return [TextContent(type="text", text="Error: symbol is required")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{limit}#select from {table_name} where symbol like "{symbol}"')
            count = kx.q(f'count select from {table_name} where symbol like "{symbol}"').py()
            return [TextContent(type="text", text=f"Data for {symbol} ({count:,} total rows, showing {limit}):\n{format_result(result)}")]

        elif name == "filter_by_price_threshold":
            table_name = arguments.get("table_name", "stocks")
            price_column = arguments.get("price_column", "close")
            threshold = arguments.get("threshold")
            operator = arguments.get("operator", "gt")
            if threshold is None:
                return [TextContent(type="text", text="Error: threshold is required")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
            op = op_map.get(operator, ">")
            result = kx.q(f'select cnt: count i, symbols: distinct symbol from {table_name} where {price_column} {op} {threshold}')
            return [TextContent(type="text", text=f"Records where {price_column} {op} {threshold}:\n{format_result(result)}")]

        elif name == "filter_by_date":
            table_name = arguments.get("table_name", "stocks")
            year = arguments.get("year")
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]

            if year:
                result = kx.q(f'select cnt: count i by symbol from {table_name} where timestamp >= {year}.01.01')
                return [TextContent(type="text", text=f"Data from {year} onward:\n{format_result(result)}")]
            elif start_date and end_date:
                result = kx.q(f'select cnt: count i by symbol from {table_name} where timestamp >= {start_date}, timestamp <= {end_date}')
                return [TextContent(type="text", text=f"Data from {start_date} to {end_date}:\n{format_result(result)}")]
            else:
                return [TextContent(type="text", text="Error: Provide either 'year' or both 'start_date' and 'end_date'")]

        elif name == "symbol_summary":
            table_name = arguments.get("table_name", "stocks")
            symbol = arguments.get("symbol", "").upper()
            if not symbol:
                return [TextContent(type="text", text="Error: symbol is required")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select cnt: count i, avg_close: avg close, avg_volume: avg volume from {table_name} where symbol like "{symbol}"')
            return [TextContent(type="text", text=f"Summary for {symbol}:\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY F: ADVANCED ANALYTICS
        # =====================================================================
        elif name == "daily_ohlc":
            table_name = arguments.get("table_name", "stocks")
            symbol = arguments.get("symbol", "").upper()
            limit = arguments.get("limit", 10)
            if not symbol:
                return [TextContent(type="text", text="Error: symbol is required")]
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'{limit} sublist `dt xdesc select open: first open, high: max high, low: min low, close: last close, volume: sum volume by dt: `date$timestamp from {table_name} where symbol like "{symbol}"')
            return [TextContent(type="text", text=f"Daily OHLC for {symbol} (last {limit} days):\n{format_result(result)}")]

        elif name == "price_change_analysis":
            table_name = arguments.get("table_name", "stocks")
            if not table_exists(table_name):
                return [TextContent(type="text", text=f"Error: Table '{table_name}' not found")]
            result = kx.q(f'select avg_daily_range: avg (high - low), avg_spread_pct: avg 100 * (high - low) % low by symbol from {table_name}')
            return [TextContent(type="text", text=f"Price change analysis by symbol:\n{format_result(result)}")]

        elif name == "execute_query":
            query = arguments.get("query", "").strip()
            max_rows = min(arguments.get("max_rows", 100), 10000)
            if not query:
                return [TextContent(type="text", text="Error: query is required")]
            is_dangerous, reason = is_dangerous_query(query)
            if is_dangerous:
                return [TextContent(type="text", text=f"Error: Query blocked for safety. {reason}")]
            result = kx.q(query)
            try:
                if hasattr(result, '__len__') and len(result) > max_rows:
                    result = kx.q(f'{max_rows}#', result)
                    return [TextContent(type="text", text=f"Query result (limited to {max_rows} rows):\n{format_result(result)}")]
            except:
                pass
            return [TextContent(type="text", text=f"Query result:\n{format_result(result)}")]

        # =====================================================================
        # CATEGORY G: SERVER & TABLE MANAGEMENT
        # =====================================================================
        elif name == "server_info":
            tables = kx.q('tables[]').py()
            table_info = []
            for t in tables:
                try:
                    count = kx.q(f'count {t}').py()
                    table_info.append(f"    {t}: {count:,} rows")
                except:
                    table_info.append(f"    {t}: N/A")
            return [TextContent(
                type="text",
                text=f"KDB+/PyKX Server Info:\n"
                     f"  PyKX Version: {kx.__version__}\n"
                     f"  PyKX Licensed: {kx.licensed}\n"
                     f"  Data Directory: {DATA_DIR or 'Not set'}\n"
                     f"  Loaded Tables ({len(tables)}):\n" + '\n'.join(table_info)
            )]

        elif name == "load_table":
            table_path = arguments.get("table_path")
            table_name = arguments.get("table_name")
            if not table_path:
                return [TextContent(type="text", text="Error: table_path is required")]
            path = Path(table_path)
            if not path.exists():
                return [TextContent(type="text", text=f"Error: Path '{table_path}' does not exist")]
            if not (path / '.d').exists():
                return [TextContent(type="text", text=f"Error: '{table_path}' is not a valid splayed table")]
            if not table_name:
                table_name = path.name
            if not validate_table_name(table_name):
                return [TextContent(type="text", text="Error: Invalid table name format")]
            kx.q(f'{table_name}: get`:{path}')
            count = kx.q(f'count {table_name}').py()
            return [TextContent(type="text", text=f"Loaded table '{table_name}' with {count:,} rows")]

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

    parser = argparse.ArgumentParser(description="KDB+ PyKX MCP Server")
    parser.add_argument("--data-dir", default=None,
                       help="Directory containing splayed tables to load at startup")

    args = parser.parse_args()

    logger.info(f"Starting KDB+ PyKX MCP Server")
    logger.info(f"PyKX Version: {kx.__version__}")
    logger.info(f"PyKX Licensed: {kx.licensed}")

    if not kx.licensed:
        logger.warning("PyKX is running in unlicensed mode. Some features may be limited.")

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
