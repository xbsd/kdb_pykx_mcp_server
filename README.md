# KDB+ PyKX MCP Server

A Model Context Protocol (MCP) server for interacting with KDB+ databases using PyKX. This server enables AI assistants like Claude to query and analyze data stored in KDB+ databases through a safe, controlled interface.

## Features

- **List Tables**: View all available tables in the KDB+ session
- **Table Schema**: Get column names and types for any table
- **Row Count**: Get the number of rows in a table
- **Sample Data**: Retrieve sample rows from tables
- **Execute Queries**: Run q queries with safety restrictions
- **Column Statistics**: Get min, max, avg, count for columns
- **Distinct Values**: View unique values in categorical columns
- **Connection Info**: Check current connection parameters

## Safety Features

The server blocks dangerous operations to protect your data:
- DROP table operations
- DELETE without proper safeguards
- System commands
- File operations to system paths
- Exit/close commands

## Prerequisites

1. **KDB+ Server**: A running KDB+ (q) instance with your data loaded
2. **Python 3.9+**: Required for PyKX and MCP
3. **PyKX**: KX's Python interface for KDB+

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd kdb_pykx_mcp_server

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Start your KDB+ server

First, start KDB+ with your data loaded on a specific port:

```q
$ q -p 5001
q)\l stocks
`stocks
q)
```

### 2. Configure your MCP client

Add the server to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "kdb": {
      "command": "python",
      "args": [
        "/path/to/kdb_mcp_server.py",
        "--host", "localhost",
        "--port", "5001"
      ]
    }
  }
}
```

### 3. Query your data

Once connected, you can ask questions like:
- "What tables are available?"
- "Show me the schema of the stocks table"
- "How many rows are in the stocks table?"
- "Get the top 10 rows from stocks"
- "What are the distinct symbols in the stocks table?"
- "Run: select avg close by symbol from stocks"

## Command Line Options

```
--host HOST       KDB+ server host (default: localhost)
--port PORT       KDB+ server port (default: 5001)
--username USER   Username for authentication (optional)
--password PASS   Password for authentication (optional)
--timeout SECS    Connection timeout in seconds (default: 10)
```

## Available Tools

| Tool | Description |
|------|-------------|
| `kdb_list_tables` | List all tables in the session |
| `kdb_table_schema` | Get column metadata for a table |
| `kdb_table_count` | Get row count for a table |
| `kdb_table_sample` | Get sample rows from a table |
| `kdb_query` | Execute a q query (with safety checks) |
| `kdb_column_stats` | Get statistics for a column |
| `kdb_distinct_values` | Get distinct values in a column |
| `kdb_connection_info` | Show connection parameters |

## Example Queries

```q
# Basic select
select from stocks where symbol="AAPL"

# Aggregation
select avg close, max high, min low by symbol from stocks

# Time-based filtering
select from stocks where timestamp > 2026.01.10

# Top N by group
select 5 sublist desc close by symbol from stocks
```

## Development

### Running Tests

```bash
# Verify syntax
python -m py_compile kdb_mcp_server.py

# Test help
python kdb_mcp_server.py --help
```

### Project Structure

```
kdb_pykx_mcp_server/
├── kdb_mcp_server.py    # Main MCP server
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── stocks/             # Sample KDB+ data (splayed table)
├── dotkx/              # KDB+ installation
│   ├── bin/q           # Q binary
│   └── kc.lic          # License file
└── pykx_docs/          # PyKX documentation
```

## Troubleshooting

### Connection refused
Make sure your KDB+ server is running on the specified port:
```bash
# Check if port is in use
netstat -an | grep 5001
```

### License errors
PyKX runs in unlicensed mode (IPC only). This is sufficient for connecting to remote KDB+ servers.

### Query blocked
Certain operations are blocked for safety. Use SELECT queries for data retrieval.

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
