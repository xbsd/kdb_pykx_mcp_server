# KDB+ PyKX MCP Server

A Model Context Protocol (MCP) server for interacting with KDB+ databases using PyKX in native/embedded mode. This server enables AI assistants like Claude to query and analyze data stored in KDB+ databases through a safe, controlled interface.

## Features

- **Native PyKX Mode**: Runs KDB+ embedded within Python - no separate Q server needed
- **List Tables**: View all available tables in the KDB+ session
- **Table Schema**: Get column names and types for any table
- **Row Count**: Get the number of rows in a table
- **Sample Data**: Retrieve sample rows from tables
- **Execute Queries**: Run q queries with safety restrictions
- **Column Statistics**: Get min, max, avg, count for columns
- **Distinct Values**: View unique values in categorical columns
- **Load Tables**: Dynamically load splayed tables from disk
- **Server Info**: Check session status and loaded tables

## Safety Features

The server blocks dangerous operations to protect your data:
- DROP table operations
- DELETE without proper safeguards
- System commands
- File operations to system paths
- Exit/close commands

## Prerequisites

1. **Python 3.9+**: Required for PyKX and MCP
2. **PyKX 4.0+**: KX's Python interface for KDB+ (with license)

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd kdb_pykx_mcp_server

# Install dependencies
pip install -r requirements.txt

# Install PyKX license (base64 encoded)
python -c "
import pykx as kx
kx.license.install('YOUR_LICENSE_BASE64_STRING')
"
```

## Usage

### Configure your MCP client

Add the server to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "kdb": {
      "command": "python",
      "args": [
        "/path/to/kdb_mcp_server.py",
        "--data-dir", "/path/to/your/kdb/tables"
      ]
    }
  }
}
```

The `--data-dir` option automatically loads all splayed tables from that directory at startup.

### Query your data

Once connected, you can ask questions like:
- "What tables are available?"
- "Show me the schema of the stocks table"
- "How many rows are in the stocks table?"
- "Get the top 10 rows from stocks"
- "What are the distinct symbols in the stocks table?"
- "Run: select avg close by symbol from stocks"

## Command Line Options

```
--data-dir PATH   Directory containing splayed tables to load at startup
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
| `kdb_load_table` | Load a splayed table from disk |
| `kdb_server_info` | Show session info and loaded tables |

## Example Queries

```q
# Basic select
select from stocks where symbol=`AAPL

# Aggregation
select avg close, max high, min low by symbol from stocks

# Time-based filtering
select from stocks where timestamp > 2026.01.10

# Top N by group
select 5 sublist desc close by symbol from stocks
```

## License Configuration

PyKX requires a valid KDB+ license. To install:

1. **From base64 string** (recommended for automation):
   ```python
   import pykx as kx
   kx.license.install("YOUR_BASE64_LICENSE_STRING")
   ```

2. **From file**:
   ```python
   import pykx as kx
   kx.license.install("/path/to/kc.lic")
   ```

3. **Environment variable**:
   ```bash
   export KDB_LICENSE_B64="YOUR_BASE64_LICENSE_STRING"
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
├── kdb_mcp_server.py    # Main MCP server (native mode)
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── stocks/             # Sample KDB+ data (splayed table)
└── pykx_docs/          # PyKX documentation
```

## Troubleshooting

### License errors
Make sure your PyKX license is installed:
```python
import pykx as kx
print(f"Licensed: {kx.licensed}")
```

### Query blocked
Certain operations are blocked for safety. Use SELECT queries for data retrieval.

### Table not found
Use `kdb_load_table` to load tables from disk, or specify `--data-dir` at startup.

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
