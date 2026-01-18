# KDB+ PyKX MCP Server

A Model Context Protocol (MCP) server for interacting with KDB+ databases using PyKX in native/embedded mode. This server enables AI assistants like Claude to query and analyze data stored in KDB+ databases through a safe, controlled interface.

## Features

- **Native PyKX Mode**: Runs KDB+ embedded within Python - no separate Q server needed
- **27 Specialized Tools**: Organized into 7 categories for comprehensive data analysis
- **Natural Language Ready**: Tools designed to answer common business questions
- **Safety Features**: Blocks dangerous operations to protect your data

## Tool Categories

### Category A: Basic Table Information (5 tools)
| Tool | Description |
|------|-------------|
| `list_tables` | List all tables in the KDB+ session |
| `table_schema` | Get column metadata (types, attributes) for a table |
| `table_count` | Get the number of rows in a table |
| `table_sample` | Retrieve sample rows from a table |
| `column_names` | Get list of column names for a table |

### Category B: Data Discovery (5 tools)
| Tool | Description |
|------|-------------|
| `distinct_values` | Get unique values in a column |
| `count_by_group` | Count records grouped by a column |
| `date_range` | Get min/max dates for a timestamp column |
| `data_points_per_day` | Count records by date |
| `column_stats` | Get min, max, avg, count for numeric columns |

### Category C: Price Analysis (5 tools)
| Tool | Description |
|------|-------------|
| `average_price_by_symbol` | Calculate average close price per symbol |
| `price_range_by_symbol` | Get min, max, and range of prices per symbol |
| `highest_prices` | Find maximum close price per symbol |
| `price_volatility` | Calculate standard deviation of prices per symbol |
| `price_statistics` | Get avg, median, and std dev of prices per symbol |

### Category D: Volume Analysis (3 tools)
| Tool | Description |
|------|-------------|
| `average_volume_by_symbol` | Calculate average trading volume per symbol |
| `total_volume_by_symbol` | Calculate total trading volume per symbol |
| `top_volume_records` | Find records with highest trading volume |

### Category E: Filtering & Selection (4 tools)
| Tool | Description |
|------|-------------|
| `filter_by_symbol` | Filter data for a specific stock symbol |
| `filter_by_price_threshold` | Find records where price exceeds threshold |
| `filter_by_date` | Filter data by date range |
| `symbol_summary` | Get comprehensive summary for a specific symbol |

### Category F: Advanced Analytics (3 tools)
| Tool | Description |
|------|-------------|
| `daily_ohlc` | Calculate daily Open/High/Low/Close summary |
| `price_change_analysis` | Analyze daily price movements and spreads |
| `execute_query` | Execute custom q queries (with safety checks) |

### Category G: Server Management (2 tools)
| Tool | Description |
|------|-------------|
| `server_info` | Show session info and loaded tables |
| `load_table` | Load a splayed table from disk |

## Safety Features

The server blocks dangerous operations to protect your data:
- DROP table operations
- DELETE without proper safeguards
- System commands (`\` commands)
- File operations to system paths
- Exit/close commands

## Prerequisites

1. **Python 3.9+**: Required for PyKX and MCP
2. **PyKX 4.0+**: KX's Python interface for KDB+ (with valid license)
3. **KDB+ License**: Usually located in `~/.kx/kc.lic`

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd kdb_pykx_mcp_server

# Install dependencies
pip install -r requirements.txt

# Verify PyKX license is working
python -c "import pykx as kx; print(f'Licensed: {kx.licensed}')"
```

### License Installation (if needed)

```python
import pykx as kx
# From base64 string
kx.license.install("YOUR_BASE64_LICENSE_STRING")
# Or from file
kx.license.install("/path/to/kc.lic")
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

### Example Questions

Once connected, you can ask natural language questions like:

**Basic Information:**
- "What tables do we have?"
- "What does the stocks table look like?"
- "How many records are in the stocks table?"

**Data Discovery:**
- "What stock symbols do we track?"
- "What's the date range of our data?"
- "How many records per symbol?"

**Price Analysis:**
- "What's the average stock price for each company?"
- "Which stocks have the highest prices?"
- "Which stocks are most volatile?"

**Volume Analysis:**
- "What's the average trading volume?"
- "Which trades had the highest volume?"

**Filtering:**
- "Show me Apple stock data"
- "Which stocks trade above $500?"
- "Show me 2025 data"

**Advanced:**
- "Show me daily OHLC for Apple"
- "Analyze daily price movements"

## Command Line Options

```
--data-dir PATH   Directory containing splayed tables to load at startup
```

## Example Q Queries

For advanced users, the `execute_query` tool accepts q queries:

```q
# Basic select
select from stocks where symbol like "AAPL"

# Aggregation
select avg close, max high, min low by symbol from stocks

# Time-based filtering
select from stocks where timestamp >= 2025.01.01

# Top N by volume
5 sublist `volume xdesc select symbol, timestamp, volume from stocks
```

## Testing

### Gold Standard Queries
Run the gold standard test suite to verify query results:
```bash
python gold_standard_queries.py
```

### Natural Language Tests
Run the natural language test suite:
```bash
python test_natural_language.py
```

## Project Structure

```
kdb_pykx_mcp_server/
├── kdb_mcp_server.py          # Main MCP server (27 tools)
├── requirements.txt           # Python dependencies
├── README.md                  # This file
├── stocks/                    # Sample KDB+ data (splayed table)
├── gold_standard_queries.py   # 25 reference queries
├── gold_standard_results.txt  # Expected query results
├── test_natural_language.py   # Natural language test suite
├── test_results.txt           # Test results and assessment
└── pykx_docs/                 # PyKX documentation reference
```

## Sample Data

The included `stocks/` directory contains a splayed KDB+ table with:
- **369,383 rows** of hourly stock data
- **20 stock symbols**: AAPL, ADBE, AMD, AMZN, AVGO, BKNG, CMCSA, COST, CSCO, GOOGL, INTC, INTU, META, MSFT, NFLX, NVDA, PEP, QCOM, TMUS, TSLA
- **Date range**: 2021-02-01 to 2026-01-13
- **Columns**: timestamp, open, high, low, close, volume, symbol, company

## Troubleshooting

### License errors
Ensure your PyKX license is valid and accessible:
```python
import pykx as kx
print(f"Licensed: {kx.licensed}")
```

### Query blocked
Certain operations are blocked for safety. Use SELECT queries for data retrieval.

### Table not found
Use `load_table` to load tables from disk, or specify `--data-dir` at startup.

## Assessment

The MCP server provides **27 tools** covering **90%+ of typical business user questions** about stock data:

**Strengths:**
1. Comprehensive coverage of common stock analysis queries
2. Safety features block dangerous operations
3. Native PyKX mode - no separate Q server needed
4. Well-organized tool categories
5. Consistent error handling and validation

**Potential Enhancements:**
1. Moving average calculations
2. Correlation analysis between symbols
3. Year-over-year comparisons
4. Percentile/ranking calculations
5. Time-windowed aggregations

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
