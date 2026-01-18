#!/usr/bin/env python3
"""
Gold Standard Queries for KDB+ Stocks MCP Server

This file contains 25 reference queries and their expected results
for testing the MCP server tools.
"""

import os
os.chdir('/home/user/kdb_pykx_mcp_server')

import pykx as kx

# Load stocks table
kx.q('stocks: get`:stocks')

print("=" * 80)
print("GOLD STANDARD QUERIES - KDB+ STOCKS TABLE")
print("=" * 80)

queries = {}

# =============================================================================
# CATEGORY A: BASIC TABLE INFORMATION (Queries 1-5)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY A: BASIC TABLE INFORMATION")
print("=" * 80)

# Query 1: List all tables
print("\n--- Query 1: List all tables ---")
q1 = kx.q('tables[]')
print(q1)
queries['list_tables'] = str(q1)

# Query 2: Table schema/metadata
print("\n--- Query 2: Table schema (meta) ---")
q2 = kx.q('meta stocks')
print(q2)
queries['table_schema'] = str(q2)

# Query 3: Row count
print("\n--- Query 3: Row count ---")
q3 = kx.q('count stocks')
print(f"Count: {q3.py():,}")
queries['row_count'] = q3.py()

# Query 4: Sample rows (first N)
print("\n--- Query 4: Sample rows (first 5) ---")
q4 = kx.q('5#stocks')
print(q4)
queries['sample_rows'] = str(q4)

# Query 5: Column names
print("\n--- Query 5: Column names ---")
q5 = kx.q('cols stocks')
print(q5)
queries['column_names'] = str(q5)

# =============================================================================
# CATEGORY B: DISTINCT VALUES & DISTRIBUTIONS (Queries 6-10)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY B: DISTINCT VALUES & DISTRIBUTIONS")
print("=" * 80)

# Query 6: Distinct symbols
print("\n--- Query 6: Distinct symbols ---")
q6 = kx.q('distinct stocks`symbol')
print(q6)
queries['distinct_symbols'] = str(q6)

# Query 7: Count by symbol
print("\n--- Query 7: Row count by symbol ---")
q7 = kx.q('select cnt: count i by symbol from stocks')
print(q7)
queries['count_by_symbol'] = str(q7)

# Query 8: Date range
print("\n--- Query 8: Date range ---")
q8 = kx.q('select min_date: min timestamp, max_date: max timestamp from stocks')
print(q8)
queries['date_range'] = str(q8)

# Query 9: Number of distinct companies
print("\n--- Query 9: Distinct companies ---")
q9 = kx.q('count distinct stocks`company')
print(f"Distinct companies: {q9.py()}")
queries['distinct_companies'] = q9.py()

# Query 10: Data points per day (sample)
print("\n--- Query 10: Data points per day ---")
q10 = kx.q('select cnt: count i by dt: `date$timestamp from stocks')
print(kx.q('10#', q10))
queries['points_per_day'] = str(kx.q('10#', q10))

# =============================================================================
# CATEGORY C: PRICE ANALYSIS (Queries 11-15)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY C: PRICE ANALYSIS")
print("=" * 80)

# Query 11: Average prices by symbol
print("\n--- Query 11: Average close price by symbol ---")
q11 = kx.q('select avg_close: avg close by symbol from stocks')
print(q11)
queries['avg_price_by_symbol'] = str(q11)

# Query 12: Min/Max prices by symbol
print("\n--- Query 12: Price range by symbol ---")
q12 = kx.q('select min_close: min close, max_close: max close, price_range: (max close) - min close by symbol from stocks')
print(q12)
queries['price_range_by_symbol'] = str(q12)

# Query 13: Latest price per symbol
print("\n--- Query 13: Latest price per symbol ---")
q13 = kx.q('select last_timestamp: last timestamp, last_close: last close by symbol from stocks')
print(q13)
queries['latest_price_by_symbol'] = str(q13)

# Query 14: Highest closing price ever
print("\n--- Query 14: Highest closing prices ---")
q14 = kx.q('select max_close: max close by symbol from stocks')
print(q14)
queries['highest_prices'] = str(q14)

# Query 15: Price volatility (std dev) by symbol
print("\n--- Query 15: Price volatility by symbol ---")
q15 = kx.q('select volatility: dev close by symbol from stocks')
print(q15)
queries['price_volatility'] = str(q15)

# =============================================================================
# CATEGORY D: VOLUME ANALYSIS (Queries 16-18)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY D: VOLUME ANALYSIS")
print("=" * 80)

# Query 16: Average volume by symbol
print("\n--- Query 16: Average volume by symbol ---")
q16 = kx.q('select avg_volume: avg volume by symbol from stocks')
print(q16)
queries['avg_volume_by_symbol'] = str(q16)

# Query 17: Total volume by symbol
print("\n--- Query 17: Total volume by symbol ---")
q17 = kx.q('select total_volume: sum volume by symbol from stocks')
print(q17)
queries['total_volume_by_symbol'] = str(q17)

# Query 18: Top 5 highest volume days
print("\n--- Query 18: Top 5 highest volume records ---")
q18 = kx.q('5 sublist `volume xdesc select symbol, timestamp, volume from stocks')
print(q18)
queries['top_volume_records'] = str(q18)

# =============================================================================
# CATEGORY E: FILTERING & CONDITIONS (Queries 19-22)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY E: FILTERING & CONDITIONS")
print("=" * 80)

# Query 19: Filter by symbol
print("\n--- Query 19: Data for AAPL (last 5) ---")
q19 = kx.q('5#select from stocks where symbol like "AAPL"')
print(q19)
queries['filter_by_symbol'] = str(q19)

# Query 20: Filter by price threshold
print("\n--- Query 20: Records where close > 500 ---")
q20 = kx.q('select cnt: count i, symbols: distinct symbol from stocks where close > 500')
print(q20)
queries['filter_by_price'] = str(q20)

# Query 21: Filter by date range
print("\n--- Query 21: Data from 2025 ---")
q21 = kx.q('select cnt: count i by symbol from stocks where timestamp >= 2025.01.01')
print(q21)
queries['filter_by_date'] = str(q21)

# Query 22: Multi-condition filter
print("\n--- Query 22: NVDA with close > 100 ---")
q22 = kx.q('select cnt: count i, avg_close: avg close, avg_vol: avg volume from stocks where symbol like "NVDA", close > 100')
print(q22)
queries['multi_condition_filter'] = str(q22)

# =============================================================================
# CATEGORY F: ADVANCED ANALYTICS (Queries 23-25)
# =============================================================================
print("\n" + "=" * 80)
print("CATEGORY F: ADVANCED ANALYTICS")
print("=" * 80)

# Query 23: Daily OHLC summary
print("\n--- Query 23: Daily OHLC for AAPL (last 5 days) ---")
q23 = kx.q('5 sublist `dt xdesc select open: first open, high: max high, low: min low, close: last close, volume: sum volume by dt: `date$timestamp from stocks where symbol like "AAPL"')
print(q23)
queries['daily_ohlc'] = str(q23)

# Query 24: Moving average (simplified)
print("\n--- Query 24: Price stats with running calculations ---")
q24 = kx.q('select avg_close: avg close, median_close: med close, std_close: dev close by symbol from stocks')
print(q24)
queries['price_stats'] = str(q24)

# Query 25: Correlation between volume and price change
print("\n--- Query 25: Price change analysis by symbol ---")
q25 = kx.q('select avg_daily_range: avg (high - low), avg_spread_pct: avg 100 * (high - low) % low by symbol from stocks')
print(q25)
queries['price_change_analysis'] = str(q25)

print("\n" + "=" * 80)
print("GOLD STANDARD COMPLETE - 25 QUERIES EXECUTED")
print("=" * 80)
