"""Quote data providers — one module per external source.

Each provider fetches price/prev_close/market_cap (and optionally PE) for a
slice of the portfolio universe. The top-level stock_quote service routes
tickers to the right provider by stock_market label.
"""
