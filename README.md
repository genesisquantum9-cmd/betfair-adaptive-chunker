# Betfair Exchange Adaptive Chunker & SQLite WAL Telemetry

Lightweight Python utilities for running a 24/7 pre-match orderbook collector (L2 top-3 BACK/LAY depth + Pinnacle reference line) on low-resource Linux environments such as **Android Termux**.

## 1. Solving `-32099 (TOO_MUCH_DATA)`

Requesting `listMarketBook` with `EX_BEST_OFFERS` and `EX_TRADED` in fixed batches fails with `-32099` whenever a batch hits heavily traded fixtures with deep orderbooks. This wrapper dynamically halves the batch size on `-32099` and ramps it back up on healthy responses:

```python
import time

def fetch_market_books_adaptive(api_call, market_ids, initial_chunk=20):
    results = []
    chunk_size = initial_chunk
    idx = 0

    while idx < len(market_ids):
        batch = market_ids[idx : idx + chunk_size]
        try:
            resp = api_call(
                marketIds=batch,
                priceProjection={"priceData": ["EX_BEST_OFFERS", "EX_TRADED"]},
            )
            results.extend(resp)
            idx += len(batch)
            chunk_size = min(initial_chunk, chunk_size + 2)
        except Exception as e:
            if "-32099" in str(e) or "TOO_MUCH_DATA" in str(e):
                if chunk_size == 1:
                    idx += 1  # Skip anomalously heavy market
                    continue
                chunk_size = max(1, chunk_size // 2)
                time.sleep(0.5)
            else:
                raise
    return results
```

## 2. Concurrent SQLite Reads/Writes on Flash Storage

To allow separate reader daemons to format telemetry logs while the main loop writes snapshots without `database is locked` errors:

```python
import sqlite3

conn = sqlite3.connect("market_telemetry.db", timeout=15)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA temp_store=MEMORY;")
```

## Live Telemetry Feeds (Raw Output)

Automated reader daemons continuously publish filtered orderbook and cross-book spread logs (strictly raw market telemetry, not financial advice):

- **English Order-Flow Index (Telegraph):** https://telegra.ph/Quant-Radar-Live-Betfair-Money-Flow--Pinnacle-Odds-Movement-Index-11-23
- **Russian Telemetry Stream (Telegram):** https://t.me/real_profit_signals
