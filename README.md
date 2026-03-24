# Stock Trend Trader

A professional backtesting tool for a 52-week breakout trend-following strategy applied across the Nasdaq and NYSE universe.

## Strategy Logic

- **Entry:** Candle body (max of open/close, ignoring wicks) makes a new 52-week high
- **Filters:** Price above trend SMA, above context SMA, volume spike confirmation, min avg volume, min price
- **Stop:** ATR trailing stop anchored to highest candle body since entry — only ratchets up, never down
- **Pyramid adds:** When candle body exceeds highest body since last add, with volume confirmation
- **Exits:** ATR trailing stop hit OR max trade age reached
- **Position sizing:** 2% of current equity per trade (compounding), 2:1 leverage

## Default Settings (optimized median)

| Parameter | Value |
|---|---|
| 52W Lookback | 220 bars |
| ATR Period | 125 |
| ATR Multiplier | 7.0x |
| Max Pyramid Adds | 5 |
| Min Bars Between Adds | 19 |
| Max Trade Age | 550 bars |
| Trend SMA | 200 period |
| Context SMA | 70 period |
| Volume Spike | 1.4x avg |
| Base Risk | 2% per trade |

## How to Use

1. Select your universe (Nasdaq + NYSE, Nasdaq, NYSE, or custom tickers)
2. Set your date range and capital
3. Adjust strategy parameters if desired
4. Click **Run Backtest**
5. Review CAGR, Sharpe, Calmar, and equity curve vs QQQ/SPY benchmarks

## Optimization

Use the **Optimization tab** to automatically search for better parameters:
1. Set search ranges for each parameter
2. Choose your target metric (Calmar recommended)
3. Set number of trials (30 is a good starting point)
4. Run multiple times targeting different metrics
5. Take the median of your best results as your robust parameter set

## Benchmarks

The equity curve compares your strategy against:
- **QQQ** — Nasdaq 100 ETF with monthly DCA
- **SPY** — S&P 500 ETF with monthly DCA

Both benchmarks receive the same initial capital and monthly contributions as your strategy for a fair comparison.
