-- =============================================================================
-- post_analytics.sql
-- Run this AFTER analytics_job.py has finished writing all tables.
--
-- Usage (from host machine):
--   docker exec -i stocks-postgres psql -U stocks -d stocks_analytics \
--       < spark/post_analytics.sql
--
-- Or from inside the container:
--   psql -U stocks -d stocks_analytics -f /path/to/post_analytics.sql
--
-- Spark JDBC mode="overwrite" drops and recreates tables without indexes.
-- Running this script after each analytics run restores all indexes and views.
-- =============================================================================

\echo '=== Creating indexes ==='

-- ── stock_summary ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_stock_summary_ticker
    ON stock_summary (ticker);

CREATE INDEX IF NOT EXISTS idx_stock_summary_total_volume
    ON stock_summary (total_volume DESC);

CREATE INDEX IF NOT EXISTS idx_stock_summary_avg_close
    ON stock_summary (avg_close DESC);

-- ── price_volatility ──────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_price_vol_ticker
    ON price_volatility (ticker);

CREATE INDEX IF NOT EXISTS idx_price_vol_cv
    ON price_volatility (coeff_variation_pct DESC);

-- ── monthly_performance ───────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_monthly_ticker_month
    ON monthly_performance (ticker, year, month);

CREATE INDEX IF NOT EXISTS idx_monthly_year_month
    ON monthly_performance (year, month);

-- ── yearly_performance ────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_yearly_ticker_year
    ON yearly_performance (ticker, year);

-- ── top_performers ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_top_performers_ticker
    ON top_performers (ticker);

CREATE INDEX IF NOT EXISTS idx_top_performers_pct
    ON top_performers (pct_change DESC);

CREATE INDEX IF NOT EXISTS idx_top_performers_direction
    ON top_performers (direction);

-- ── volume_leaders ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_volume_leaders_ticker
    ON volume_leaders (ticker);

CREATE INDEX IF NOT EXISTS idx_volume_leaders_total
    ON volume_leaders (total_volume DESC);

-- ── dividend_analysis ─────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_dividend_ticker
    ON dividend_analysis (ticker);

CREATE INDEX IF NOT EXISTS idx_dividend_total
    ON dividend_analysis (total_dividends_paid DESC);

-- ── stochastic_signals ────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_stoch_ticker
    ON stochastic_signals (ticker);

CREATE INDEX IF NOT EXISTS idx_stoch_overbought
    ON stochastic_signals (overbought_k DESC);

-- ── daily_market_breadth ──────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_daily_breadth_date
    ON daily_market_breadth (date DESC);

-- ── streaming_window_summary (optional) ──────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_tables
        WHERE tablename = 'streaming_window_summary'
    ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_window_ticker
                     ON streaming_window_summary (ticker)';
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_window_start
                     ON streaming_window_summary (window_start DESC)';
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_window_date
                     ON streaming_window_summary (window_date DESC)';
    END IF;
END $$;

\echo '=== Creating views ==='

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_top_gainers — top 50 all-time gainers
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_top_gainers AS
SELECT
    ticker,
    first_date,
    last_date,
    days_held,
    first_close,
    last_close,
    pct_change,
    absolute_change
FROM top_performers
WHERE direction = 'GAINER'
ORDER BY pct_change DESC
LIMIT 50;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_top_losers — top 50 all-time losers
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_top_losers AS
SELECT
    ticker,
    first_date,
    last_date,
    days_held,
    first_close,
    last_close,
    pct_change,
    absolute_change
FROM top_performers
WHERE direction = 'LOSER'
ORDER BY pct_change ASC
LIMIT 50;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_high_volatility — most volatile stocks (CV > 50 %)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_high_volatility AS
SELECT
    pv.ticker,
    pv.coeff_variation_pct,
    pv.avg_daily_range_pct,
    pv.all_time_high,
    pv.all_time_low,
    pv.green_days,
    pv.red_days,
    pv.win_rate_pct,
    ss.avg_close,
    ss.total_volume
FROM price_volatility pv
JOIN stock_summary    ss ON pv.ticker = ss.ticker
WHERE pv.coeff_variation_pct > 50
ORDER BY pv.coeff_variation_pct DESC;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_most_traded — top 100 tickers by total volume
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_most_traded AS
SELECT
    vl.ticker,
    vl.total_volume,
    vl.avg_daily_volume,
    vl.max_single_day_volume,
    vl.high_volume_days,
    ss.avg_close,
    ss.trading_days
FROM volume_leaders vl
JOIN stock_summary  ss ON vl.ticker = ss.ticker
ORDER BY vl.total_volume DESC
LIMIT 100;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_dividend_champions — dividend-paying stocks sorted by total payout
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_dividend_champions AS
SELECT
    da.ticker,
    da.num_dividend_events,
    da.total_dividends_paid,
    da.avg_dividend_per_event,
    da.max_single_dividend,
    da.approx_dividend_yield_pct,
    ss.avg_close,
    ss.trading_days
FROM dividend_analysis da
JOIN stock_summary     ss ON da.ticker = ss.ticker
ORDER BY da.total_dividends_paid DESC;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_overbought_stocks — stocks most frequently in overbought territory
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_overbought_stocks AS
SELECT
    ticker,
    rows_with_stoch,
    overbought_k,
    oversold_k,
    overbought_rate_pct,
    oversold_rate_pct,
    avg_stochk,
    avg_stochd
FROM stochastic_signals
WHERE overbought_rate_pct > 20
ORDER BY overbought_rate_pct DESC;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_market_trend — 30-day rolling advance/decline ratio
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_market_trend AS
SELECT
    date,
    active_stocks,
    advancing_stocks,
    declining_stocks,
    advance_decline_pct,
    avg_price_change_pct,
    total_market_volume,
    AVG(advance_decline_pct) OVER (
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                               AS rolling_30d_ad_pct,
    AVG(avg_price_change_pct) OVER (
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                               AS rolling_30d_return_pct
FROM daily_market_breadth
ORDER BY date DESC;

-- ──────────────────────────────────────────────────────────────────────────────
-- View: v_full_stock_profile — combined single-row summary per ticker
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_full_stock_profile AS
SELECT
    ss.ticker,
    ss.trading_days,
    ss.first_date,
    ss.last_date,
    ss.first_close,
    ss.last_close,
    ss.avg_close,
    ss.min_close,
    ss.max_close,
    ss.total_volume,
    ss.total_dividends,
    ss.num_splits,
    pv.coeff_variation_pct,
    pv.avg_daily_range_pct,
    pv.win_rate_pct,
    pv.green_days,
    pv.red_days,
    tp.pct_change             AS alltime_pct_change,
    tp.direction,
    COALESCE(da.total_dividends_paid, 0)        AS dividends_paid,
    COALESCE(da.num_dividend_events, 0)          AS dividend_count,
    COALESCE(sto.avg_stochk, NULL)               AS avg_stochk,
    COALESCE(sto.overbought_rate_pct, NULL)      AS overbought_rate_pct
FROM stock_summary     ss
LEFT JOIN price_volatility  pv  ON ss.ticker = pv.ticker
LEFT JOIN top_performers    tp  ON ss.ticker = tp.ticker
LEFT JOIN dividend_analysis da  ON ss.ticker = da.ticker
LEFT JOIN stochastic_signals sto ON ss.ticker = sto.ticker;

\echo '=== post_analytics.sql complete ==='
\echo 'Indexes and views are ready. Connect with:'
\echo '    psql -U stocks -d stocks_analytics'
