"""Compatibility wrapper for the trend-analysis CLI."""

from analysis.trend_analysis import main


if __name__ == "__main__":
    raise SystemExit(main())
