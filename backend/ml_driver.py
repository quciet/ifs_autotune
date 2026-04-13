"""Compatibility wrapper for the ML driver CLI."""

from runtime.ml_driver import main


if __name__ == "__main__":
    raise SystemExit(main())
