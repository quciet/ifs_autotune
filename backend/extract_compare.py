"""Compatibility wrapper for the IFs extract-and-compare CLI."""

from ifs.extract_compare import main


if __name__ == "__main__":
    raise SystemExit(main())
