"""Compatibility wrapper for the IFs run CLI."""

from ifs.run_ifs import main


if __name__ == "__main__":
    raise SystemExit(main())
