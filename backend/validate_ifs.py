"""Compatibility wrapper for the IFs validation CLI."""

from ifs.validate_ifs import main


if __name__ == "__main__":
    raise SystemExit(main())
