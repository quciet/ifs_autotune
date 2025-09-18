#!/usr/bin/env python3
"""Run the BIGPOPA backend and frontend dev servers together."""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent

SERVICES = (
    {
        "name": "backend",
        "cmd": [sys.executable, "-m", "uvicorn", "app.main:app", "--reload"],
        "cwd": ROOT / "backend",
    },
    {
        "name": "frontend",
        "cmd": ["npm", "run", "dev"],
        "cwd": ROOT / "frontend",
    },
)

processes: list[tuple[str, subprocess.Popen[bytes]]] = []
_shutting_down = False


def _shutdown(signum: int | None = None, frame: object | None = None, *, reason: str | None = None, exit_code: int = 0) -> None:
    """Terminate all running child processes and exit."""
    del frame  # Unused.
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True

    if reason:
        print(f"\n{reason}")
    print("Stopping development environment...")

    for name, proc in processes:
        if proc.poll() is None:
            print(f"Terminating {name} server...")
            proc.terminate()

    for name, proc in processes:
        if proc.poll() is None:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(f"Forcing {name} server to exit...")
                proc.kill()
                proc.wait()

        if proc.returncode and exit_code == 0:
            exit_code = proc.returncode

    sys.exit(exit_code)


def main() -> None:
    """Launch both development servers and monitor them."""
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    for service in SERVICES:
        name = service["name"]
        cmd = service["cmd"]
        cwd = service["cwd"]
        print(f"Starting {name} server: {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(cmd, cwd=cwd)
        except FileNotFoundError as exc:
            _shutdown(reason=f"Failed to start {name} server: {exc}", exit_code=1)
            return
        processes.append((name, proc))

    try:
        while True:
            for name, proc in processes:
                retcode = proc.poll()
                if retcode is not None:
                    exit_status = retcode if retcode != 0 else 1
                    _shutdown(reason=f"{name} server exited with code {retcode}.", exit_code=exit_status)
                    return
            time.sleep(0.5)
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - best effort error surface
        _shutdown(reason=f"Encountered unexpected error: {exc}", exit_code=1)
