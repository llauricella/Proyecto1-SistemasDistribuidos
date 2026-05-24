import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


def start_process(args):
    return subprocess.Popen(
        [PYTHON, *args],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def main() -> int:
    processes = []
    try:
        server = start_process(["server.py", "--host", "127.0.0.1", "--port", "5050"])
        processes.append(("server", server))
        time.sleep(0.8)

        for name in ["V1", "V2", "V3"]:
            proc = start_process(["validator.py", "--name", name, "--host", "127.0.0.1", "--port", "5050"])
            processes.append((name, proc))
            time.sleep(0.2)

        monitor = start_process(
            [
                "monitor.py",
                "--host",
                "127.0.0.1",
                "--port",
                "5050",
                "--validators",
                "V1,V2,V3",
                "--auto",
                "data/transactions.txt",
            ]
        )
        processes.append(("monitor", monitor))

        try:
            output, _ = monitor.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            monitor.kill()
            output, _ = monitor.communicate()
            print("[DEMO] El monitor excedió el tiempo límite.")
            return 1

        print("========== SALIDA DEL MONITOR ==========")
        print(output)
        return 0 if "CONSENSO ALCANZADO" in output else 1

    finally:
        for _, process in processes:
            if process.poll() is None:
                process.terminate()
        time.sleep(0.5)
        for _, process in processes:
            if process.poll() is None:
                process.kill()


if __name__ == "__main__":
    raise SystemExit(main())

