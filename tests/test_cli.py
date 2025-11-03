import os
import signal
import subprocess
import time

from pytest import CaptureFixture

import fakesnow.cli


def test_split() -> None:
    assert fakesnow.cli.split(["pytest", "-m", "integration"]) == (["pytest"], ["-m", "integration"])
    assert fakesnow.cli.split(["-m", "pytest", "-m", "integration"]) == (["-m", "pytest"], ["-m", "integration"])
    assert fakesnow.cli.split(["pytest"]) == (["pytest"], [])
    assert fakesnow.cli.split(["-m", "pytest"]) == (["-m", "pytest"], [])
    assert fakesnow.cli.split(["-d", "databases/", "--module", "pytest", "-m", "integration"]) == (
        ["-d", "databases/", "--module", "pytest"],
        ["-m", "integration"],
    )


def test_run_module(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["-m", "tests.hello"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake world!',)\n"

    fakesnow.cli.main(["-m", "tests.hello", "frobnitz", "--colour", "rainbow", "-m", "integration"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz --colour rainbow -m integration!',)\n"


def test_run_path(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["tests/hello.py"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake world!',)\n"

    fakesnow.cli.main(["tests/hello.py", "frobnitz", "--colour", "rainbow", "-m", "integration"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz --colour rainbow -m integration!',)\n"


def test_run_server() -> None:
    # Start uvicorn server
    proc = subprocess.Popen(["fakesnow", "-s"], stderr=subprocess.PIPE, text=True)
    print("start")

    assert proc.stderr

    # Set stderr pipe to non-blocking mode
    os.set_blocking(proc.stderr.fileno(), False)

    # Collect stderr output
    stderr = ""
    while not stderr:
        chunk = proc.stderr.read(4096)
        stderr += chunk

        print("Waiting for stderr")
        time.sleep(1)

    print(stderr)

    # Send SIGINT to stop uvicorn
    proc.send_signal(signal.SIGINT)
    exit_code = proc.wait()

    # Check if test passed
    assert "Application startup complete" in stderr
    assert exit_code == 0


def test_run_no_args_shows_usage(capsys: CaptureFixture) -> None:
    fakesnow.cli.main([])

    captured = capsys.readouterr()
    assert "usage" in captured.out
