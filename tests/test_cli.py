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

    # Wait for startup then hit CTRL+C
    time.sleep(1)
    proc.send_signal(signal.SIGINT)
    exit_code = proc.wait()

    # Collect stderr output
    assert proc.stderr
    stderr_output = proc.stderr.read()

    # Check if test passed
    assert "Application startup complete" in stderr_output
    assert exit_code == 0


def test_run_no_args_shows_usage(capsys: CaptureFixture) -> None:
    fakesnow.cli.main([])

    captured = capsys.readouterr()
    assert "usage" in captured.out
