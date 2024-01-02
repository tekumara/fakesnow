from pytest import CaptureFixture

import fakesnow.cli


def test_run_module(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["pytest", "-m", "tests.hello", "frobnitz"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz!',)\n"


def test_run_path(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["pytest", "tests/hello.py", "frobnitz"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz!',)\n"
