from pytest import CaptureFixture

import fakesnow.cli


def test_run_module(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["-m", "tests.hello"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake world!',)\n"

    fakesnow.cli.main(["-m", "tests.hello", "frobnitz", "--colour", "rainbow"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz --colour rainbow!',)\n"


def test_run_path(capsys: CaptureFixture) -> None:
    fakesnow.cli.main(["tests/hello.py"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake world!',)\n"

    fakesnow.cli.main(["tests/hello.py", "frobnitz", "--colour", "rainbow"])

    captured = capsys.readouterr()
    assert captured.out == "('Hello fake frobnitz --colour rainbow!',)\n"


def test_run_no_args_shows_usage(capsys: CaptureFixture) -> None:
    fakesnow.cli.main([])

    captured = capsys.readouterr()
    assert "usage" in captured.out
