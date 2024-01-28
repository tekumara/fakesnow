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


def test_run_no_args_shows_usage(capsys: CaptureFixture) -> None:
    fakesnow.cli.main([])

    captured = capsys.readouterr()
    assert "usage" in captured.out
