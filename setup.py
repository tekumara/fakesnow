from __future__ import annotations

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel


class BinaryDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True


class PythonIndependentPlatformWheel(bdist_wheel):
    """Build platform-specific wheels that are independent of Python's ABI.

    The bundled DuckDB extension is a native library, but it does not link
    against Python or use the Python C API. The wheel therefore needs a platform
    tag, but not a CPython-version or ABI tag.
    """

    def get_tag(self) -> tuple[str, str, str]:
        _, _, platform_tag = super().get_tag()
        return "py3", "none", platform_tag


setup(
    cmdclass={"bdist_wheel": PythonIndependentPlatformWheel},
    distclass=BinaryDistribution,
)
