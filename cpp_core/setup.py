from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup, Extension
import pybind11
import os

# 定义扩展模块
ext_modules = [
    Pybind11Extension(
        "db_core",
        [
            "db_engine.cpp",
        ],
        include_dirs=[
            pybind11.get_cmake_dir() + "/../../../include",
        ],
        cxx_std=17,
        extra_compile_args=[
            "/utf-8",
        ],
    ),
]

setup(
    name="db_core",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
    python_requires=">=3.6",
)
