from setuptools import setup

setup(
    name="curvinefs",
    version="0.1",
    entry_points={
        "fsspec.specs": [
            "curvinefs = python.CurvineFileSystem", 
        ],
    },
)
