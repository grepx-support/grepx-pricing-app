"""
Setup file for grepx-shared-models package
"""
from setuptools import setup, find_packages

setup(
    name="grepx-models",
    version="0.1.0",
    description="Shared database models for all Grepx services",
    author="Grepx Team",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "sqlalchemy>=2.0.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)

