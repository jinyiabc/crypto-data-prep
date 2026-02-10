#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="crypto-data-prep",
    version="0.1.0",
    description="Cryptocurrency data preparation toolkit",
    author="Your Name",
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "requests>=2.31.0",
        "python-dateutil>=2.8.2",
    ],
    extras_require={
        "ibkr": ["ib-insync>=0.9.86"],
        "dev": ["pytest", "black", "flake8"],
    },
    entry_points={
        "console_scripts": [
            "crypto-data=main:main",
        ],
    },
)
