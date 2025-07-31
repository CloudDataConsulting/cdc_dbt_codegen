#!/usr/bin/env python
# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""Setup script for CDC DBT Codegen."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_path = Path(__file__).parent / "readme.md"
long_description = readme_path.read_text() if readme_path.exists() else ""

setup(
    name="cdc-dbt-codegen",
    version="0.3.0",
    author="Cloud Data Consultants Inc.",
    author_email="info@clouddataconsulting.com",
    description="Automated dbt code generation for Snowflake data warehouses",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CloudDataConsulting/cdc_dbt_codegen",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        "snowflake-connector-python>=3.0.0",
        "pyyaml>=5.4",
        "dbt-core>=1.0.0,<2.0.0",
        "dbt-snowflake>=1.0.0,<2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.9",
        ],
    },
    entry_points={
        "console_scripts": [
            "cdc-dbt-codegen=cdc_dbt_codegen.cli:main",
            "codegen=cdc_dbt_codegen.cli:main",  # Shorter alias
        ],
    },
    include_package_data=True,
    package_data={
        "cdc_dbt_codegen": ["models/**/*.sql", "seeds/**/*.csv"],
    },
)