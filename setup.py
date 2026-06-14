from setuptools import find_packages, setup

setup(
    name="invest-model",
    version="0.3.0",
    packages=find_packages(include=["invest_model*"]),
    python_requires=">=3.10",
    install_requires=[
        "tushare>=1.2.89",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "pyarrow>=12.0.0",
        "sqlalchemy>=2.0.0",
        "pymysql>=1.1.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "loguru>=0.7.0",
    ],
    extras_require={
        "dev": ["pytest>=7.4.0"],
    },
)
