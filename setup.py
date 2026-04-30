from setuptools import setup, find_packages

setup(
    name="invest-model",
    version="0.2.0",
    packages=find_packages(include=["invest_model*"]),
    python_requires=">=3.10",
    install_requires=[
        "tushare>=1.2.89",
        "baostock>=0.8.8",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "sqlalchemy>=2.0.0",
        "pymysql>=1.1.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "loguru>=0.7.0",
        "plotly>=5.14.0",
    ],
    extras_require={
        "dev": ["pytest>=7.4.0", "jupyter>=1.0.0", "notebook>=7.0.0", "ipywidgets>=8.0.0"],
    },
)
