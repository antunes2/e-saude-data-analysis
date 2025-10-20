# setup.py
from setuptools import setup, find_packages

setup(
    name="e-saude-analysis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "psycopg2-binary", 
        "python-dotenv",
        "openpyxl"
    ],
)