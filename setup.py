from setuptools import setup, find_packages

setup(
    name="data-processing-pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "python-dotenv",
        "PyYAML",
        "pytest",
        "black",
        "numpy",
        "scikit-learn",
        "prophet",
        "pymongo",
    ],
)
