import pathlib
from setuptools import setup
import os


# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()
DATABRICKS_RUNTIME="dbr7.3"

# This call to setup() does all the work
setup(
    name=f"setuptools-databricks",
    version_config={
        "template": "{tag}",
        "dev_template": "{tag}.dev{ccount}",
        "dirty_template": "{tag}.dev{ccount}.git{sha}.dirty",
        "starting_version": "0.1.0",
        "version_callback": None,
        "version_file": None,
        "count_commits_from_version_file": False
    },
    setup_requires=['setuptools-git-versioning'],
    description="Databricks Project Setuptools",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/semanticinsight/setup-databricks.git",
    author="Shaun Ryan",
    author_email="shaun_chiburi@hotmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["setuptools_databricks"],
    zip_safe=False
)
