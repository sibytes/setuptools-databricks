<img src="https://img.shields.io/badge/Python-v3.8-blue">

# Introduction 

Builds parameterised databricks cluster definitions (yaml) and init scripts (sh) for cluster deployment.

What problem does this solve?

It will automatically name clusters using databricks runtime version numbers allowing you to maintain tight version control of cluster definitions from code. 

It also will automatically build an init script for clusters that automatically install the current build version of custom libs. This means there's no need to start up clusters to uninstall the existing libs. The existing cluster is destroyed and a new one is created with an init scripts with the current lib. Or you can leave the existing cluster in place and create a new version of the cluster with the latest libs.

## Examples:

stub

# Setup

Create virual environment and install dependencies for local development:

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r dev_requirements.txt
```


# Build

Build python wheel for Databricks cluster:
```
python setup.py sdist bdist_wheel
```


# Test



```
pip install --editable .

pytest
```


