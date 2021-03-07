<img src="https://img.shields.io/badge/Python-v3.7-blue">

# Introduction 

stub

## Examples:

stub

# Setup

Create virual environment and install dependencies for local development:

```
python3.7 -m venv venv
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


