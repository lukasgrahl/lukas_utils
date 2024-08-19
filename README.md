# Lukas utils
Util functions for various python projects

## *init.py* files
Each *__init__.py* file needs to contain
> from file import method

where file.py holds method()

## How to manage packages
Each package requires a *pyproject.toml* file, containing package specific information.

The following line initiates the package
> python -m build

## Install
Install package from GitHub
> pip install lukas_utils@git+https://github.com/lukasgrahl/lukas_utils#egg=lukas_utils