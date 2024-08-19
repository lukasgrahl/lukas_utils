# Repo

## init's
Each init file needs to contain
> from file import method

where file.py holds method()

## How to manage packages
Each package requires a setup.py file, containing package specific information.
Once the setup.py file has been created run the following line in the parent directory:
> python setup.py sdist bdist_wheel

## Install
Install package from GitHub
> pip install lukas_utils@git+https://github.com/lukasgrahl/lukas_utils#egg=lukas_utils