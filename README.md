# db_engines

## Description

This package contains utility functions for interacting with MySQL and MongoDB database backends from Python.

It was originally developed as part of the YouTube video predictive analytics (YTPA) system.


## Installation

Install from PyPI with `pip install db-engines`.

## Make commands

Several make commands are implemented in `Makefile`.

### Testing

Tests are implemented in the `test/` directory, one file per module. Run them locally with `make test`.

### Deploying a new package version

A new version of the package can be deployed by incrementing the version number in `pyproject.toml` and running 
`make publish`. 
A better practice is to `git push` and allow the Github Actions pipeline to take care of running tests and deploying 
the new version only when the entire pipeline succeeds.
Make sure to use the right version of the package in your other environments (update it in a requirements file and 
update the environment).
