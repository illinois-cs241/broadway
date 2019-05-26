# Broadway API
[![Build Status](https://www.travis-ci.com/illinois-cs241/broadway-api.svg?branch=master)](https://www.travis-ci.com/illinois-cs241/broadway-api)
[![Coverage Status](https://coveralls.io/repos/github/illinois-cs241/broadway-api/badge.svg?branch=master)](https://coveralls.io/github/illinois-cs241/broadway-api?branch=master)
![License](https://img.shields.io/badge/license-NCSA%2FIllinois-blue.svg)
![Python Versions](https://img.shields.io/badge/python-3.5%20%7C%203.6-blue.svg)

The Broadway API is a service that receives, distributes, and keeps track of grading jobs and runs.

The aim of this project is to provide a generic interface to a distributed autograding system that can be used by multiple courses. Broadway aims to provide the following benefits:
* More stable and reliable grading runs. No one student can break the entire AG run.
* Faster grading runs. Multiple machines can grade the same assignment.
* Easier tracking and debugging of student failures during grading.
* A more consistent environment to grade student code.
* Easier to scale out the infrastructure.

**_Please read the [Wiki](../../wiki)_ for documentation.** It explains how Broadway works and how to interact with it. Please be sure to read all the pages if you are planning on using Broadway.

## Requirements

MongoDB must be installed and the `mongod` daemon must be running locally before starting the API. Default options are usually sufficient (but for security purposes, be sure to disallow external access to the store).

Python 3.5 is the minimum supported interpreter version. Versions 3.5 and 3.6 are officially supported, but 3.7 should work just as well.

## Configuration

All configuration options are available and documented in-line in `config.py` at the root of the project directory. This is the file that will be imported by the API and used for configuration.

## Running the API
Python dependencies can be installed by executing (from the project root):
```shell
pip3 install -r requirements.txt
```

Then the API can be started by running executing:
```shell
python3 api.py
```

## Starting a Grading Run
We provide a [sample script](scripts/start_run_script.py) to start a grading run. Make sure `HOST` and `PORT` are set correctly. Usage:
```shell
python start_run_script.py <path to grading config json> <path to run time env json> <token>
```
It is recommended to build a CLI which can generate the required config files and start the grading run (so that AG run scheduling can be automated).

## Testing
Please run/modify the [tests](tests) each time a change is made to the logic or structure. You can run tests using:
```shell
python3 -m unittest tests/**/*.py
```

In addition, we run a linter/formatter to keep things standard and clean. For formatting, be sure to execute `black`
```shell
black broadway_api/ tests/ api.py
```

and then `flake8`

```shell
flake8 --config=setup.cfg
```

before opening a pull request.
