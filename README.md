# Broadway

[![Build Status](https://www.travis-ci.com/illinois-cs241/broadway.svg?branch=master)](https://www.travis-ci.com/illinois-cs241/broadway)
[![Coverage Status](https://coveralls.io/repos/github/illinois-cs241/broadway/badge.svg?branch=master)](https://coveralls.io/github/illinois-cs241/broadway?branch=master)
![License](https://img.shields.io/badge/license-NCSA%2FIllinois-blue.svg)
![Python Versions](https://img.shields.io/badge/python-3.5%20%7C%203.6-blue.svg)

The Broadway is a distributed grading service that receives, executes, and keeps track of grading jobs and runs.

The aim of this project is to provide a generic interface to a distributed autograding system that can be used by multiple courses. Broadway aims to provide the following benefits:
* More stable and reliable grading runs. No one student can break the entire AG run.
* Faster grading runs. Multiple machines can grade the same assignment.
* Easier tracking and debugging of student failures during grading.
* A more consistent environment to grade student code.
* Easier to scale out the infrastructure.

**_Please read the [Wiki](https://github.com/illinois-cs241/broadway/wiki) for documentation._** It explains how Broadway works and how to interact with it. Please be sure to read all the pages if you are planning on using Broadway.

See our [contribution guidelines](CONTRIBUTING.md) if you want to contribute.

## Requirements

MongoDB must be installed and the `mongod` daemon must be running locally before starting the API. Default options are usually sufficient (but for security purposes, be sure to disallow external access to the store).

Python 3.5 is the minimum supported interpreter version. Versions 3.5 and 3.6 are officially supported, but 3.7 should work just as well.

To install the dependencies (with venv)

    python3 -m venv venv
    . venv/bin/activate
    pip3 install -r requirements.txt

## Configuration

Most of our configuration variables can be set from three sources: command-line flags,
environment variables, config file, in the order of decreasing precedence.

## Broadway API and grader

API and grader are two major parts of a broadway cluster. API is in charge of receiving and scheduling jobs
across graders, while graders have the simple job of executing them in containers.

To bring up a functioning broadway cluster, you have spin up API first, then connect grader to the API
using the authentication token (either given or automatically generated).

## Running the API

(After installing requirements)

    python3 -m broadway.api [--token TOKEN] [--bind-addr ADDR] [--bind-port PORT]

More info can be found by running `python3 -m broadway.api --help`

## Running the grader

`broadway.grader` takes two positional arguments, where `TOKEN` is the cluster token in API,
and `GRADER_ID` should be a unique identifier of the grader (and only letters, digits, and dashes are allowed)

`API_ADDR` points to where API was bound to along with the protocol you wish to use.
e.g. `ws://127.0.0.1:1470` means that grader should find API at `127.0.0.1:1470` and
use the websocket version of our protocol.

    python3 -m broadway.grader <TOKEN> <GRADER_ID> [--api-host API_ADDR]

More info can be found by running `python3 -m broadway.grader --help`
