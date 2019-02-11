# Broadway Grader
A worker node in a distributed autograding system which runs a pipeline of docker containers and communicates with the master node ([Broadway API](https://github.com/illinois-cs241/broadway-api)). More information about the distributed autograder and the grader's role and responsibilities are on the API's [Wiki page](https://github.com/illinois-cs241/broadway-api/wiki).

The grader uses the [Chainlink library](https://github.com/illinois-cs241/chainlink) to sequencially run docker containers and aggregate results.

## Installation

The Braodway grader instances require Python 3.5+. 

### Environment

The following environment variables need to be set before continuing:

```sh
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"
```

### Python Packages

We recommend installing packages inside a virtualenv. To create one, run:

```sh
sudo apt-get install python3-pip python3-venv
python3 -m venv venv
```

Then activate the virtualenv and install the packages from requirements.txt:

```sh
source venv/bin/activate
pip3 install -r requirements.txt
```

### Additional Software

Docker is needed to containerize incoming job requests. We recommend using the [convinience script](https://get.docker.com/) on docker.com and completing the suggested [post-installation steps](https://docs.docker.com/install/linux/linux-postinstall/).

## Configuration

Ensure `API_HOST` and `API_PORT` in the config file (config.py) is pointing to the [Broadway API](https://github.com/illinois-cs241/broadway-api) instance you have set up.

## Running

Start the grader using:

```sh
nohup sudo venv/bin/python run.py <cluster token> &
```

Note that under `sudo` the python interpreter path changes to `/usr/bin/python` even when inside a virtual environment.
