# Alplakes Simulations

## Installation

:warning: You need to have [git](https://git-scm.com/downloads) and [git-lfs](https://git-lfs.github.com/) installed in order to successfully clone the repository.

- Clone the repository to your local machine using the command: 

 `git clone https://github.com/eawag-surface-waters-research/alplakes-simulations.git`
 
 Note that the repository will be copied to your current working directory.

- Use Python 3 and install the requirements with:

 `pip install -r requirements.txt`

- Install [docker engine](https://docs.docker.com/desktop/)

 The python version can be checked by running the command `python --version`. In case python is not installed or only an older version of it, it is recommend to install python through the anaconda distribution which can be downloaded [here](https://www.anaconda.com/products/individual). 

## Run Simulations

Simulations are defined using .yaml files (see ```tests/``` for example files.) 
They can then be run by calling ```src/main.py``` passing the location of the .yaml file as a parameter.

e.g. ```python src/main.py example.yml```


