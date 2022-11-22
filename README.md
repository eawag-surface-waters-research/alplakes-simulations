# Alplakes Simulations

## Installation

- Clone the repository to your local machine using the command: 

 `git clone https://github.com/eawag-surface-waters-research/alplakes-simulations.git`
 
 Note that the repository will be copied to your current working directory.

- Use Python 3 and install the requirements with:

 `pip install -r requirements.txt`

If you want to run the simulations as part of the pipeline you need to install docker.

- Install [docker engine](https://docs.docker.com/desktop/)

 The python version can be checked by running the command `python --version`. In case python is not installed or only an older version of it, it is recommend to install python through the anaconda distribution which can be downloaded [here](https://www.anaconda.com/products/individual). 

## Quick start

`src/main.py` accepts arguments to define the simulation.

The most simple call to the script is as follows:

```commandline
python src/main.py -m delft3d-flow/greifensee -d eawag/delft3d-flow:6.03.00.62434 -s 20221009 -e 20221011
```
Where -m references the static data folder, -d is the docker image, -s is the start date and -e is the end date.

For a full list of inputs run:

```commandline
python src/main.py --help
```

## Expand to other lakes

In order to adapt the processing for other lakes, you need to provide the relevant static files in the `static` folder. 
These are the standard static files for running the simulation (excluding the meteo data files which will be generated)
and a `properties.json` file which defines location, rivers etc. It is best to look at the examples in the `static` folder. 

For a Delft3D flow simulation with no river inputs, the `properties.json file` can be as simple as:

```json
{
  "grid": {"minx": 569000, "miny": 205000, "maxx": 590000, "maxy": 224000, "dx": 500, "dy": 500}
}
```



