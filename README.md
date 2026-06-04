# Alplakes Simulations

Python framework for setting up, running, and post-processing 3D hydrodynamic simulations of Alpine lakes using Delft3D Flow and MITgcm, simulations are run using Docker.

> **Note:** Meteorological and river discharge data are sourced from Eawag-internal APIs. Eawag users must be connected to the Eawag intranet. External users will need to modify the relevant functions in [src/weather.py](src/weather.py) and [src/river.py](src/river.py) to connect to their own data sources.

## Installation

```bash
git clone https://github.com/eawag-surface-waters-research/alplakes-simulations.git
cd alplakes-simulations
pip install -r requirements.txt
```

## Supported models

| Docker image | Model |
|---|---|
| `eawag/delft3d-flow:5.01.00.2163` | Delft3D Flow 5.01 |
| `eawag/delft3d-flow:6.02.10.142612` | Delft3D Flow 6.02 |
| `eawag/delft3d-flow:6.03.00.62434` | Delft3D Flow 6.03 |
| `eawag/mitgcm:67z` | MITgcm 67z |

## Usage

### Setup — `src/main.py`

Prepares input files and optionally runs the simulation.

```bash
python src/main.py -m delft3d-flow/greifensee -d eawag/delft3d-flow:6.03.00.62434 -s 20221009 -e 20221011
```

| Argument | Short | Description | Default |
|---|---|---|---|
| `--model` | `-m` | Static data folder, e.g. `delft3d-flow/greifensee` | required |
| `--docker` | `-d` | Docker image | required |
| `--start` | `-s` | Start date `YYYYMMDD` | required |
| `--end` | `-e` | End date `YYYYMMDD` | required |
| `--run` | `-r` | Execute simulation after setup | false |
| `--upload` | `-u` | Upload input files to S3 | false |
| `--profile` | `-p` | Profile name to initialise from (`{lake}/profiles`) | false |
| `--restart` | `-z` | Path to restart file | false |
| `--threads` | `-th` | Number of threads | 1 |
| `--bucket` | `-b` | S3 bucket name | `alplakes-eawag` |
| `--api` | `-a` | Alplakes API URL | `http://eaw-alplakes2:8000` |
| `--today` | `-t` | Override today's date `YYYYMMDD` | system date |
| `--log` | `-l` | Log output directory | stdout |

### Run simulation

See [eawag-surface-waters-research/docker](https://github.com/eawag-surface-waters-research/docker) for instructions on installing the Docker images.

**Delft3D Flow** — navigate to the generated run folder and execute:

```bash
cd {{ run folder }}
docker run -v $(pwd):/job --rm eawag/delft3d-flow:6.02.10.142612
```

**MITgcm** — the setup step generates a `Dockerfile` inside the run folder tailored to the lake's inputs. Build the image from that folder, then run it:

```bash
cd {{ run folder }}
docker build -t eawag/mitgcm:67z_{{ lake }} .
docker run \
  -v $(pwd)/binary_data:/simulation/binary_data \
  -v $(pwd)/run_config:/simulation/run_config \
  -v $(pwd)/run:/simulation/run \
  eawag/mitgcm:67z_{{ lake }}
```

### Post-processing — `src/postprocess.py`

Verifies results, splits output into weekly NetCDF files, and computes derived variables (e.g. thermocline).

```bash
python src/postprocess.py -f {{ run folder }} -d eawag/delft3d-flow:6.02.10.142612
```

| Argument | Short | Description | Default |
|---|---|---|---|
| `--folder` | `-f` | Simulation run folder | required |
| `--docker` | `-d` | Docker image used for the simulation | `eawag/delft3d-flow:6.02.10.142612` |
| `--skip` | `-s` | Skip weeks before `YYYYMMDD` | false |

## Adding a new lake

Copy an existing lake folder from `static/{model}/` that is similar to your target lake (e.g. similar size or river configuration) and rename it to your lake. Replace the static simulation input files with those for your lake, then update `properties.json` to match your lake's grid, rivers, secchi depth, etc. Meteo files are generated at runtime and do not need to be included.

Minimal `properties.json` for a Delft3D Flow lake with no river inputs:

```json
{
  "grid": {"minx": 569000, "miny": 205000, "maxx": 590000, "maxy": 224000, "dx": 500, "dy": 500}
}
```

## Change log

#### 27 June 2024 - Temporary fix to improve summer surface temperature prediction
Zurich and Geneva switched to half secchi values and D3D 6.03.00.62434 (old model) due to underprediction
of summer surface temperature. The full dataset was not re-computed hence there is a discrepancy in the timeseries on the
19 May 2024 where it shifts from the old values of secchi and the new model to the half secchi values and the old model.
