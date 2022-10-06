# -*- coding: utf-8 -*-
import sys
import argparse
from models import *
from functions import verify_args


def main(inputs):
    setups = {"eawag/delft3d-flow:5.01.00.2163": Delft3D_501002163,
              "eawag/delft3d-flow:6.03.00.62434": Delft3D_6030062434}
    verify_args(inputs)
    if inputs.docker in setups:
        setups[inputs.docker](inputs)
    else:
        raise Exception("Currently only the following simulations are supported: {}".format(list(setups.keys())))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', '-m', help="Model name e.g. delft3d-flow/greifensee", type=str)
    parser.add_argument('--docker', '-b', help="Docker image e.g. eawag/delft3d-flow:5.01.00.2163", type=str,)
    parser.add_argument('--start', '-s', help="Start date e.g. 20221901", type=str)
    parser.add_argument('--end', '-e', help="End date e.g. 20221905, defaults to latest forecast", type=str, default=False)
    parser.add_argument('--upload', '-u', help="Bucket to upload data to", type=str, default=False)
    parser.add_argument('--run', '-r', help="Run the simulation", type=bool, default=False)
    args = parser.parse_args()
    main(args)
