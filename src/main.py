# -*- coding: utf-8 -*-
import sys
import argparse
from models import *
from functions import verify_args, boolean_string


def main(params):
    setups = {"eawag/delft3d-flow:5.01.00.2163": delft3d_flow_501002163,
              "eawag/delft3d-flow:6.03.00.62434": delft3d_flow_6030062434}
    if params["docker"] in setups:
        params = verify_args(params)
        run = setups[params["docker"]](params)
        run.process()
    else:
        raise Exception("Currently only the following simulations are supported: {}".format(list(setups.keys())))


if __name__ == "__main__":
    if sys.version_info[0:2] != (3, 9):
        raise Exception('Requires python 3.9')
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', '-m', help="Model name e.g. delft3d-flow/greifensee", type=str)
    parser.add_argument('--docker', '-d', help="Docker image e.g. eawag/delft3d-flow:5.01.00.2163", type=str,)
    parser.add_argument('--start', '-s', help="Start date e.g. 20221901", type=str)
    parser.add_argument('--end', '-e', help="End date e.g. 20221905", type=str)
    parser.add_argument('--bucket', '-b', help="S3 Bucket for restart-files and uploading results.", type=str, default="alplakes-eawag")
    parser.add_argument('--upload', '-u', help="Upload files to bucket?", type=boolean_string, default=False)
    parser.add_argument('--run', '-r', help="Run the simulation", type=boolean_string, default=False)
    parser.add_argument('--files', '-f', help="Path to local files, defaults to API", type=str, default=False)
    parser.add_argument('--api', '-a', help="Url of remote API", type=str, default="http://eaw-alplakes2:8000")
    parser.add_argument('--today', '-t', help="Today's date e.g. 20220102", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument('--log', '-l', help="Log directory", type=str, default=False)
    args = parser.parse_args()
    main(vars(args))
