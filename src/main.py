# -*- coding: utf-8 -*-
import sys
from models import *
from functions import parse_parameters


def main(file):
    setups = {"delft3d_5.01.00.2163": Delft3D_501002163,
              "delft3d_6.03.00.62434": Delft3D_6030062434}
    parameters = parse_parameters(file)
    if parameters["setup"] in setups:
        run = setups[parameters["setup"]](parameters)
        run.process()
    else:
        raise Exception("Currently only the following setups are supported: {}".format(list(setups.keys())))


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(str(sys.argv[1]).replace('\\', '/'))
    else:
        raise Exception("Run file path must be provided as an argument.")

