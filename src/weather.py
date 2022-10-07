# -*- coding: utf-8 -*-
import netCDF4
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def cosmo_point_timeseries(coordinates, parameter, files):
    out = [[] for _ in range(len(coordinates))]
    time = []
    for file in files:
        date = datetime.strptime(file.split(".")[-2].split("_")[-1], '%Y%m%d')
        time = time + list(np.arange(date, date + timedelta(days=1), timedelta(hours=1)).astype(datetime))
        nc = netCDF4.Dataset(file, mode='r', format='NETCDF4_CLASSIC')
        lat = nc.variables["lat_1"][:]
        lon = nc.variables["lon_1"][:]

        for i in range(len(coordinates)):
            dist = np.sqrt((lat - coordinates[i][0]) ** 2 + (lon - coordinates[i][1]) ** 2)
            min_dist = np.argwhere(dist == np.min(dist))[0]
            out[i] = out[i] + list(np.array(nc.variables[parameter][:, min_dist[0], min_dist[1]]) - 273.15)
        nc.close()

    pt = []
    for i in range(len(coordinates)):
        df = pd.DataFrame(list(zip(time, out[i])), columns=['datetime', parameter])
        df = df.sort_values(by=['datetime'])
        pt.append(df)
    return pt