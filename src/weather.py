# -*- coding: utf-8 -*-
import os
import pytz
import netCDF4
import requests
import xarray as xr
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from datetime import datetime, timedelta
from functions import latlng_to_ch1903


def write_weather_data_to_file(time, var, lat, lng, gxx, gyy, properties, folder, origin=datetime(2008, 3, 1, tzinfo=pytz.utc)):
    var = np.array(var) + properties["adjust"]
    time = np.array(time, dtype="datetime64")
    lat = np.array(lat)
    lng = np.array(lng)
    with open(os.path.join(folder, properties["filename"]), "a") as f:
        for i in range(len(time)):
            diff = datetime.fromtimestamp(time[i].astype('datetime64[s]').astype('int'), pytz.utc) - origin
            hours = diff.total_seconds() / 3600
            time_str = "TIME = " + str(hours) + "0 hours since " + origin.strftime(
                "%Y-%m-%d %H:%M:%S") + " +00:00"
            f.write(time_str)
            mx, my = latlng_to_ch1903(lat, lng)
            mxx, myy = mx.flatten(), my.flatten()
            grid_interp = griddata((mxx, myy), var[i].flatten(), (gxx, gyy))
            grid_interp[np.isnan(grid_interp)] = -999.00
            f.write("\n")
            np.savetxt(f, grid_interp, fmt='%.2f')


def download_meteolakes_cosmo_area(minx, miny, maxx, maxy, day, variables, api, today):
    if day.strftime("%Y%m%d") != today.strftime("%Y%m%d"):
        # /meteoswiss/cosmo/area/reanalysis/{model}/{start_date}/{end_date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/cosmo/area/reanalysis/VNXQ34/{}/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        print(query)
        response = requests.get(query)
        if response.status_code == 200:
            data = response.json()
        else:
            raise ValueError("Unable to download data, HTTP error code {}".format(response.status_code))
    else:
        # /meteoswiss/cosmo/area/forecast/{model}/{date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/cosmo/area/forecast/VNXZ32/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        print(query)
        response = requests.get(query)
        if response.status_code == 200:
            data = response.json()
        else:
            raise ValueError("Unable to download data, HTTP error code {}".format(response.status_code))
    return data


def download_meteolakes_cosmo_point(x, y, start, end, variables, api, today):
    print("download_meteolakes_cosmo_point not currently implemented")
