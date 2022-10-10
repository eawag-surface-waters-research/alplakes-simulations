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


def collect_data_api(minx, miny, maxx, maxy, day, variables, api):
    today = datetime.today()
    if day != today:
        # /meteoswiss/cosmo/reanalysis/{model}/{start_date}/{end_date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/cosmo/reanalysis/VNXQ34/{}/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        response = requests.get(query)
        if response.status_code == 200:
            data = response.json()
        else:
            raise ValueError("Unable to download data, HTTP error code {}".format(response.status_code))
    else:
        data = get_cosmo_forecast(api, "VNXZ32", variables, day, minx, miny, maxx, maxy)
    return data


def collect_data_local(minx, miny, maxx, maxy, day, variables, folder):
    today = datetime.today()
    if day != today:
        data = get_cosmo_reanalysis(folder, "VNXQ34", variables, day, day, minx, miny, maxx, maxy)
    else:
        data = get_cosmo_forecast(folder, "VNXZ32", variables, day, minx, miny, maxx, maxy)
    return data


def get_cosmo_forecast(filesystem, model, variables, forecast_date, ll_lat, ll_lng, ur_lat, ur_lng):
    file = os.path.join(filesystem, "meteoswiss/cosmo", model, "{}.{}0000.nc".format(model, forecast_date.strftime("%Y%m%d")))
    if not os.path.isfile(file):
        raise ValueError("Data not available for COSMO {} for the following date: {}".format(model, forecast_date))
    output = {}
    with xr.open_mfdataset(file) as ds:
        bad_variables = []
        for var in variables:
            if var not in ds.variables.keys():
                bad_variables.append(var)
        if len(bad_variables) > 0:
            raise ValueError("{} are bad variables for COSMO {}. Please select from: {}".format(
                                    ", ".join(bad_variables), model, ", ".join(ds.keys())))

        output["time"] = np.array(ds.variables["time"].values)
        x, y = np.where(((ds.variables["lat_1"] >= ll_lat) & (ds.variables["lat_1"] <= ur_lat) & (ds.variables["lon_1"] >= ll_lng) & (ds.variables["lon_1"] <= ur_lng)))
        x_min, x_max, y_min, y_max = min(x), max(x), min(y), max(y)
        output["lat"] = ds.variables["lat_1"][x_min:x_max, y_min:y_max].values
        output["lng"] = ds.variables["lon_1"][x_min:x_max, y_min:y_max].values
        for var in variables:
            if var in ds.variables.keys():
                if len(ds.variables[var].dims) == 4:
                    data = ds.variables[var][:, 0, x_min:x_max, y_min:y_max].values
                elif len(ds.variables[var].dims) == 5:
                    data = ds.variables[var][:, 0, 0, x_min:x_max, y_min:y_max].values
                else:
                    data = []
                output[var] = np.where(np.isnan(data), None, data)
            else:
                output[var] = []
    return output


def get_cosmo_reanalysis(filesystem, model, variables, start_date, end_date, ll_lat, ll_lng, ur_lat, ur_lng):
    # For reanalysis files the date on the file is one day after the data in the file
    folder = os.path.join(filesystem, "meteoswiss/cosmo", model)
    files = [os.path.join(folder, "{}.{}0000.nc".format(model, (start_date+timedelta(days=x)).strftime("%Y%m%d")))
             for x in range(0, (end_date-start_date).days + 1)]
    bad_files = []
    for file in files:
        if not os.path.isfile(file):
            bad_files.append(file.split("/")[-1].split(".")[1][:8])
    if len(bad_files) > 0:
        raise ValueError("Data not available for COSMO {} for the following dates: {}".format(model, ", ".join(bad_files)))
    output = {}
    with xr.open_mfdataset(files) as ds:
        bad_variables = []
        for var in variables:
            if var not in ds.variables.keys():
                bad_variables.append(var)
        if len(bad_variables) > 0:
            raise ValueError("{} are bad variables for COSMO {}. Please select from: {}".format(", ".join(bad_variables), model, ", ".join(ds.keys())))
        output["time"] = np.array(ds.variables["time"].values)
        x, y = np.where(((ds.variables["lat_1"] >= ll_lat) & (ds.variables["lat_1"] <= ur_lat) & (
                    ds.variables["lon_1"] >= ll_lng) & (ds.variables["lon_1"] <= ur_lng)))
        x_min, x_max, y_min, y_max = min(x), max(x), min(y), max(y)
        output["lat"] = ds.variables["lat_1"][x_min:x_max, y_min:y_max].values
        output["lng"] = ds.variables["lon_1"][x_min:x_max, y_min:y_max].values
        for var in variables:
            if var in ds.variables.keys():
                if len(ds.variables[var].dims) == 3:
                    data = ds.variables[var][:, x_min:x_max, y_min:y_max].values
                elif len(ds.variables[var].dims) == 4:
                    data = ds.variables[var][:, 0, x_min:x_max, y_min:y_max].values
                else:
                    data = []
                output[var] = np.where(np.isnan(data), None, data)
            else:
                output[var] = []
    return output


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