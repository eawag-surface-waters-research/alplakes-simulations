# -*- coding: utf-8 -*-
import os
import pytz
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from datetime import datetime
from functions import latlng_to_ch1903, latlng_to_utm, download_data


def write_weather_data_to_file(time, var, lat, lng, gxx, gyy, system, properties, folder, no_data_value, origin=datetime(2008, 3, 1, tzinfo=pytz.utc), method='linear', warning=print):
    var = np.array(pd.to_numeric(var, errors='coerce'), dtype=float)
    var = var + properties["adjust"]
    if "min" in properties and np.any(var < properties["min"]):
        var[var < properties["min"]] = properties["min"]
        warning("{} values detected below allowable min of {}{}, setting values to min."
                .format(properties["parameter"], properties["min"], properties["unit"]))
    if "max" in properties and np.any(var > properties["max"]):
        var[var > properties["max"]] = properties["max"]
        warning("{} values detected above allowable max of {}{}, setting values to max."
                .format(properties["parameter"], properties["min"], properties["unit"]))
    time = np.array(time, dtype="datetime64")
    lat = np.array(lat)
    lng = np.array(lng)
    if system == "WGS84":
        mx, my = latlng_to_ch1903(lat, lng)
    elif system == "CH1903":
        mx, my = latlng_to_ch1903(lat, lng)
    elif system == "UTM":
        mx, my, zone_number, zone_letter = latlng_to_utm(lat, lng)
    else:
        raise ValueError("{} not implemented as a coordinate system.".format(system))
    mxx, myy = mx.flatten(), my.flatten()
    if np.nanmax(gxx) > np.nanmax(mxx) or np.nanmax(gyy) > np.nanmax(myy) or np.nanmin(gxx) < np.nanmin(mxx) or np.nanmin(gyy) < np.nanmin(myy):
        method = 'nearest'
        warning("Lake grid area exceeds model weather area")
    with open(os.path.join(folder, properties["filename"]), "a") as f:
        for i in range(len(time)):
            diff = datetime.fromtimestamp(time[i].astype('datetime64[s]').astype('int'), pytz.utc) - origin
            hours = diff.total_seconds() / 3600
            time_str = "TIME = " + str(hours) + "0 hours since " + origin.strftime(
                "%Y-%m-%d %H:%M:%S") + " +00:00"
            f.write(time_str)
            v = var[i].flatten()
            if len(v[~np.isnan(v)]) == 0:
                warning("Zero valid points, timestep will be no_data values only.")
            grid_interp = griddata((mxx, myy), v, (gxx, gyy), method=method)
            grid_interp[np.isnan(grid_interp)] = no_data_value
            f.write("\n")
            np.savetxt(f, np.flip(grid_interp, 0), fmt='%.2f')


def download_meteolakes_cosmo_area(minx, miny, maxx, maxy, day, variables, api, today):
    if day.strftime("%Y%m%d") != today.strftime("%Y%m%d"):
        # /meteoswiss/cosmo/area/reanalysis/{model}/{start_date}/{end_date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/cosmo/area/reanalysis/VNXQ34/{}/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        data = download_data(query)
        if data == False:
            raise ValueError("Unable to download data.")
    else:
        # /meteoswiss/cosmo/area/forecast/{model}/{date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/cosmo/area/forecast/VNXZ32/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        data = download_data(query)
        if data:
            for key in list(data.keys()):
                if "_MEAN" in key:
                    data[key.replace("_MEAN", "")] = data.pop(key)
        else:
            raise ValueError("Unable to download data.")
    return data


def download_meteolakes_icon_area(minx, miny, maxx, maxy, day, variables, api, today):
    if day.strftime("%Y%m%d") != today.strftime("%Y%m%d"):
        # /meteoswiss/icon/area/reanalysis/{model}/{start_date}/{end_date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/icon/area/reanalysis/kenda-ch1/{}/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        data = download_data(query)
        if data == False:
            raise ValueError("Unable to download data.")
    else:
        # /meteoswiss/icon/area/forecast/{model}/{date}/{ll_lat}/{ll_lng}/{ur_lat}/{ur_lng}
        query = "{}/meteoswiss/icon/area/forecast/icon-ch2-eps/{}/{}/{}/{}/{}"
        query = query.format(api, day.strftime("%Y%m%d"), minx, miny, maxx, maxy)
        data = download_data(query)
        if data:
            for key in list(data.keys()):
                if "_MEAN" in key:
                    data[key.replace("_MEAN", "")] = data.pop(key)
        else:
            raise ValueError("Unable to download data.")
    return data


def download_meteolakes_cosmo_point(x, y, start, end, variables, api, today):
    print("download_meteolakes_cosmo_point not currently implemented")
