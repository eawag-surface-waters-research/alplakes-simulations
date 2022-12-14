# -*- coding: utf-8 -*-
import os
import math
import requests
import numpy as np
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from datetime import datetime, timedelta

from functions import logger


def download_bafu_hydrodata_measured(api, station_id, parameter, start_date, end_date):
    # /bafu/hydrodata/measured/{station_id}/{parameter}/{start_date}/{end_date}
    query = "{}/bafu/hydrodata/measured/{}/{}/{}/{}".format(api, station_id, parameter, start_date, end_date)
    response = requests.get(query)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.json())
        raise ValueError("Unable to download data, HTTP error code {}".format(response.status_code))


def download_bafu_hydrodata(parameters, start, end, api, today, log=logger):
    forecast = False
    if end.strftime("%Y%m%d") == today.strftime("%Y%m%d") or end > today:
        forecast = True
        end = today - timedelta(days=1)

    for key in ["inflows", "outflows", "waterlevels"]:
        if key in parameters:
            for parameter in parameters[key]:
                if "station_id" in parameter:
                    for parameter_type in ["flow", "temperature", "level"]:
                        if parameter_type in parameter and "parameter" in parameter[parameter_type]:
                            log.info("Downloading {} from {} ({}) from {} to {}"
                                     .format(parameter[parameter_type]["parameter"],
                                             parameter["station_name"],
                                             parameter["station_id"],
                                             start.strftime("%Y%m%d"),
                                             end.strftime("%Y%m%d")),
                                     indent=2)
                            parameter[parameter_type]["data"] = \
                                download_bafu_hydrodata_measured(api,
                                                                 parameter["station_id"],
                                                                 parameter[parameter_type]["parameter"],
                                                                 start.strftime("%Y%m%d"),
                                                                 end.strftime("%Y%m%d"))
                            if forecast and "forecast" in parameter[parameter_type]:
                                log.warning("Forecast collection not yet implemented, data will be extrapolated.", indent=2)

    return parameters


def clean_smooth_resample(parameters, start_date, end_date, dt=timedelta(minutes=10), log=logger, plot=False):
    time = np.arange(start_date, end_date, dt)
    for key in ["inflows", "outflows", "waterlevels"]:
        if key in parameters:
            for parameter in parameters[key]:
                if "station_id" in parameter:
                    for parameter_type in ["flow", "temperature", "level"]:
                        if parameter_type in parameter and "parameter" in parameter[parameter_type]:
                            log.info("Processing {} from {} ({})"
                                     .format(parameter[parameter_type]["parameter"],
                                             parameter["station_name"],
                                             parameter["station_id"], ),
                                     indent=2)
                            # Clean
                            df = pd.DataFrame(parameter[parameter_type]["data"])
                            df.columns = ["ds", "y"]
                            df["ds"] = pd.to_datetime(df["ds"], errors="coerce", utc=True)
                            df["y"] = pd.to_numeric(df["y"], errors="coerce")
                            df = df.dropna()
                            df['y'].where(df['y'] < parameter[parameter_type]["max"], parameter[parameter_type]["max"],
                                          inplace=True)
                            df['y'].where(df['y'] > parameter[parameter_type]["min"], parameter[parameter_type]["min"],
                                          inplace=True)

                            # Smooth
                            if "smooth" in parameter[parameter_type]:
                                df["y"] = df["y"].rolling(window=parameter[parameter_type]["smooth"]["window"],
                                                          win_type='gaussian', min_periods=10,
                                                          center=True).mean(
                                    std=parameter[parameter_type]["smooth"]["std"])
                                df = df.dropna()

                            # Resample
                            f = interp1d(df["ds"].astype(int) / 10 ** 9, df["y"], kind='linear', bounds_error=False, fill_value=np.nan)
                            df = pd.DataFrame({"ds": time, "y": f(time.astype('int') / 10 ** 6)})
                            if plot:
                                plt.plot(df["ds"], df["y"])
                                plt.title(parameter["station_name"] + " " + parameter[parameter_type]["parameter"])
                                plt.show()
                            parameter[parameter_type]["data"] = df
    return parameters


def forecast(parameters, log=logger, plot=False):
    for key in ["inflows", "outflows", "waterlevels"]:
        if key in parameters:
            for parameter in parameters[key]:
                if "station_id" in parameter:
                    for parameter_type in ["flow", "temperature", "level"]:
                        if parameter_type in parameter and "parameter" in parameter[parameter_type]:
                            log.info("Processing {} from {} ({})"
                                     .format(parameter[parameter_type]["parameter"],
                                             parameter["station_name"],
                                             parameter["station_id"], ),
                                     indent=2)

                            # Forecast
                            if parameter_type == "flow":
                                full = forecast_flow(parameter[parameter_type]["data"])
                            elif parameter_type == "temperature":
                                full = forecast_temperature(parameter[parameter_type]["data"])
                            elif parameter_type == "level":
                                full = forecast_level(parameter[parameter_type]["data"])

                            parameter[parameter_type]["data"] = full
                            if plot:
                                plt.plot(full["ds"], full["y"])
                                plt.show()


def forecast_flow(df, method="fixed"):
    if method == "fixed":
        return forecast_fixed(df)
    else:
        raise ValueError("Method {} not implemented.".format(method))


def forecast_temperature(df, method="fixed"):
    if method == "fixed":
        return forecast_fixed(df)
    else:
        raise ValueError("Method {} not implemented.".format(method))


def forecast_level(df, method="fixed"):
    if method == "fixed":
        return forecast_fixed(df)
    else:
        raise ValueError("Method {} not implemented.".format(method))


def forecast_fixed(df):
    data = np.array(df["y"])
    fvi = df["y"].first_valid_index()
    lvi = df["y"].last_valid_index()
    if fvi > 0:
        data[0:fvi] = data[fvi]
    if lvi < len(df):
        data[lvi:] = data[lvi]
    df["y"] = data
    return df


def flow_balance(parameters, log=logger, plot=False):
    return parameters


def write_river_data_to_file(parameters, folder, start, end, filename="RiversOperationsQuantities.dis",
                             origin=datetime(2008, 3, 1), log=logger):
    f = open(os.path.join(folder, filename), "w")
    for inflow in parameters["inflows"]:
        f.write("table-name           'Discharge : {}'\n".format(str(inflow["id"])))
        f.write("contents             'momentum'\n")
        f.write("location             '{}'\n".format(inflow["name"]))
        f.write("time-function        'non-equidistant'\n")
        f.write("reference-time       {}\n".format(origin.strftime("%Y%m%d")))
        f.write("time-unit            'minutes'\n")
        f.write("interpolation        'linear'\n")
        f.write("parameter            'time                '                     unit '[min]'\n")
        f.write("parameter            'flux/discharge rate '                     unit '[m3/s]'\n")
        f.write("parameter            'Temperature         '                     unit '[°C]'\n")
        f.write("parameter            'flow magnitude      '                     unit '[m/s]'\n")
        f.write("parameter            'flow direction      '                     unit '[deg]'\n")
        f.write("records-in-table     {}\n ".format(str(len(inflow["data"]))))

        df = inflow["data"]
        df["flow_velocity"] = inflow["flow_velocity"]
        df["flow_direction"] = inflow["flow_direction"]
        df["minutes"] = (df["datetime"] - origin).astype('timedelta64[m]')
        out = np.column_stack((df["minutes"], df["flow"], df["temperature"], df["flow_velocity"], df["flow_direction"]))
        np.savetxt(f, out, fmt="%.7e", delimiter="   ", newline="\n ")

    f.write("table-name           'Discharge : {}'\n".format(str(parameters["outflow"]["id"])))
    f.write("contents             'regular  '\n")
    f.write("location             '{}           '\n".format(parameters["outflow"]["name"]))
    f.write("time-function        'non-equidistant'\n")
    f.write("reference-time       {}\n".format(origin.strftime("%Y%m%d")))
    f.write("time-unit            'minutes'\n")
    f.write("interpolation        'linear'\n")
    f.write("parameter            'time                '                     unit '[min]'\n")
    f.write("parameter            'flux/discharge rate '                     unit '[m3/s]'\n")
    f.write("parameter            'Temperature         '                     unit '[°C]'\n")
    f.write("records-in-table     {}\n ".format(str(len(parameters["outflow"]["data"]))))

    df = parameters["outflow"]["data"]
    df["temperature"] = 6
    df["minutes"] = (df["datetime"] - origin).astype('timedelta64[m]')
    out = np.column_stack((df["minutes"], -df["flow"], df["temperature"]))
    np.savetxt(f, out, fmt="%.7e", delimiter="   ", newline="\n ")

    f.close()


# OLD FUNCTIONS
def get_water_level_change_flow_equivalent(df_wl, altitude, bathymetry):
    df_wl['level+'] = df_wl['level'].shift(-1)
    df_wl.iloc[-1, df_wl.columns.get_loc('level+')] = df_wl['level'].iloc[-1]
    depth_area = interp1d(bathymetry["depth"], bathymetry["area"], fill_value="extrapolate")
    df_wl["area"] = depth_area(altitude - df_wl['level'])
    df_wl["area+"] = depth_area(altitude - df_wl['level+'])
    df_wl["area_mean"] = df_wl[['area', 'area+']].mean(axis=1)
    df_wl["dV"] = (df_wl['level+'] - df_wl['level']) * df_wl["area_mean"]
    dt = (df_wl["datetime"].iloc[1] - df_wl["datetime"].iloc[0]).total_seconds()
    df_wl["flow"] = df_wl["dV"] / dt
    return np.array(df_wl["flow"])


def predict_water_level(parameters):
    flow_change = -np.array(parameters["outflow"]["data"]["flow"])
    for inflow in parameters["inflows"]:
        flow_change = flow_change + inflow["data"]["flow"]

    dt = (parameters["outflow"]["data"]["datetime"].iloc[1] - parameters["outflow"]["data"]["datetime"].iloc[
        0]).total_seconds()

    level = np.array(parameters["waterlevel"]["data"]["level"])[0]

    dv = flow_change * dt

    df = pd.read_csv("geneva.csv")
    depth_volume = interp1d(df[" Depth (m)"], df["Volume (m3)"], fill_value="extrapolate")
    volume_depth = interp1d(df["Volume (m3)"], df[" Depth (m)"], fill_value="extrapolate")

    volume = [depth_volume(parameters["altitude"] - level)]

    for i in range(len(dv)):
        volume.append(volume[-1] + dv[i])
    wl = parameters["altitude"] - volume_depth(np.array(volume).astype(float))

    return wl


def verify_flow_balance(parameters):
    water_level_flow = get_water_level_change_flow_equivalent(parameters["waterlevel"]["data"], parameters["altitude"],
                                                              parameters["bathymetry"])
    predicted_water_level = predict_water_level(parameters)
    outflow = np.array(parameters["outflow"]["data"]["flow"])

    extra_flow = outflow + water_level_flow

    for inflow in parameters["inflows"]:
        extra_flow = extra_flow - inflow["data"]["flow"]

    ax = plt.subplot(311)
    ax.plot(extra_flow)
    ax = plt.subplot(312)
    ax.plot(np.cumsum(extra_flow))
    ax = plt.subplot(313)
    ax.plot(parameters["waterlevel"]["data"]["level"], label="real")
    ax.plot(predicted_water_level, label="predicted")
    ax.legend()
    plt.show()


def create_periods(dt, rebalance_period, length):
    steps = min(round(rebalance_period / dt), round(length / 2))
    period = math.floor(length / steps)
    periods = []
    for p in range(period):
        periods.append([p * steps, p * steps + steps])
    periods[-1][1] = length
    return periods


def flow_balance_ungauged_rivers(parameters, min_flow=0.0001, dt=600, rebalance_period=604800):
    outflow = np.array(parameters["outflow"]["data"]["flow"])
    master_datetime = np.array(parameters["outflow"]["data"]["datetime"])
    temperature = [np.nan] * len(master_datetime)
    water_level_flow = get_water_level_change_flow_equivalent(parameters["waterlevel"]["data"], parameters["altitude"],
                                                              parameters["bathymetry"])
    extra_flow = outflow + water_level_flow
    periods = create_periods(dt, rebalance_period, len(master_datetime))

    for inflow in parameters["inflows"]:
        if "folder" in inflow:
            extra_flow = extra_flow - np.array(inflow["data"]["flow"])
    for inflow in parameters["inflows"]:
        if "folder" not in inflow:
            flow = inflow["contribution"] * extra_flow
            flow[np.isnan(flow)] = 0
            neg_flow = np.copy(flow)
            neg_flow[neg_flow > min_flow] = 0

            out_flow = np.array([])
            for p in periods:
                temp_flow = flow[p[0]: p[1]]
                added_vol = np.abs(np.sum(neg_flow[p[0]: p[1]] * dt))
                temp_flow[temp_flow <= min_flow] = min_flow
                total_vol = np.sum(temp_flow * dt)
                temp_flow = temp_flow * (1 - added_vol / total_vol)
                out_flow = np.concatenate((out_flow, temp_flow))

            inflow["data"] = pd.DataFrame(zip(master_datetime, out_flow, temperature),
                                          columns=["datetime", "flow", "temperature"])
    return parameters


def toffolon_river_water_temperature(Ts, Ta, Q, time, a, ty=366):
    """Calculates river water temperature from air temperature and flow rate.
       Ref: Marco Toffolon and Sebastiano Piccolroaz 2015 Environ. Res. Lett. 10 114011

            Parameters
            ----------
            Ts : float
                Initial water temperature of the river
            Ta : array, float
                Array of air temperature floats (degC)
            Q : array, float
                Array of flow rate floats (m3/s)
            time : array, datetime
                Array of datetimes
            a : array, float
                Array of floats (len 8) that defines the calibration parameters
            """
    a = np.array(a) / (3600 * 24)
    a1, a2, a3, a4, a5, a6, a7, a8 = a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]
    Tw = np.array([Ts] * len(Ta))
    dt = (time[1] - time[0]).total_seconds()
    for i in range(1, len(Ta)):
        theta = np.abs(Q[i] / np.nanmean(Q))
        delta = theta ** a4
        t = (time[i] - datetime(time[i].year, 1, 1)).total_seconds() / (3600 * 24)
        ty = (datetime(time[i].year + 1, 1, 1) - datetime(time[i].year, 1, 1)).total_seconds() / (3600 * 24)
        dTw = 1 / delta * (a1 + a2 * Ta[i - 1] - a3 * Tw[i - 1] + theta * (
                a5 + a6 * math.cos(2 * math.pi * (t / ty - a7)) - a8 * Tw[i - 1])) * dt
        Tw[i] = Tw[i] + dTw
    return Tw


def estimate_flow_temperature(parameters, files, start_date, temperature="T_2M"):
    coordinates = list(map(lambda x: x["coordinates"], parameters["inflows"]))
    air_temperature = cosmo_point_timeseries(coordinates, temperature, files)

    for i in range(len(parameters["inflows"])):
        if parameters["inflows"][i]["data"]["temperature"].isnull().values.any():
            df = parameters["inflows"][i]["data"].merge(air_temperature[i], on="datetime", how="left")
            df = df.interpolate(method="linear")
            start_temperature = parameters["inflows"][i]["temperature"][start_date.month - 1]
            df["temperature"] = toffolon_river_water_temperature(start_temperature, df[temperature],
                                                                 parameters["inflows"][i]["data"]["flow"],
                                                                 parameters["inflows"][i]["data"]["datetime"],
                                                                 parameters["inflows"][i]["a"])
            parameters["inflows"][i]["data"] = df
    return parameters



