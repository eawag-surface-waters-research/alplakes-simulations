# -*- coding: utf-8 -*-
import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from datetime import datetime, timedelta
from weather import *
from functions import logger


def get_raw_river_data(parameters, start, end, log=logger):
    for inflow in parameters["inflows"]:
        if "folder" in inflow:
            inflow["files"] = []
            datetime_arr = []
            temperature_arr = []
            flow_arr = []
            ideal_files = []
            for day in np.arange(start_date, end_date, timedelta(days=1)).astype(datetime):
                ideal_files.append(inflow["prefix"] + day.strftime('%Y%m%d') + ".txt")
            for path, subdirs, files in os.walk(folder):
                for name in files:
                    if name in ideal_files:
                        inflow["files"].append(os.path.join(path, name))
                        df = pd.read_csv(os.path.join(path, name), sep='\t')
                        df["datetime"] = pd.to_datetime(df['Date'] + df["Time"], format=date_format)
                        datetime_arr = datetime_arr + list(df["datetime"])
                        if "temperature" in inflow:
                            temperature_arr = temperature_arr + list(df[inflow["temperature"]])
                        else:
                            temperature_arr = temperature_arr + [np.nan] * len(df["datetime"])
                        if "flow" in inflow:
                            flow_arr = flow_arr + list(df[inflow["flow"]])
                        else:
                            flow_arr = flow_arr + [np.nan] * len(df["datetime"])
            inflow["data"] = pd.DataFrame(zip(datetime_arr, flow_arr, temperature_arr), columns=["datetime", "flow", "temperature"])

    if "outflow" in parameters:
        parameters["outflow"]["files"] = []
        datetime_arr = []
        temperature_arr = []
        flow_arr = []
        ideal_files = []
        for day in np.arange(start_date, end_date, timedelta(days=1)).astype(datetime):
            ideal_files.append(parameters["outflow"]["prefix"] + day.strftime('%Y%m%d') + ".txt")
        for path, subdirs, files in os.walk(folder):
            for name in files:
                if name in ideal_files:
                    parameters["outflow"]["files"].append(os.path.join(path, name))
                    df = pd.read_csv(os.path.join(path, name), sep='\t')
                    df["datetime"] = pd.to_datetime(df['Date'] + df["Time"], format=date_format)
                    datetime_arr = datetime_arr + list(df["datetime"])
                    if "temperature" in parameters["outflow"]:
                        temperature_arr = temperature_arr + list(df[parameters["outflow"]["temperature"]])
                    else:
                        temperature_arr = temperature_arr + [np.nan] * len(df["datetime"])
                    if "flow" in parameters["outflow"]:
                        flow_arr = flow_arr + list(df[parameters["outflow"]["flow"]])
                    else:
                        flow_arr = flow_arr + [np.nan] * len(df["datetime"])
        parameters["outflow"]["data"] = pd.DataFrame(zip(datetime_arr, flow_arr, temperature_arr), columns=["datetime", "flow", "temperature"])

    if "waterlevel" in parameters:
        parameters["waterlevel"]["files"] = []
        datetime_arr = []
        level_arr = []
        ideal_files = []
        for day in np.arange(start_date, end_date, timedelta(days=1)).astype(datetime):
            ideal_files.append(parameters["waterlevel"]["prefix"] + day.strftime('%Y%m%d') + ".txt")
        for path, subdirs, files in os.walk(folder):
            for name in files:
                if name in ideal_files:
                    parameters["waterlevel"]["files"].append(os.path.join(path, name))
                    df = pd.read_csv(os.path.join(path, name), sep='\t')
                    df["datetime"] = pd.to_datetime(df['Date'] + df["Time"], format=date_format)
                    datetime_arr = datetime_arr + list(df["datetime"])
                    level_arr = level_arr + list(df[parameters["waterlevel"]["level"]])
        parameters["waterlevel"]["data"] = pd.DataFrame(zip(datetime_arr, level_arr), columns=["datetime", "level"])
    return parameters


def clean_raw_river_data(parameters, start_date, end_date, dt=timedelta(minutes=10), interpolate="linear"):
    master_datetime = np.arange(start_date, end_date, dt)
    df = pd.DataFrame(master_datetime, columns=["datetime"])
    for inflow in parameters["inflows"]:
        if "folder" in inflow:
            data = df.merge(inflow["data"], on='datetime', how='left')
            data['flow'].values[data['flow'].values < inflow["min_flow"]] = np.nan
            data = data.interpolate(method=interpolate)
            inflow["data"] = data

    if "outflow" in parameters:
        data = df.merge(parameters["outflow"]["data"], on='datetime', how='left')
        data['flow'].values[data['flow'].values < parameters["outflow"]["min_flow"]] = np.nan
        data = data.interpolate(method=interpolate)
        data["raw_flow"] = data["flow"]
        data["flow"] = data["flow"].rolling(window=12, win_type='gaussian', center=True).mean(std=2)
        data = data.interpolate(method=interpolate, limit_direction='backward')
        data = data.interpolate(method=interpolate)
        parameters["outflow"]["data"] = data

    if "waterlevel" in parameters:
        data = df.merge(parameters["waterlevel"]["data"], on='datetime', how='left')
        data = data.interpolate(method=interpolate)
        data["level"] = data["level"].rolling(window=144, win_type='gaussian', center=True).mean(std=32)
        data = data.interpolate(method=interpolate, limit_direction='backward')
        data = data.interpolate(method=interpolate)
        parameters["waterlevel"]["data"] = data
    return parameters


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

    dt = (parameters["outflow"]["data"]["datetime"].iloc[1] - parameters["outflow"]["data"]["datetime"].iloc[0]).total_seconds()

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
    steps = min(round(rebalance_period / dt), round(length/2))
    period = math.floor(length/steps)
    periods = []
    for p in range(period):
        periods.append([p * steps, p * steps + steps])
    periods[-1][1] = length
    return periods


def flow_balance_ungauged_rivers(parameters, min_flow=0.0001, dt=600, rebalance_period=604800):
    outflow = np.array(parameters["outflow"]["data"]["flow"])
    master_datetime = np.array(parameters["outflow"]["data"]["datetime"])
    temperature = [np.nan] * len(master_datetime)
    water_level_flow = get_water_level_change_flow_equivalent(parameters["waterlevel"]["data"], parameters["altitude"], parameters["bathymetry"])
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
                temp_flow = temp_flow * (1 - added_vol/total_vol)
                out_flow = np.concatenate((out_flow, temp_flow))

            inflow["data"] = pd.DataFrame(zip(master_datetime, out_flow, temperature), columns=["datetime", "flow", "temperature"])
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
        t = (time[i] - datetime(time[i].year, 1, 1)).total_seconds()/(3600*24)
        ty = (datetime(time[i].year + 1, 1, 1) - datetime(time[i].year, 1, 1)).total_seconds()/(3600*24)
        dTw = 1 / delta * (a1 + a2 * Ta[i - 1] - a3 * Tw[i - 1] + theta * (a5 + a6 * math.cos(2 * math.pi * (t / ty - a7)) - a8 * Tw[i - 1])) * dt
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


def write_river_data_to_file(parameters, folder, filename="RiversOperationsQuantities.dis", origin=datetime(2008, 3, 1)):
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
        df["minutes"] = (df["datetime"]-origin).astype('timedelta64[m]')
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
