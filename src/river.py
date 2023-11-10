# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from datetime import datetime, timedelta

from functions import logger, download_data


def empty_arrays(parameters, start, end, no_data=0.0):
    if "dt" not in parameters:
        raise ValueError("dt in minutes must be defined in properties.json")
    if "origin" not in parameters:
        raise ValueError("origin in format YYYYMMDD must be defined in properties.json")
    origin = datetime.strptime(parameters["origin"], "%Y%m%d")
    time = np.arange(start, end, timedelta(minutes=parameters["dt"]))
    for river in parameters["rivers"]:
        df = pd.DataFrame(time, columns=['time'])
        df["minutes"] = (df["time"] - origin).astype('timedelta64[m]')
        df["flow"] = no_data
        if "average_monthly_temperature" in river:
            df["temperature"] = monthly_temperature(time, river["average_monthly_temperature"])
        elif "temperature" in river:
            df["temperature"] = river["temperature"]
        else:
            df["temperature"] = no_data
        if river["type"] == "inflow":
            df["velocity"] = river["velocity"]
            df["direction"] = river["direction"]
        river["data"] = df
    return parameters


def monthly_temperature(time, average_temperature):
    arr = np.array(pd.DatetimeIndex(time).month) - 1
    return np.array([average_temperature[i] for i in arr])


def download_bafu_hydrodata_measured(api, station_id, parameter, start_date, end_date, log):
    # /bafu/hydrodata/measured/{station_id}/{parameter}/{start_date}/{end_date}
    query = "{}/bafu/hydrodata/measured/{}/{}/{}/{}".format(api, station_id, parameter, start_date, end_date)
    data = download_data(query)
    if data:
        return data
    else:
        log.warning("Unable to download data.", indent=2)
        return False


def download_bafu_hydrodata(parameters, start, end, api, today, log=logger):
    forecast = False
    if end.strftime("%Y%m%d") == today.strftime("%Y%m%d") or end > today:
        forecast = True
        end = today - timedelta(days=1)

    for station in parameters["stations"]:
        for parameter_type in ["flow", "temperature", "level"]:
            if parameter_type in station and station[parameter_type]["download"]:
                log.info("Downloading {} from {} ({}) from {} to {}"
                         .format(station[parameter_type]["parameter"],
                                 station["name"],
                                 station["id"],
                                 start.strftime("%Y%m%d"),
                                 end.strftime("%Y%m%d")),
                         indent=2)
                data = download_bafu_hydrodata_measured(api,
                                                        station["id"],
                                                        station[parameter_type]["parameter"],
                                                        start.strftime("%Y%m%d"),
                                                        end.strftime("%Y%m%d"), log)
                if data:
                    station[parameter_type]["data"] = data

                if forecast and "forecast" in station[parameter_type]:
                    log.warning("Forecast collection not yet implemented, data will be extrapolated.", indent=2)

    return parameters


def clean_smooth_resample(parameters, start_date, end_date, log=logger, plot=False):
    if "dt" not in parameters:
        raise ValueError("dt in minutes must be defined in properties.json")
    time = np.arange(start_date, end_date, timedelta(minutes=parameters["dt"]))
    for station in parameters["stations"]:
        for parameter_type in ["flow", "temperature", "level"]:
            if parameter_type in station and "data" in station[parameter_type]:
                log.info("Processing {} from {} ({})"
                         .format(station[parameter_type]["parameter"],
                                 station["name"],
                                 station["id"], ),
                         indent=2)
                # Clean
                df = pd.DataFrame(station[parameter_type]["data"])
                df.columns = ["ds", "y"]
                df["ds"] = pd.to_datetime(df["ds"], errors="coerce", utc=True)
                df["y"] = pd.to_numeric(df["y"], errors="coerce")
                df = df.dropna()
                df['y'].where(df['y'] < station[parameter_type]["max"], station[parameter_type]["max"],
                              inplace=True)
                df['y'].where(df['y'] > station[parameter_type]["min"], station[parameter_type]["min"],
                              inplace=True)

                # Smooth
                if "downsample" in station[parameter_type]:
                    df = df.set_index("ds").resample(station[parameter_type]["downsample"]).mean()
                    df = df.reset_index()
                    f = interp1d(df["ds"].astype(int) / 10 ** 9, df["y"], kind="linear", bounds_error=False,
                                 fill_value=np.nan)
                    df = pd.DataFrame({"ds": time, "y": f(time.astype('int') / 10 ** 6)})
                if "smooth" in station[parameter_type]:
                    df["y"] = df["y"].rolling(window=station[parameter_type]["smooth"]["window"],
                                              win_type='gaussian',
                                              min_periods=station[parameter_type]["smooth"]["min"],
                                              center=True).mean(
                        std=station[parameter_type]["smooth"]["std"])
                    df = df.dropna()

                # Resample
                resample = "linear"
                if "resample" in station[parameter_type]:
                    resample = station[parameter_type]["resample"]
                f = interp1d(df["ds"].astype(int) / 10 ** 9, df["y"], kind=resample, bounds_error=False,
                             fill_value=np.nan)
                df = pd.DataFrame({"ds": time, "y": f(time.astype('int') / 10 ** 6)})

                df['y'].notnull().where(df['y'].notnull() < station[parameter_type]["max"],
                                        station[parameter_type]["max"],
                                        inplace=True)
                df['y'].notnull().where(df['y'].notnull() > station[parameter_type]["min"],
                                        station[parameter_type]["min"],
                                        inplace=True)

                if plot:
                    plt.plot(df["ds"], df["y"])
                    plt.title(station["name"] + " " + station[parameter_type]["parameter"])
                    plt.show()
                station[parameter_type]["data"] = df
    return parameters


def forecast(parameters, log=logger, plot=False):
    for station in parameters["stations"]:
        for parameter_type in ["flow", "temperature", "level"]:
            if parameter_type in station and "data" in station[parameter_type]:
                log.info("Processing {} from {} ({})"
                         .format(station[parameter_type]["parameter"],
                                 station["name"],
                                 station["id"], ),
                         indent=2)

                # Forecast
                if parameter_type == "flow":
                    full = forecast_flow(station[parameter_type]["data"], station[parameter_type]["forecast_method"])
                elif parameter_type == "temperature":
                    full = forecast_temperature(station[parameter_type]["data"],
                                                station[parameter_type]["forecast_method"])
                elif parameter_type == "level":
                    full = forecast_level(station[parameter_type]["data"], station[parameter_type]["forecast_method"])

                station[parameter_type]["data"] = full
                if plot:
                    plt.plot(full["ds"], full["y"])
                    plt.show()
    return parameters


def forecast_flow(df, method):
    if method == "fixed":
        return forecast_fixed(df)
    else:
        raise ValueError("Method {} not implemented.".format(method))


def forecast_temperature(df, method):
    if method == "fixed":
        return forecast_fixed(df)
    else:
        raise ValueError("Method {} not implemented.".format(method))


def forecast_level(df, method):
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


def flow_balance(parameters, folder, log=logger, plot=False):
    if parameters["river_balance_method"] == "outflow_from_total_inflow":
        parameters = outflow_from_total_inflow(parameters, folder, log, plot)
    else:
        raise ValueError("Unrecognised flow balance method: {}".format(parameters["river_balance_method"]))
    return parameters


def outflow_from_total_inflow(parameters, folder, log, plot):
    log.info("Calculate outflow based on total inflows and set emtpy inflows to 0.", indent=2)
    time = parameters["rivers"][0]["data"]["time"]
    outflow = np.zeros(len(time))
    for river in parameters["rivers"]:
        if len(river["stations"]) == 0:
            continue
        flow = np.zeros(len(time))
        temperature = []
        for station in river["stations"]:
            station_data = next(item for item in parameters["stations"] if item["id"] == station["id"])
            if "flow" in station_data and "data" in station_data["flow"]:
                df = pd.DataFrame(time.copy())
                df.columns = ["ds"]
                df = df.merge(station_data["flow"]["data"], on='ds', how='left')
                f = np.array(df["y"])
                flow[~np.isnan(f)] = flow[~np.isnan(f)] + (f[~np.isnan(f)] * station["factor"])
            if "temperature" in station_data and "data" in station_data["temperature"]:
                df = pd.DataFrame(time.copy())
                df.columns = ["ds"]
                df = df.merge(station_data["temperature"]["data"], on='ds', how='left')
                temperature.append(np.array(df["y"]))
        temperature = np.array(temperature)
        out_temperature = np.array(river["data"]["temperature"])
        if len(temperature) != 0:
            temperature = np.nanmean(temperature, axis=0)
            out_temperature[~np.isnan(temperature)] = temperature[~np.isnan(temperature)]
        river["data"]["temperature"] = out_temperature
        river["data"]["flow"] = flow
        outflow = outflow + flow
    for river in parameters["rivers"]:
        if river["type"] == "outflow":
            river["data"]["flow"] = outflow

    log.info("Generating plot of river inputs and outputs.", indent=2)
    fig, (ax1, ax2) = plt.subplots(1, 2)
    fig.suptitle('Model river inputs.')
    ax1.title.set_text('Flow (m3/s)')
    ax2.title.set_text('Temperature (°C)')
    for river in parameters["rivers"]:
        ax1.plot(river["data"]["time"], river["data"]["flow"], label="{} ({})".format(river["name"], river["type"]))
        ax2.plot(river["data"]["time"], river["data"]["temperature"], label="{} ({})".format(river["name"], river["type"]))
    plt.legend()
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(os.path.join(folder, "river_inputs.png"))
    if plot:
        plt.show()
    return parameters


def write_river_data_to_file(parameters, folder, filename="RiversOperationsQuantities.dis"):
    if "origin" not in parameters:
        raise ValueError("origin in format YYYYMMDD must be defined in properties.json")
    origin = datetime.strptime(parameters["origin"], "%Y%m%d")
    f = open(os.path.join(folder, filename), "w")
    i = 1
    for river in parameters["rivers"]:
        if river["type"] == "inflow":
            f.write("table-name           'Discharge : {}'\n".format(str(i)))
            f.write("contents             'momentum'\n")
            f.write("location             '{}'\n".format(river["name"]))
            f.write("time-function        'non-equidistant'\n")
            f.write("reference-time       {}\n".format(origin.strftime("%Y%m%d")))
            f.write("time-unit            'minutes'\n")
            f.write("interpolation        'linear'\n")
            f.write("parameter            'time                '                     unit '[min]'\n")
            f.write("parameter            'flux/discharge rate '                     unit '[m3/s]'\n")
            f.write("parameter            'Temperature         '                     unit '[°C]'\n")
            f.write("parameter            'flow magnitude      '                     unit '[m/s]'\n")
            f.write("parameter            'flow direction      '                     unit '[deg]'\n")
            f.write("records-in-table     {}\n ".format(str(len(river["data"]))))

            df = river["data"]
            out = np.column_stack((df["minutes"], df["flow"], df["temperature"], df["velocity"], df["direction"]))
            np.savetxt(f, out, fmt="%.7e", delimiter="   ", newline="\n ")
        else:
            f.write("table-name           'Discharge : {}'\n".format(str(i)))
            f.write("contents             'regular  '\n")
            f.write("location             '{}           '\n".format(river["name"]))
            f.write("time-function        'non-equidistant'\n")
            f.write("reference-time       {}\n".format(origin.strftime("%Y%m%d")))
            f.write("time-unit            'minutes'\n")
            f.write("interpolation        'linear'\n")
            f.write("parameter            'time                '                     unit '[min]'\n")
            f.write("parameter            'flux/discharge rate '                     unit '[m3/s]'\n")
            f.write("parameter            'Temperature         '                     unit '[°C]'\n")
            f.write("records-in-table     {}\n ".format(str(len(river["data"]))))

            df = river["data"]
            out = np.column_stack((df["minutes"], -df["flow"], df["temperature"]))
            np.savetxt(f, out, fmt="%.7e", delimiter="   ", newline="\n")
        i = i + 1
    f.close()
