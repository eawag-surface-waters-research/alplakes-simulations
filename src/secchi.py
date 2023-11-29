import pytz
import numpy as np
from datetime import datetime, timedelta


def write_fixed_secchi_to_file(file, value, scaling_factor, start, end, cols, rows, origin=datetime(2008, 3, 1, tzinfo=pytz.utc)):
    grid = np.ones((rows, cols))
    grid[:] = value * scaling_factor
    start_diff = (start.replace(tzinfo=pytz.UTC) - origin).total_seconds() / 3600
    end_diff = ((end.replace(tzinfo=pytz.UTC) - origin) + timedelta(days=1)).total_seconds() / 3600
    with open(file, "a") as f:
        time_str = "TIME = " + str(start_diff) + "0 hours since " + origin.strftime(
            "%Y-%m-%d %H:%M:%S") + " +00:00"
        f.write(time_str)
        f.write("\n")
        np.savetxt(f, grid, fmt='%.2f')

        time_str = "TIME = " + str(end_diff) + "0 hours since " + origin.strftime(
            "%Y-%m-%d %H:%M:%S") + " +00:00"
        f.write(time_str)
        f.write("\n")
        np.savetxt(f, grid, fmt='%.2f')


def write_monthly_secchi_to_file(file, values, scaling_factor, start, end, cols, rows, origin=datetime(2008, 3, 1, tzinfo=pytz.utc)):
    grid = np.ones((rows, cols))
    days = int((end - start).total_seconds() / (3600 * 24))
    start_diff = (start.replace(tzinfo=pytz.UTC) - origin).total_seconds() / 3600
    end_diff = ((end.replace(tzinfo=pytz.UTC) - origin) + timedelta(days=1)).total_seconds() / 3600

    with open(file, "a") as f:
        time_str = "TIME = " + str(start_diff) + "0 hours since " + origin.strftime(
            "%Y-%m-%d %H:%M:%S") + " +00:00"
        f.write(time_str)
        f.write("\n")
        grid[:] = values[start.month - 1] * scaling_factor
        np.savetxt(f, grid, fmt='%.2f')

        for i in range(1, days + 1):
            time_str = "TIME = " + str(start_diff + i * 24) + "0 hours since " + origin.strftime(
                "%Y-%m-%d %H:%M:%S") + " +00:00"
            f.write(time_str)
            f.write("\n")
            grid[:] = values[(start + timedelta(days=i)).month - 1] * scaling_factor
            np.savetxt(f, grid, fmt='%.2f')

        time_str = "TIME = " + str(end_diff) + "0 hours since " + origin.strftime(
            "%Y-%m-%d %H:%M:%S") + " +00:00"
        f.write(time_str)
        f.write("\n")
        grid[:] = values[end.month - 1] * scaling_factor
        np.savetxt(f, grid, fmt='%.2f')
