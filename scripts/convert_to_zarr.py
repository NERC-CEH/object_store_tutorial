# Script adapted from Iain Walmsley #
# Converts a multi-file netCDF dataset to a chunked zarr file #

# NOTE: a limitiation with xarray means it will
# never create chunks greater in size than a single
# file. E.g. a dataset that has individual files for each month
# will not be chunked to anything greater than a month for the
# time dimension, regardless of what you specify.

import datetime
import xarray
import zarr
import pandas as pd
import dask
import glob
import sys
import os
from pathlib import Path
from dask_jobqueue import SLURMCluster
from dask.distributed import Client, LocalCluster

# Either run this from the command line and put the ensmem and varname
# as the arguments, or just specify them here and run the script without
# any arguments
# Edit these variables AND the chunksize near the end of the file
ensmem = sys.argv[1]
varname = sys.argv[2]
INPUT_DIRECTORY = (
    "/gws/nopw/j04/hydro_jules/data/uk/driving_data/ukcp18_1km/rcp85_bias_corrected/"
    + ensmem
    + "/daily/"
    + varname
)
OUTPUT_DIRECTORY = "/gws/nopw/j04/ceh_generic/matbro/chess_scape/10kmmonthchunk"
FILE_PATH = os.path.join(OUTPUT_DIRECTORY, ensmem + varname + ".zarr")

# Number of Dask Workers
# It's important not to create too many workers as the conversion consists
# of some activities that hold the GIL, hence when benchmarking with > 70
# workers tasks would time out.
# Two examples for WRF that have worked well have been
#  ~ 7TB - 100 workers (>150 failed)
#  ~ 1TB - 45 workers (>70 failed)
# MJ: TBH I'm not entirely sure what Iain means by all this...
WORKERS = 30

if not os.path.exists(OUTPUT_DIRECTORY):
    os.makedirs(OUTPUT_DIRECTORY)

# Create glob of all NetCDF files to be converted
filename = glob.glob(os.path.join(INPUT_DIRECTORY, "*.nc"))


def log(msg):
    now = datetime.datetime.now()
    print("%s: %s" % (now.strftime("%d-%m/%H:%M:%S"), msg))


# Configure the Dask workers that will be created, note that on LOTUS
# when using the short-serial queue only 1 core can be allocated per
# job, however memory can be altered
lotus = SLURMCluster(
    cores=1,
    memory="64GB",
    queue="long-serial",
    walltime="72:00:00",
    local_directory="/work/scratch-pw/mattjbr/chess_scape_" + ensmem + "_" + varname,
    silence_logs="debug",
)

lotus.scale(WORKERS)
client = Client(lotus)
client.wait_for_workers(WORKERS - 4)

log("Opening multi-file dataset")
ds = xarray.open_mfdataset(filename, combine="nested", parallel=True, concat_dim="time")

log("Compiled dataframe from xarray")
# EDIT THIS BEFORE RUNNING! THIS CHUNK SIZE IS WAAAAAAY TOO SMALL AND THINGS BREAK
ds_chunked = ds.chunk({"time": 30, "x": 10, "y": 10})

log("Outputting to zarr: %s" % FILE_PATH)
ds_chunked.to_zarr(FILE_PATH, mode="w")
