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

ensmem = sys.argv[1]
varname = sys.argv[2]
INPUT_DIRECTORY = (
    "/gws/nopw/j04/hydro_jules/data/uk/driving_data/ukcp18_1km/rcp85_bias_corrected/"
    + ensmem
    + "/daily/"
    + varname
)
OUTPUT_DIRECTORY = "/gws/nopw/j04/ceh_generic/matbro/chess_scape/10km_month_chunk"
FILE_PATH = os.path.join(OUTPUT_DIRECTORY, ensmem + varname + ".zarr")

# Create glob of all NetCDF files to be converted
filename = glob.glob(os.path.join(INPUT_DIRECTORY, "*.nc"))

if not os.path.exists(OUTPUT_DIRECTORY):
    os.makedirs(OUTPUT_DIRECTORY)

# Number of Dask Workers
# It's important not to create too many workers as the conversion consists
# of some activities that hold the GIL, hence when benchmarking with > 70
# workers tasks would time out.
# Two examples for WRF that have worked well have been
#  ~ 7TB - 100 workers (>150 failed)
#  ~ 1TB - 45 workers (>70 failed)
WORKERS = 10


def log(msg):
    now = datetime.datetime.now()
    print("%s: %s" % (now.strftime("%d-%m/%H:%M:%S"), msg))


# Configure the Dask workers that will be created, note that on LOTUS
# when using the short-serial queue only 1 core can be allocated per
# job, however memory can be altered
lotus = SLURMCluster(
    cores=1,
    memory="12GB",
    queue="short-serial-4hr",
    walltime="4:00:00",
    local_directory="/work/scratch-pw/mattjbr/" + varname,
    silence_logs="debug",
)

lotus.scale(WORKERS)
client = Client(lotus)
client.wait_for_workers(WORKERS - 2)

log("Opening multi-file dataset")
ds = xarray.open_mfdataset(filename, combine="nested", parallel=True, concat_dim="time")

log("Compiled dataframe from xarray")
ds_chunked = ds.chunk({"time": 30})

log("Outputting to zarr: %s" % FILE_PATH)
ds_chunked.to_zarr(FILE_PATH, mode="w")
