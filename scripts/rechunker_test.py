## Script version of rechunker_test.ipynb
## Script for using the rechunking tool to rechunk the chess-scape dataset
## shown here is an example for a single variable of a single ensmember of
## the CHESS-SCAPE dataset
## MJB 18/11/2021

import os
import zarr
import xarray as xr
import subprocess as subp
from rechunker import rechunk
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster

# create the dask cluster that will do the calculation
# these settings should work well for the high memory sci-analysis machines, sci3,6,8
cluster = LocalCluster(
    dashboard_address=":8788", threads_per_worker=1, n_workers=10, memory_limit="21GiB"
)
client = Client(cluster)

# maxmem is the maximum RAM you want each dask worker to consume. Must be lower than
# 'memory_limit' set in the dask creation command above
maxmem = "20GB"

# temporary file store that rechunker will use to do it's work
# this must be on the parallel scratch file space
# and must be deleted manually after each conversion
tempstore = "/work/scratch-pw/mattjbr/chess_scape/tempstore.zarr"
if not os.path.exists(os.path.dirname(tempstore)):
    os.makedirs(os.path.dirname(tempstore))

# ensemble member and variable name of the source dataset
ensmem = "01"
varname = "tmean"

# source and target datasets
# these should also be copied to/from the parallel scratch filespace
# you'll find the data in the folders in the same folder as this script
source_group = zarr.open(
    "/work/scratch-pw/mattjbr/chess_scape/10year100kmchunk/"
    + varname
    + "_"
    + ensmem
    + "_year.zarr"
)
targetstore = (
    "/work/scratch-pw/mattjbr/chess_scape/100year10kmchunk/"
    + varname
    + "_"
    + ensmem
    + "_100year10km.zarr"
)

if not os.path.exists(os.path.dirname(targetstore)):
    os.makedirs(os.path.dirname(targetstore))

# somewhere along the line I gave the files different names than the names of the variables
# actually in the files, so this block accounts for that
if varname == "tmean":
    zarrvname = "tas"
elif varname == "tmax":
    zarrvname = "tasmax"
elif varname == "tmin":
    zarrvname = "tasmin"
else:
    zarrvname = varname

# target-chunk dimensions
target_chunks = {
    zarrvname: {"time": 36000, "y": 10, "x": 10},
    "lat": {"y": 10, "x": 10},
    "lon": {"y": 10, "x": 10},
    "y": {"y": 10},
    "x": {"x": 10},
    "time": {"time": 36000},
}

# remove the temporary files if not done already  from previous conversions
subp.call(["rm " + tempstore], shell=True)
# create the rechunking plan
array_plan = rechunk(
    source_group, target_chunks, maxmem, targetstore, temp_store=tempstore
)
# execute the plan, i.e. do the calculation!
array_plan.execute()
