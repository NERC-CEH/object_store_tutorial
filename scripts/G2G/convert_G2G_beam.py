# MJB (UKCEH) Nov-2023
# Example script for a pangeo-forge-recipe to convert
# gridded netcdf files to a zarr datastore ready for upload
# to object storage.
# See jupyter notebook for more details and explanations/comments
# Please note that this notebook is intended to serve as an example only,
# and be adapted for your own datasets.

import os
import apache_beam as beam
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from apache_beam.options.pipeline_options import PipelineOptions
from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr


RCMs = ["01", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "15"]
indir = "/home/users/mattjbr/object_storage/data/G2G/preproc"
pre = "G2G_DailyRiverFlow_NATURAL_RCM"
suf = "_19801201_20801130.nc"
td = "/work/scratch-pw2/mattjbr"
tn = "fulloutput_yearly_100km_chunks.zarr"
target_chunks = {"RCM": 1, "Time": 360, "Northing": 100, "Easting": 100}
nprocs = 64

if not os.path.exists(td):
    os.makedirs(td)


def make_path(RCM):
    return os.path.join(indir, pre + RCM + suf)


RCM_concat_dim = ConcatDim("RCM", RCMs, nitems_per_file=1)

pattern = FilePattern(make_path, RCM_concat_dim)

transforms = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        target_root=td,
        store_name=tn,
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
    )
)

beam_options = PipelineOptions(
    direct_num_workers=nprocs, direct_running_mode="multi_processing"
)
with beam.Pipeline(options=beam_options) as p:
    p | transforms
