# MJB (UKCEH) Aug-2024
# Example script for a pangeo-forge-recipe to convert
# gridded netcdf files to a zarr datastore ready for upload
# to object storage.
# See jupyter notebook for more details and explanations/comments
# Please note that this script/notebook is intended to serve as an example only,
# and be adapted for your own datasets.

import os
import apache_beam as beam
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from apache_beam.options.pipeline_options import PipelineOptions
from pangeo_forge_recipes.transforms import (
    OpenWithXarray,
    StoreToZarr,
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
)

startyear = 1990
endyear = 2016 # inclusive
indir = "/home/users/mattjbr/fdri/data/gear-hrly"
pre = "CEH-GEAR-1hr-v2_"
suf = ".nc"
td = "/work/scratch-pw2/mattjbr"
tn = "gear_1hrly_fulloutput_yearly_100km_chunks.zarr"
target_chunks = {"time": int(365.25*24), "y": 100, "x": 100}
#nprocs = 64
prune = 12 # no. of files to process, set to 0 to use all

if not os.path.exists(td):
    os.makedirs(td)

def make_path(time):
    filename = pre + time + suf
    return os.path.join(indir, filename)

years = list(range(startyear, endyear + 1))
months = list(range(1, 13))
ymonths = [f"{year}{month:02d}" for month in months for year in years]
time_concat_dim = ConcatDim("time", ymonths)

pattern = FilePattern(make_path, time_concat_dim)
if prune > 0:
    pattern = pattern.prune(nkeep=prune)

recipe = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        target_root=td,
        store_name=tn,
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
)

#beam_options = PipelineOptions(
#    direct_num_workers=nprocs, direct_running_mode="multi_processing"
#)
#with beam.Pipeline(options=beam_options) as p:
#    p | recipe

with beam.Pipeline() as p:
    p | recipe
