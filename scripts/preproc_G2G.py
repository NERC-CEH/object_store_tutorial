import xarray as xr
import os

filedir = "/home/users/mattjbr/object_storage/data/G2G"
outdir = "/home/users/mattjbr/object_storage/data/G2G/preproc"
RCMs = ["11", "12", "13", "15"]
files = [
    "G2G_DailyRiverFlow_NATURAL_RCM" + RCM + "_19801201_20801130." + "nc"
    for RCM in RCMs
]
infilepaths = [os.path.join(filedir, filen) for filen in files]
outfilepaths = [os.path.join(outdir, filen) for filen in files]

if not os.path.exists(outdir):
    os.makedirs(outdir)

for path in range(0, len(infilepaths)):
    infilepath = infilepaths[path]
    outfilepath = outfilepaths[path]
    print("Processing " + infilepath)

    infile = xr.open_dataset(infilepath)
    outfile = infile.expand_dims(dim={"RCM": [RCMs[path]]})

    outfile.to_netcdf(outfilepath)
