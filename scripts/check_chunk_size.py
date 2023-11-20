import dask.array as dsa

zarrpath = (
    "/gws/nopw/j04/ceh_generic/matbro/chess_scape/monthchunk/ens01-monthchunk/tas.zarr"
)
varname = "tas"

dsa.from_zarr(zarrpath + "/" + varname)
