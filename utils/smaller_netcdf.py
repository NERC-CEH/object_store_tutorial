import xarray as xr
import sys

def extract_first_percent_time(file_path, output_path, size):
    # Open the NetCDF file
    ds = xr.open_dataset(file_path)

    # Check if 'time' dimension exists
    if 'time' not in ds.dims:
        print("Error: No 'time' dimension found in the NetCDF file.")
        ds.close()
        return

    # Get the total number of time steps
    total_time_steps = ds.dims['time']

    # Calculate the number of time steps for size
    slice_size = max(1, int(total_time_steps * size))

    # Slice the dataset along the time dimension
    ds_slice = ds.isel(time=slice(0, slice_size))

    # Save the sliced dataset
    ds_slice.to_netcdf(output_path)

    print(f"Extracted first {size} ({slice_size} out of {total_time_steps} time steps)")
    print(f"Original time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    print(f"New time range: {ds_slice.time.values[0]} to {ds_slice.time.values[-1]}")

    # Close the datasets
    ds.close()
    ds_slice.close()

if len(sys.argv) != 4:
   print("Usage: python utils/smaller_netcdf.py input_file output_file 0.01 # 1 percent of original size")
   sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]
size = float(sys.argv[3])

extract_first_percent_time(input_file, output_file, size)
