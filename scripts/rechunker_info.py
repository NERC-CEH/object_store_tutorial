from rechunker import algorithm
import numpy as np
import sys
import math
from math import prod, lcm
from math import ceil, floor
from typing import List, Optional, Sequence, Tuple


def calculate_single_stage_io_ops(shape, in_chunks, out_chunks):
    """Estimate the number of irregular chunks required for rechunking."""
    return prod(map(count_num_splits, in_chunks, out_chunks, shape))


def count_num_splits(source_chunk, target_chunk, size):
    multiple = lcm(source_chunk, target_chunk)
    splits_per_lcm = multiple // source_chunk + multiple // target_chunk - 1
    lcm_count, remainder = divmod(size, multiple)
    if remainder:
        splits_in_remainder = (
            ceil(remainder / source_chunk) + ceil(remainder / target_chunk) - 1
        )
    else:
        splits_in_remainder = 0
    return lcm_count * splits_per_lcm + splits_in_remainder


def calculate_shared_chunks(read_chunks, write_chunks):
    # Intermediate chunks are the smallest possible chunks which fit
    # into both read_chunks and write_chunks.
    # Example:
    #   read_chunks:            (20, 5)
    #   target_chunks:          (4, 25)
    #   intermediate_chunks:    (4, 5)
    # We don't need to check their memory usage: they are guaranteed to be smaller
    # than both read and write chunks.
    return tuple(
        min(c_read, c_target) for c_read, c_target in zip(read_chunks, write_chunks)
    )


def evaluate_stage_v2(shape, read_chunks, int_chunks, write_chunks):
    tasks = calculate_single_stage_io_ops(shape, read_chunks, write_chunks)
    read_tasks = tasks if write_chunks != read_chunks else 0
    write_tasks = tasks if read_chunks != int_chunks else 0
    return read_tasks, write_tasks


def evaluate_plan(stages, shape, itemsize):
    total_reads = 0
    total_writes = 0
    for i, stage in enumerate(stages):
        read_chunks, int_chunks, write_chunks = stage
        read_tasks, write_tasks = evaluate_stage_v2(
            shape,
            read_chunks,
            int_chunks,
            write_chunks,
        )
        total_reads += read_tasks
        total_writes += write_tasks
    return total_reads, total_writes


def print_summary(stages, shape, itemsize):
    for i, stage in enumerate(stages):
        print(f"stage={i}: " + " -> ".join(map(str, stage)))
        read_chunks, int_chunks, write_chunks = stage
        read_tasks, write_tasks = evaluate_stage_v2(
            shape,
            read_chunks,
            int_chunks,
            write_chunks,
        )
        print(f"  Tasks: {read_tasks} reads, {write_tasks} writes")
        print(f"  Split chunks: {itemsize*np.prod(int_chunks)/1e6 :1.3f} MB")

    total_reads, total_writes = evaluate_plan(stages, shape, itemsize)
    print("Overall:")
    print(f"  Reads count: {total_reads:1.3e}")
    print(f"  Write count: {total_writes:1.3e}")


# dask.array.rechunk is the function
rechunk_module = sys.modules["dask.array.rechunk"]


def dask_plan(shape, source_chunks, target_chunks, threshold=None):
    source_expanded = rechunk_module.normalize_chunks(source_chunks, shape)
    target_expanded = rechunk_module.normalize_chunks(target_chunks, shape)
    # Note: itemsize seems to be ignored, by default
    stages = rechunk_module.plan_rechunk(
        source_expanded,
        target_expanded,
        threshold=threshold,
        itemsize=4,
    )
    write_chunks = [tuple(s[0] for s in stage) for stage in stages]
    read_chunks = [source_chunks] + write_chunks[:-1]
    int_chunks = [
        calculate_shared_chunks(r, w) for r, w in zip(write_chunks, read_chunks)
    ]
    return list(zip(read_chunks, int_chunks, write_chunks))


def rechunker_plan(shape, source_chunks, target_chunks, **kwargs):
    stages = algorithm.rechunking_plan(shape, source_chunks, target_chunks, **kwargs)
    return [(source_chunks, stages[1], target_chunks)]


################################################################
# EDIT THESE
itemsize = 4  # how many bytes is each number/data point in the source arrays. float32=4, float64=8
shape = [36000, 1057, 656]
source_chunks = [360, 1057, 656]
target_chunks = [36000, 10, 10]
maxmem = 20e9  # max memory of a single dask worker, corresponding to the max size in mem a chunk can be. E6=MB, E9=GB, E12=TB
################################################################

print(f"Total size: {itemsize*np.prod(shape)/1e9:.3} GB")
print(f"Source chunk count: {np.prod(shape)/np.prod(source_chunks):1.3e}")
print(f"Target chunk count: {np.prod(shape)/np.prod(target_chunks):1.3e}")

print()
print("Rechunker plan (max_mem=" + str(int(maxmem / 1e9)) + "GB):")
plan = rechunker_plan(
    shape, source_chunks, target_chunks, itemsize=4, max_mem=int(maxmem)
)
print_summary(plan, shape, itemsize=4)

print()
print("Dask plan (default):")
plan = dask_plan(shape, source_chunks, target_chunks)
print_summary(plan, shape, itemsize=4)
