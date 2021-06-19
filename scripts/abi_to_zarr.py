import argparse
parse_desc = """Convert gridded 1 min GLM imagery produced by
glmtools to a zarr store, rechunking to smaller tiles across a longer time
interval.

Requires glmtools and dask
"""

def create_parser():
    parser = argparse.ArgumentParser(description=parse_desc)
    parser.add_argument(dest='filenames',metavar='filename', nargs='*')
    parser.add_argument('-o', '--output_path',
                        metavar='filename for the output zarr store',
                        required=True, dest='zarr_out', action='store')
    parser.add_argument('-m', '--temp_zarr_path',
                        metavar='filename template including path',
                        required=False, dest='zarr_temp', action='store',
                        default='./_glm_store_temp.zarr')
    parser.add_argument('-x', '--x_chunks',
                        metavar='number of x gridpoints per chunk',
                        required=False, dest='x_chunks', action='store',
                        default=678, type=int)
    parser.add_argument('-y', '--y_chunks',
                        metavar='number of y gridpoints per chunk',
                        required=False, dest='y_chunks', action='store',
                        default=678, type=int)
    parser.add_argument('-t', '--t_chunks',
                        metavar='number of time intervals per chunk',
                        required=False, dest='t_chunks', action='store',
                        default=2, type=int)
    parser.add_argument('--dask_workers',
                        metavar='number of dask workers',
                        required=False, dest='dask_workers', action='store',
                        default=1, type=int)
    parser.add_argument('--dask_threads',
                        metavar='number of threads per dask worker',
                        required=False, dest='dask_threads', action='store',
                        default=8, type=int)

    return parser

import os
from glm_satpy_overlays import open_abi_time_series

if __name__ == '__main__':

    parser = create_parser()
    args = parser.parse_args()

    from dask.distributed import Client
    dask_client=Client(n_workers=args.dask_workers, threads_per_worker=args.dask_threads)
    print(dask_client)
    # from chunks import rechunker_wrapper
    import numpy as np


    rechunk_spec = {'x':args.x_chunks, 'y':args.y_chunks, 'time':args.t_chunks}


    filenames = args.filenames
    zarr_store = args.zarr_out
    temp_zarr_store = args.zarr_temp
    filenames.sort()
    n_files = len(filenames)

    n_time_chunks = int(np.ceil(n_files/rechunk_spec['time']))
    for i in range(n_time_chunks):
        these_files = slice(i*rechunk_spec['time'],
                           int(np.min([(i+1)*rechunk_spec['time'], n_files])))


        print("Reading GLM for time chunk {0}/{1}".format(i+1,n_time_chunks))
        ltg_ds_nc =  open_abi_time_series(filenames[these_files]
                        ).sortby('time').chunk(chunks=rechunk_spec)
        n_times = ltg_ds_nc.dims['time']
        print("Converting {0} GLM times".format(n_times))


# 64x64 spatial tiles are 165 Mpixels per variable
#     In [85]: 64*64*(24*60)*30/(1024**3)
#     Out[85]: 0.164794921875
        print(ltg_ds_nc.time)
        if os.path.exists(zarr_store):
            ltg_ds_nc.to_zarr(zarr_store, consolidated=True, append_dim='time')
        else:
            ltg_ds_nc.to_zarr(zarr_store, consolidated=True, mode='w')
        # Cannot run this with Distributed client - get error
        # distributed.utils_perf - WARNING - full garbage collections took 10% CPU time recently
#             rechunker_wrapper(ltg_ds_nc, zarr_store, temp_zarr_store, chunks=rechunk_spec,  consolidated=True)
# Replaced by above
#         ltg_ds_nc.chunk(rechunk_spec).to_zarr(zarr_store)
#         ltg_ds_nc.close()

