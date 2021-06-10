import argparse
parse_desc = """Convert gridded 1 min GLM imagery produced by
glmtools to a zarr store, rechunking to smaller tiles across a longer time
interval.

Requires glmtools and dask
"""

def create_parser():
    parser = argparse.ArgumentParser(description=parse_desc)
    # parser.add_argument(dest='filenames',metavar='filename', nargs='*')
    parser.add_argument('-o', '--output_path',
                        metavar='filename for the output zarr store',
                        required=True, dest='zarr_out', action='store')
    parser.add_argument('-i', '--input_path',
                        metavar='zarr store with 1 min GLM data',
                        required=True, dest='zarr_in', action='store')
    parser.add_argument('-m', '--temp_zarr_path',
                        metavar='filename template including path',
                        required=False, dest='zarr_temp', action='store',
                        default='./_glm_window_store_temp.zarr')
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
                        default=60, type=int)
    parser.add_argument('--dask_workers',
                        metavar='number of dask workers',
                        required=False, dest='dask_workers', action='store',
                        default=1, type=int)
    parser.add_argument('--dask_threads',
                        metavar='number of threads per dask worker',
                        required=False, dest='dask_threads', action='store',
                        default=2, type=int)
    parser.add_argument('-a', '--agg_minutes',
                        metavar='number of minutes to aggregate',
                        required=False, dest='agg_minutes', action='store',
                        default=5, type=float)

    return parser


import os
from datetime import datetime, timedelta
from glob import glob
import xarray as xr
import pandas as pd
import numpy as np


from glmtools.io.imagery import aggregate

do_rolling = True

# def compress_all(glm_grids):
#     for var in glm_grids:
#         glm_grids[var].encoding["zlib"] = True
#         glm_grids[var].encoding["complevel"] = 4
#         glm_grids[var].encoding["contiguous"] = False
#     return glm_grids
#
#
# def simplify_one_time(thisglm):
#     # Patch up at least some metadata
#     start = thisglm.time - (agg_minutes_dt - one_minute_dt)
#     end = thisglm.time + one_minute_dt
#     now = datetime.now()
#
#     start = pd.to_datetime(start.data).to_pydatetime()
#     end = pd.to_datetime(end.data).to_pydatetime()
#
#     thisglm.attrs['time_coverage_start'] = start.isoformat()
#     thisglm.attrs['time_coverage_end'] = end.isoformat()
#     thisglm.attrs['date_created'] = now.isoformat()
#
#     dataset_name = "OR_GLM-L2-GLM{5}-{0}_{1}_s{2}_e{3}_c{4}.nc"
#
#     outname = dataset_name.format(
#         'M3', thisglm.platform_ID,
#         start.strftime('%Y%j%H%M%S0'),
#         end.strftime('%Y%j%H%M%S0'),
#         now.strftime('%Y%j%H%M%S0'),
#         'C')
#     # print(outname)
#     thisglm.attrs['dataset_name']=outname
#
#     thisglm = compress_all(thisglm)
#
#     return thisglm, outname
#
# def write_ncs(glmagg):
#
#     outnames = []
#     for ti in range(glmagg.dims['time']):
#         thisglm = glmagg[{'time':ti}]
#         glmout, outname = simplify_one_time(thisglm)
# #         glmout.to_netcdf(outname)
#         glmout.drop_dims(['dim_0']).to_zarr(outname+'.zarr')
# #         print(glmout)
#         outnames.append(outname)
#     return outnames


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()

    zarr_store = args.zarr_out
    temp_zarr_store = args.zarr_temp

    agg_minutes = args.agg_minutes
    rechunk_spec = {'x':args.x_chunks, 'y':args.y_chunks, 'time':args.t_chunks}

    from dask.distributed import Client
    dask_client=Client(n_workers=args.dask_workers, threads_per_worker=args.dask_threads)
    print(dask_client)

    agg_minutes_dt = pd.Timedelta(minutes=agg_minutes)
    one_minute_dt = pd.Timedelta(minutes=1)

    # if do_rolling:
    #     ltg_agg = ltg_agg.rename({'time':'grid_time'})
    # else:
    #     ltg_agg['time_bins'] = [v.left for v in ltg_agg.time_bins.values]
    #     ltg_agg = ltg_agg.rename({'time_bins':'grid_time'})
    # ltg_agg=ltg_agg.sortby('grid_time')


    ltg_ds = xr.open_zarr(args.zarr_in, chunks='auto')
    print("opened data")
    print(ltg_ds.chunks)

    # The rolling operation marks the time at the end of the rolling window.
    # The initial four minutes (for a five min aggregation) processed will be
    # missing data, so we want to drop those. For processing of long
    # time periods, it's necessary to add extra files at the beginning to
    # keep things consistent across a boundary.
    #0 0000-0001 1
    #1 0001-0002 2
    #2 0002-0003 3
    #3 0003-0004 4
    #4 0004-0005 5: will have this plus previous four when aggregating by 5
    # So index by slice(4, None) to guarantee no missing minutes.
    drop_times = {'time':slice(agg_minutes-1, None)}

    skip_agg = ['goes_imager_projection', 'nominal_satellite_subpoint_lat',
                'nominal_satellite_subpoint_lon', 'DQF']
    ltg_agg = aggregate(ltg_ds.drop(skip_agg),
                        agg_minutes, rolling=do_rolling).drop(
                        ['total_flash_area', 'total_group_area'])
    print("aggregated")
    # Rename aggregated variables to indicate they are windowed.
    orig_vars = ['flash_extent_density',
    'flash_centroid_density',
    'total_energy',
    'event_flash_fraction',
    'group_extent_density',
    'group_centroid_density',
    'average_flash_area',
    'average_group_area',
    'minimum_flash_area',]
    window_vars = {v:v+'_window' for v in orig_vars}

    ltg_agg=ltg_agg.rename(window_vars)
    # print("renamed")
    # override: skip comparing attrs; copy attrs from the first dataset to the result.
    # New feature since xarray 0.15.2

    full_merge = False
    if full_merge:
        ds_out = xr.merge([ltg_ds[drop_times], ltg_agg[drop_times]], combine_attrs='override', compat='override')
        # print(ds_out.flash_extent_density_window)
        print(ds_out)
        print("ready to compute")

        for var in ds_out.variables.keys():
            if 'chunks' in ds_out[var].encoding:
                # print("popping in ", var)
                ds_out[var].encoding.pop('chunks', None)
                ds_out[var].encoding.pop('preferred_chunks', None)
    #         for var in ds_out.variables.keys():
    #             print(ds_out[var].encoding)
        out_chunks = rechunk_spec.copy()
    #         out_chunks['time'] = 15
        print(out_chunks)
        if os.path.exists(zarr_store):
            ds_out.to_zarr(zarr_store, consolidated=True, append_dim='time')
        else:
            ds_out.chunk(out_chunks).to_zarr(zarr_store, consolidated=True, mode='w')
        # outnames = write_ncs(ds_out)
        # print(outnames)
    else:
        ds_out = ltg_agg[drop_times]
        out_chunks = rechunk_spec.copy()
        print(ds_out)
        print("ready to compute")

        if os.path.exists(zarr_store):
            ds_out.to_zarr(zarr_store, consolidated=True, append_dim='time')
        else:
            ds_out.chunk(out_chunks).to_zarr(zarr_store, consolidated=True, mode='w')


