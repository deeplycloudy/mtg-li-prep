import dask

import argparse
parse_desc = """Create overlays of GLM imagery on ABI imagery"""

def create_parser():
    parser = argparse.ArgumentParser(description=parse_desc)
    parser.add_argument(dest='abi_filenames',metavar='ABI filenames',
                        nargs='*')
    parser.add_argument('-o', '--output_path',
                        metavar='directory for output images, including'
                                ' any filename prefix',
                        required=True, dest='dir_out', action='store')
    parser.add_argument('-i', '--input_path',
                        metavar='zarr store with 1 min GLM data',
                        required=True, dest='zarr_in', action='store')
    parser.add_argument('-w', '--windowed', default=False, action='store_true',
                        required=False, dest='do_window')
    # parser.add_argument('-m', '--temp_zarr_path',
    #                     metavar='filename template including path',
    #                     required=False, dest='zarr_temp', action='store',
    #                     default='./_glm_window_store_temp.zarr')
    # parser.add_argument('-x', '--x_chunks',
    #                     metavar='number of x gridpoints per chunk',
    #                     required=False, dest='x_chunks', action='store',
    #                     default=678, type=int)
    # parser.add_argument('-y', '--y_chunks',
    #                     metavar='number of y gridpoints per chunk',
    #                     required=False, dest='y_chunks', action='store',
    #                     default=678, type=int)
    # parser.add_argument('-t', '--t_chunks',
    #                     metavar='number of time intervals per chunk',
    #                     required=False, dest='t_chunks', action='store',
    #                     default=60, type=int)
    parser.add_argument('--dask_workers',
                        metavar='number of dask workers',
                        required=False, dest='dask_workers', action='store',
                        default=1, type=int)
    parser.add_argument('--dask_threads',
                        metavar='number of threads per dask worker',
                        required=False, dest='dask_threads', action='store',
                        default=2, type=int)
    # parser.add_argument('-a', '--agg_minutes',
    #                     metavar='number of minutes in window aggregation',
    #                     required=False, dest='agg_minutes', action='store',
    #                     default=5, type=float)

    return parser





from glob import glob
from copy import deepcopy

import xarray as xr
import numpy as np
import pandas as pd

from satpy import Scene, MultiScene
from satpy.composites import BackgroundCompositor, GenericCompositor
from satpy.readers import group_files


from glmtools.io.imagery import gen_file_times

from overlap import regions as overlap_regions


# CONFIG
coast_dir = '/archive/shapefiles/gshhg-shp-2/'

# works with scene.show and writer.add_overlay
overlay_spec = {'coast_dir':coast_dir, 'color':(64,128,255), 'width':1}
decorate_spec = {
    "decorate": [
        {"text": {
            "txt": "{time:%Y-%m-%d %H:%M} UTC",
            "align": {
                "top_bottom": "bottom",
                "left_right": "right"},
#                 "font": '/usr/share/fonts/truetype/arial.ttf',
            "font_size": 18,
            "height": 24,
            "bg": "white",
            "bg_opacity": 128,
#                 "line": "white"
            },
#              "scale":{
#              }
        },
    ]
}
enh_args={
    "decorate": decorate_spec,
    "overlay": overlay_spec
}



def open_abi_time_series(filenames, chunks=None):
    """ Convenience function for combining individual 1-min GLM gridded imagery
    files into a single xarray.Dataset with a time dimension.

    Creates an index on the time dimension.

    Time is the middle of the scan interval, so matching with `xr.sel` on the time dimension
    with a time from another dataset will select the appropriate ABI scan interval for that time.

    time_start and time_end are also preserved as variables, and are taken from the filenames.

    The time dimension will be in the order in which the files are listed
    due to the behavior of combine='nested' in open_mfdataset.

    Adjusts the time_coverage_start and time_coverage_end metadata.
    """
    # Need to fix time_coverage_start and _end in concat dataset
    starts = np.asarray([t for t in gen_file_times(filenames)])
    ends = np.asarray([t for t in gen_file_times(filenames, time_attr='time_coverage_end')])

    d = xr.open_mfdataset(filenames, concat_dim='time', chunks=chunks, combine='nested')
    d['time'] = starts + (ends-starts)/2.0
    d = d.set_index({'time':'time'})
    d = d.set_coords('time')
    d['time_start'] = xr.DataArray(starts, dims=['time'])
    d['time_end'] = xr.DataArray(ends, dims=['time'])

    d.attrs['time_coverage_start'] = pd.Timestamp(min(starts)).isoformat()
    d.attrs['time_coverage_end'] = pd.Timestamp(max(ends)).isoformat()

    return d



# This is a horrible hack to use the ABI projection machinery to
# generate the area definition needed to crop. This area_def is
# added as an atribute on the DataArray just before it's added to the Scene.

from satpy.readers.abi_base import NC_ABI_BASE
class FakeABI:
    def __init__(self, ds):
        self.nc = ds
        self.ncols = ds.dims['x']
        self.nlines = ds.dims['y']
    def __getitem__(self,item):
        return self.nc[item]
def get_goes_area(ds):
    area_def = NC_ABI_BASE._get_areadef_fixedgrid(FakeABI(ds), None)
    return area_def

# === End horrible hack ===

def lims_to_bbox(lims):
    return lims[0], lims[2], lims[1], lims[3]

# @dask.delayed
def data_for_time(glm_fn, abi_filenames, time, debug=False):
    ltg_ds = xr.open_zarr(glm_fn)
    if 'windowed' in glm_fn:
        orig = glm_fn.replace('_windowed', '')
        orig_ltg_ds = xr.open_zarr(orig)
        ltg_ds['goes_imager_projection'] = orig_ltg_ds['goes_imager_projection']
        orig_ltg_ds.close()
    # abi_ds = open_abi_time_series(abi_filenames, chunks={'x':1356, 'y':1356})
    abi_ds = xr.open_zarr(abi_filenames[0]) #, chunks={'x':1356, 'y':1356})

    this_ltg = ltg_ds.sel(time=time, method='nearest')
    this_sat = abi_ds.sel(time=time, method='nearest')

    # Add some metadata usually added by the SatPy file readers
    this_ltg['sensor'] = 'glm'
    this_sat['sensor'] = 'abi'

    if debug:
        print("GLM:", this_ltg.time.data, "; ABI:", this_sat.time_start.data, '-', this_sat.time_end.data)
    return this_ltg, this_sat

# @dask.delayed
def save_each_region(scene, time, composite, prefix=''):
    timestr = str(time.dt.strftime('%Y%m%d_%H%M').data)

    # Need to loop over each of the keys in overlap_regions
    for region in overlap_regions:
        bbox = lims_to_bbox(overlap_regions[region])
        scn = scene.crop(xy_bbox=bbox)
        scn.load([composite])

        outname = f'{prefix}{timestr}_{composite}-{region}.png'
        print(outname)

        decorate = deepcopy(decorate_spec)
        decorate["decorate"][0]["text"]["txt"] = str(time.dt.strftime("%Y-%m-%d %H:%M UTC").data)
        scn.save_dataset(composite, filename=outname, overlay=overlay_spec, decorate=decorate)

# @dask.delayed
def build_one_scene(this_ltg, this_sat, fields):

    scn = Scene()
    # Thanks to David Hoese for the hack to override the compositor loading that
    # normally is done by the file-based readers.
    from satpy.dependency_tree import DependencyTree
    comps, mods = scn._composite_loader.load_compositors({'abi', 'glm'})
    scn._dependency_tree = DependencyTree(scn._readers, comps, mods)

    # Manually build up just what we need for the scene.
    da = this_sat['Rad']
    da.attrs['area'] = get_goes_area(this_sat)
    da.attrs['sensor']='abi'
    da.attrs['name']='C13'
    scn['C13'] = da
    for field in fields:
        da = this_ltg[field]
        masked_da = xr.where(da>0.1e-15, da, np.nan)
        masked_da.name = this_ltg[field].name
        masked_da.attrs['sensor']='glm'
        masked_da.attrs['name']=field
        masked_da.attrs['area'] = get_goes_area(this_ltg)
        scn[field] = masked_da
        # print(field, scn[field].min().compute().data, scn[field].max().compute().data)
    return scn

#@dask.delayed
def process_one_time(glm_fn, abi_fns, grid_time, fields, outfile_prefix):
    this_ltg, this_sat = data_for_time(glm_fn, abi_fns, grid_time)
    scn = build_one_scene(this_ltg, this_sat, fields)
    for field in fields:
        composite = 'C13_'+field
        save_each_region(scn, grid_time, composite, prefix=outfile_prefix)

from itertools import zip_longest
def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def main(glm_fn, abi_fns, do_window=False, outfile_prefix=''):
    fields = ['flash_extent_density', 'minimum_flash_area',
                      'total_energy', 'event_flash_fraction']
    if do_window:
        single_or_window_fields = [f+'_window' for f in fields]
    else:
        single_or_window_fields = fields

    start_index=0
    # Inevitably things crash, here's how to find the time for start_index:
    # glm_ds = xr.open_zarr('../../GLMF_201809_windowed.zarr/')
    # ts=glm_ds.time.sortby('time')
    # ts[19*60+38]
    # 19*60+38
    glm_ds = xr.open_zarr(glm_fn)
    glm_times = glm_ds.time.sortby('time')[start_index:].compute()
    glm_ds.close()



    #filename_lists = []
    #for iframe, grid_time in enumerate(glm_times):
    #    filename_lists.append(process_one_time(glm_fn, abi_fns, grid_time,
    #        single_or_window_fields, outfile_prefix))
    #print(filename_lists)
    #dask.compute(*filename_lists)

    # Manually loop over chunks to work around some sort of memory leak
    for these_times in grouper(120, glm_times):
        with Pool(12) as p:
            args = [(glm_fn, abi_fns, grid_time,
                     single_or_window_fields, outfile_prefix)
                     for grid_time in these_times if grid_time is not None]
            p.starmap(process_one_time, args)

if __name__=='__main__':
    parser = create_parser()
    args = parser.parse_args()

    import dask
    # dask.config.set(split_every=4)
    #from dask.distributed import Client
    #dask_client=Client(n_workers=args.dask_workers, threads_per_worker=args.dask_threads)
    #print(dask_client)
    from multiprocessing import Pool


    abi_filenames = args.abi_filenames
    abi_filenames.sort()

    main(args.zarr_in, abi_filenames, outfile_prefix=args.dir_out, do_window=args.do_window)
