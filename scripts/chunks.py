"""
Wrapper for rechunker that takes dimension names instead of variable names.
Source: https://github.com/pangeo-data/rechunker/issues/83
"""

import shutil
import zarr
from rechunker.api import rechunk
import xarray as xr
import pathlib


def rechunker_wrapper(source_store, target_store, temp_store, chunks=None, mem="2GiB", consolidated=False, verbose=True):

    # convert str to paths
    def maybe_convert_to_path(p):
        if isinstance(p, str):
            return pathlib.Path(p)
        else:
            return p

    print("converting paths")
    source_store = maybe_convert_to_path(source_store)
    target_store = maybe_convert_to_path(target_store)
    temp_store = maybe_convert_to_path(temp_store)

    print("creating temp")
    # erase target and temp stores
    if temp_store.exists():
        shutil.rmtree(temp_store)

    if target_store.exists():
        shutil.rmtree(target_store)


    if isinstance(source_store, xr.Dataset):
        print("working with xr")
        g = source_store  # trying to work directly with a dataset
        ds_chunk = g
    else:
        print("working with zr")
        g = zarr.group(str(source_store))
        # get the correct shape from loading the store as xr.dataset and parse the chunks
        ds_chunk = xr.open_zarr(str(source_store))
        

    print("path formatting")
    # convert all paths to strings
    source_store = str(source_store)
    target_store = str(target_store)
    temp_store = str(temp_store)

    group_chunks = {}
    # newer tuple version that also takes into account when specified chunks are larger than the array
    for var in ds_chunk.variables:
        print('checking var', var)
        # pick appropriate chunks from above, and default to full length chunks for dimensions that are not in `chunks` above.
        group_chunks[var] = []
        for di in ds_chunk[var].dims:
            if di in chunks.keys():
                if chunks[di] > len(ds_chunk[di]):
                    group_chunks[var].append(len(ds_chunk[di]))
                else:
                    group_chunks[var].append(chunks[di])

            else:
                group_chunks[var].append(len(ds_chunk[di]))

        group_chunks[var] = tuple(group_chunks[var])
    if verbose:
        print(f"Rechunking to: {group_chunks}")
    rechunked = rechunk(g, group_chunks, mem, target_store, temp_store=temp_store)
    rechunked.execute()
    if consolidated:
        if verbose:
            print('consolidating metadata')
        zarr.convenience.consolidate_metadata(target_store)
    if verbose:
        print('removing temp store')
    shutil.rmtree(temp_store)
    if verbose:
        print('done')