#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate satpy-dev

#python abi_to_zarr.py -o ../../ABI_201809.zarr ../../ABI/OR_ABI-L1b-RadF-M3C13_G16_s201824400*.nc
python abi_to_zarr.py -o ../../ABI_201809.zarr ../../ABI/*.nc
