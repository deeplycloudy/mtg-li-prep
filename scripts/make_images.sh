#!/bin/bash

python -W ignore glm_satpy_overlays.py -o "../../2018/region_images/" \
  -w -i "../../GLMF_201809_windowed.zarr" \
  --dask_workers=1 --dask_threads=4 \
  ../../ABI_201809.zarr > /dev/null 2>&1 &

#python -W ignore glm_satpy_overlays.py -o "../../2018/region_images/" \
#  -i "../../GLMF_201809.zarr" \
#  --dask_workers=4 --dask_threads=4 \
#  ../../ABI_201809.zarr

#  ../../ABI/OR_ABI-L1b-RadF-M3C13_G16_s2018244*.nc


