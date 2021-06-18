#!/bin/bash

python glm_satpy_overlays.py -o "../../overlayloop/" \
  -w -i "../../test_2hr_windowed.zarr" \
  --dask_workers=8 --dask_threads=1 \
  ../../ABI/OR_ABI-L1b-RadF-M3C13_G16_s20182460[0-1]*.nc

python glm_satpy_overlays.py -o "../../overlayloop/" \
  -i "../../test_2hr_windowed.zarr" \
  --dask_workers=8 --dask_threads=1 \
  ../../ABI/OR_ABI-L1b-RadF-M3C13_G16_s20182460[0-1]*.nc


