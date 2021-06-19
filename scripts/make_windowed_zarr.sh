#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate glmval

#python add_rolling.py --dask_workers=1 --dask_threads=8 --t_chunks=60 \
#	-i ../../GLMF_201809.zarr -o ../../GLMF_201809_windowed.zarr 
#> window_add.log 2>&1 

python add_rolling.py --dask_workers=1 --dask_threads=8 --t_chunks=60 \
	-i ../../GLMF_201809.zarr -o ./test_GLMF_201809_windowed.zarr 
#> window_add.log 2>&1 

