#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate glmval

#python glml2_to_zarr.py --dask_workers=2 -o ../../GLMF_201809.zarr ../../2018/Sep/01/OR_GLM-L2-GLMF-M3_G16_s20182440[4-9]*.nc
#python glml2_to_zarr.py --dask_workers=2 -o ../../GLMF_201809.zarr ../../2018/Sep/01/OR_GLM-L2-GLMF-M3_G16_s20182441*.nc
#python glml2_to_zarr.py --dask_workers=2 -o ../../GLMF_201809.zarr ../../2018/Sep/01/OR_GLM-L2-GLMF-M3_G16_s20182442*.nc
#python glml2_to_zarr.py -o ../../GLMF_201809.zarr ../../2018/Sep/01/*.nc
python glml2_to_zarr.py -o ../../GLMF_201809.zarr ../../2018/Sep/02/*.nc
python glml2_to_zarr.py -o ../../GLMF_201809.zarr ../../2018/Sep/03/*.nc
python glml2_to_zarr.py -o ../../GLMF_201809.zarr ../../2018/Sep/04/*.nc
