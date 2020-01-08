import glob
import logging
import os
import shutil
import tarfile
from os import path

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
from rasterio import features, warp, windows

from lausanne_heat_islands import settings

LANDSAT_NODATA = 0
LANDSAT8_SUFFIXES = [
    'B1.TIF',
    'B2.TIF',
    'B3.TIF',
    'B4.TIF',
    'B5.TIF',
    'B6.TIF',
    'B7.TIF',
    # 'B8.TIF',
    # 'B9.TIF',
    'B10.TIF',
    'B11.TIF',
]


def _include_band(landsat_filename):
    for landsat_suffix in LANDSAT8_SUFFIXES:
        if landsat_filename.endswith(landsat_suffix):
            return True

    return False


def _iteration_body(landsat_filepath, window, window_transform,
                    agglom_mask_geom):
    with rio.open(landsat_filepath) as src:
        landsat_band = src.read(1, window=window)
    agglom_mask_arr = features.rasterize([agglom_mask_geom],
                                         out_shape=landsat_band.shape,
                                         transform=window_transform)
    return np.where(agglom_mask_arr, landsat_band, LANDSAT_NODATA)


def _first_iteration(landsat_filepath, agglom_mask_gdf):
    with rio.open(landsat_filepath) as src:
        crs = src.crs
        agglom_mask_geom = agglom_mask_gdf.to_crs(crs)['geometry'].iloc[0]
        transform = src.transform

    window = windows.from_bounds(*agglom_mask_geom.bounds, transform=transform)
    window_transform = windows.transform(window, transform)
    return _iteration_body(
        landsat_filepath, window, window_transform,
        agglom_mask_geom), crs, window, window_transform, agglom_mask_geom


def get_landsat_img(landsat_tar_filepath,
                    agglom_mask_gdf,
                    return_meta=False,
                    inpaint_radius=3):
    # first extract the tar contents to a temporary dir
    tmp_dir = path.join(path.dirname(landsat_tar_filepath), 'tmp')
    if not path.exists(tmp_dir):
        os.mkdir(tmp_dir)
    with tarfile.open(landsat_tar_filepath) as tar:
        tar.extractall(tmp_dir)

    def iteration(landsat_filepath, window, window_transform,
                  agglom_mask_geom):
        return _iteration_body(landsat_filepath, window, window_transform,
                               agglom_mask_geom)

    # now get the bands that concern us
    landsat_filepaths = sorted([
        landsat_filepath
        for landsat_filepath in glob.glob(path.join(tmp_dir, '*'))
        if _include_band(landsat_filepath)
    ])

    # use the head/tile design pattern so that we know the output raster
    # characteristics after the first iteration and we can therefore build the
    # raster as we iterate over the rest of bands
    band, crs, window, window_transform, agglom_mask_geom = _first_iteration(
        landsat_filepaths[0], agglom_mask_gdf)

    landsat_img = np.full((len(landsat_filepaths), *band.shape),
                          LANDSAT_NODATA,
                          dtype=band.dtype)
    landsat_img[0] = band
    for i, landsat_filepath in enumerate(landsat_filepaths[1:], start=1):
        landsat_img[i] = iteration(landsat_filepath, window, window_transform,
                                   agglom_mask_geom)

    # remove temporary directory
    shutil.rmtree(tmp_dir)

    if return_meta:
        height, width = band.shape
        return landsat_img, dict(driver='GTiff',
                                 width=width,
                                 height=height,
                                 count=len(landsat_filepaths),
                                 dtype=band.dtype,
                                 transform=window_transform,
                                 crs=crs,
                                 nodata=LANDSAT_NODATA)

    return landsat_img


@click.command()
@click.argument('landsat_tar_filepath', type=click.Path(exists=True))
@click.argument('agglom_extent_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--inpaint-radius', default=3, required=False)
def main(landsat_tar_filepath, agglom_extent_filepath, dst_filepath,
         inpaint_radius):
    logger = logging.getLogger(__name__)

    # load the agglomeration extent
    agglom_mask_gdf = gpd.read_file(agglom_extent_filepath)

    # get the landsat image (and meta data) for the agglomeration extent
    landsat_img, landsat_meta = get_landsat_img(landsat_tar_filepath,
                                                agglom_mask_gdf,
                                                return_meta=True,
                                                inpaint_radius=inpaint_radius)
    logger.info("cropped landsat image to agglomeration extent with shape %s",
                str(landsat_img.shape))

    # reproject it to the project's CRS (i.e., LV95/EPSG:2056)
    landsat_transform = landsat_meta['transform']
    landsat_crs = landsat_meta['crs']
    width = landsat_meta['width']
    height = landsat_meta['height']

    left = landsat_transform.c
    top = landsat_transform.f
    right = left + landsat_transform.a * width
    bottom = top + landsat_transform.e * height

    dst_count = landsat_meta['count']
    dst_crs = agglom_mask_gdf.crs
    dst_dtype = landsat_img.dtype
    dst_nodata = LANDSAT_NODATA
    dst_transform, dst_width, dst_height = warp.calculate_default_transform(
        landsat_meta['crs'], dst_crs, width, height, left, bottom, right, top)
    dst_arr = np.full((dst_count, dst_height, dst_width),
                      dst_nodata,
                      dtype=dst_dtype)
    dst_arr, dst_transform = warp.reproject(landsat_img,
                                            dst_arr,
                                            src_transform=landsat_transform,
                                            src_crs=landsat_crs,
                                            dst_transform=dst_transform,
                                            dst_crs=dst_crs)
    logger.info("reprojected landsat extract from %s to %s with shape %s",
                landsat_crs, dst_crs, str(landsat_img.shape))

    # dump the reprojected landsat image at the agglomeration extent
    with rio.open(dst_filepath,
                  'w',
                  driver='GTiff',
                  width=dst_width,
                  height=dst_height,
                  count=dst_count,
                  crs=dst_crs,
                  transform=dst_transform,
                  dtype=dst_dtype,
                  nodata=dst_nodata) as dst:
        for i in range(dst_count):
            dst.write(dst_arr[i], i + 1)

    logger.info("dumped reprojected landsat extract to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
