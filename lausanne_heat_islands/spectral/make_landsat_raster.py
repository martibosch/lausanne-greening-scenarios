import gzip
import logging
import os
import shutil
import tarfile
from os import path

import click
import cv2
import geopandas as gpd
import numpy as np
import rasterio as rio
from rasterio import features, windows
from shapely import geometry

from lausanne_heat_islands import settings

LANDSAT_FILENAMES = [
    'LE07_L1TP_196028_20120313_20161202_01_T1_B1.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B2.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B3.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B4.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B5.TIF',
    # 'LE07_L1TP_196028_20120313_20161202_01_T1_B6_VCID_1.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B6_VCID_2.TIF',
    'LE07_L1TP_196028_20120313_20161202_01_T1_B7.TIF',
    # 'LE07_L1TP_196028_20120313_20161202_01_T1_B8.TIF',
    # 'LE07_L1TP_196028_20120313_20161202_01_T1_BQA.TIF'
]
LANDSAT_CRS = 'EPSG:32631'
LANDSAT_DTYPE = 'uint8'
LANDSAT_NODATA = 0

GAP_MASK_DIR = 'gap_mask'


@click.command()
@click.argument('landsat_tar_filepath', type=click.Path(exists=True))
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--tmp-dir', type=click.Path(exists=True), required=False)
@click.option('--inpaint-radius', default=3, required=False)
def main(landsat_tar_filepath, agglom_lulc_filepath, dst_filepath, tmp_dir,
         inpaint_radius):
    logger = logging.getLogger(__name__)
    logger.info("assembling landsat bands in %s into single TIF",
                landsat_tar_filepath)
    if tmp_dir is None:
        tmp_dir = path.join(path.dirname(landsat_tar_filepath), 'tmp')
        if not path.exists(tmp_dir):
            os.mkdir(tmp_dir)

    with tarfile.open(landsat_tar_filepath) as tar:
        tar.extractall(tmp_dir)
    logger.info("extracted %s to the temporary directory at %s",
                landsat_tar_filepath, tmp_dir)

    # get the agglomeration extent
    with rio.open(agglom_lulc_filepath) as src:
        agglom_lulc = src.read(1)
        agglom_mask = np.array(agglom_lulc != src.nodata, dtype=np.uint8)
        agglom_mask_geom = gpd.GeoSeries(
            [
                geometry.shape([(geom, val) for geom, val in features.shapes(
                    agglom_mask, transform=src.transform) if val == 1][-1][0])
            ],
            crs=src.crs).to_crs(LANDSAT_CRS).iloc[0]

    # inpainting to correct for the Landsat's 7 scan line corrector malfunction
    gap_mask_dir = path.join(tmp_dir, GAP_MASK_DIR)

    # use the head/tile design pattern so that we know the output raster
    # characteristics after the first iteration and we can therefore build the
    # raster as we iterate over the rest of bands
    def _iteration_body(landsat_filename, mask_src, transform, window,
                        window_transform):
        mask_arr = mask_src.read(1, window=window)
        # opencv needs the mask to be of ones and zeros (pixels that need and
        # do not inpainting respectively
        zero_cond = mask_arr == 0
        mask_arr[zero_cond] = 1
        mask_arr[~zero_cond] = 0
        with rio.open(path.join(tmp_dir, landsat_filename)) as landsat_src:
            # restore the image
            restored_img = cv2.inpaint(landsat_src.read(1, window=window),
                                       mask_arr, inpaint_radius,
                                       cv2.INPAINT_TELEA)
            # now crop the image to the exact agglomeration extent
            agglom_mask_arr = features.rasterize([agglom_mask_geom],
                                                 out_shape=restored_img.shape,
                                                 transform=window_transform)
        return np.where(agglom_mask_arr, restored_img, LANDSAT_NODATA)

    def _landsat_mask_open_gzip(i, landsat_filename):
        return gzip.open(
            path.join(gap_mask_dir,
                      f"{landsat_filename}.gz".replace(f"B{i}", f"GM_B{i}")))

    def first_iteration(landsat_filename):
        with rio.open(_landsat_mask_open_gzip(1,
                                              landsat_filename)) as mask_src:
            transform = mask_src.transform
            # first mask for the agglomeration extent minimum containing
            # rectangle
            window = windows.from_bounds(*agglom_mask_geom.bounds,
                                         transform=transform)
            window_transform = windows.transform(window, transform)

            return _iteration_body(
                landsat_filename, mask_src, transform, window,
                window_transform), transform, window, window_transform

    # first iteration
    landsat_img, transform, window, window_transform = first_iteration(
        LANDSAT_FILENAMES[0])

    # we now curry the variables of interest into `_iteration_body` for the
    # remaining iterations
    def iteration(i, landsat_filename):
        with rio.open(_landsat_mask_open_gzip(i,
                                              landsat_filename)) as mask_src:
            return _iteration_body(landsat_filename, mask_src, transform,
                                   window, window_transform)

    # from the characteristics of the image resulting from the first iteration,
    # we can build the output raster
    height, width = landsat_img.shape
    with rio.open(dst_filepath,
                  'w',
                  driver='GTiff',
                  width=width,
                  height=height,
                  count=len(LANDSAT_FILENAMES),
                  dtype=LANDSAT_DTYPE,
                  transform=window_transform,
                  crs=LANDSAT_CRS,
                  nodata=LANDSAT_NODATA) as dst:
        dst.write(landsat_img, 1)
        for i, landsat_filename in enumerate(LANDSAT_FILENAMES[1:], start=2):
            dst.write(iteration(i, landsat_filename), i)
    logger.info(
        "cropped and corrected %d landsat bands to the agglomeration extent",
        len(LANDSAT_FILENAMES))
    logger.info("dumped rasterized dataset to %s", dst_filepath)

    # remove temporary directory
    shutil.rmtree(tmp_dir)
    logger.info("deleted temporary directory at %s", tmp_dir)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
