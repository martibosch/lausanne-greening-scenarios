import logging
import os
import shutil
import zipfile
from os import path

import click
import geopandas as gpd
import laspy as lp
import numpy as np
import pandas as pd
import rasterio as rio
from dotenv import find_dotenv, load_dotenv
from rasterio import enums, features
from scipy import ndimage as ndi
from shapely import geometry

from lausanne_heat_islands import settings
from lausanne_heat_islands.utils import download_s3

LIDAR_DIR_KEY = 'cantons/asit-vd/lidar-2015/points'


@click.command()
@click.argument('split_filepath', type=click.Path(exists=True))
@click.argument('response_tiles_dir', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--high-veg-val', type=int, default=5)
@click.option('--dst-tree-val', type=int, default=255)
@click.option('--dst-nodata', type=int, default=0)
@click.option('--keep-raw', is_flag=True)
@click.option('--lidar-raw-dir', type=click.Path(exists=True), required=False)
@click.pass_context
def main(ctx, split_filepath, response_tiles_dir, dst_filepath, high_veg_val,
         dst_tree_val, dst_nodata, keep_raw, lidar_raw_dir):
    logger = logging.getLogger(__name__)

    if lidar_raw_dir is None:
        lidar_raw_dir = 'data/raw/lidar'
        if not path.exists(lidar_raw_dir):
            os.mkdir(lidar_raw_dir)

    dst_dtype = rio.uint8

    def df_from_lidar(tile_filename, bounds):
        tile_ref = path.splitext(tile_filename)[0]
        local_zip_filename = f'{tile_ref}.zip'
        local_zip_filepath = path.join(lidar_raw_dir, local_zip_filename)
        local_las_filepath = path.join(lidar_raw_dir, f'{tile_ref}.las')
        if not path.exists(local_las_filepath):
            # logger.info("downloading LIDAR data for tile %s to %s",
            #             tile_filename, local_tile_filepath)
            ctx.invoke(download_s3.main,
                       file_key=path.join(LIDAR_DIR_KEY, local_zip_filename),
                       output_filepath=local_zip_filepath)
            with zipfile.ZipFile(local_zip_filepath) as zf:
                for filename in zf.namelist():
                    if filename.endswith('.las'):
                        las_data = zf.read(filename)
                        with open(local_las_filepath, 'wb') as fout:
                            fout.write(las_data)
                        break

        with lp.file.File(local_las_filepath) as src:
            c = src.get_classification()
            x = src.get_x_scaled()
            y = src.get_y_scaled()
            cond = ((c == 4) ^
                    (c == 5)) & ((x >= bounds.left) & (x <= bounds.right) &
                                 (y >= bounds.bottom) & (y <= bounds.top))
            return pd.DataFrame({
                'class_val': c[cond],
                'x': x[cond],
                'y': y[cond]
            })

    split_df = pd.read_csv(split_filepath)
    tile_filepaths = split_df[split_df['train']]['img_filepath']

    for tile_filepath in tile_filepaths:
        with rio.open(tile_filepath) as src:
            df = df_from_lidar(path.basename(tile_filepath), src.bounds)
            gser = gpd.GeoSeries(
                [geometry.Point(x, y) for x, y in zip(df['x'], df['y'])])
            try:
                arr = features.rasterize(shapes=[
                    (geom, class_val)
                    for geom, class_val in zip(gser, df['class_val'])
                ],
                                         out_shape=src.shape,
                                         transform=src.transform,
                                         merge_alg=enums.MergeAlg('ADD'))
            except ValueError:
                # there are no vegetation points in this tile, e.g., a lake.
                # Create array of zeros so that the response tile is all
                # non-tree pixels
                arr = np.zeros(src.shape, dtype=dst_dtype)

            meta = src.meta.copy()

        meta.update(dtype=dst_dtype, count=1, nodata=dst_nodata)
        response_tile_filepath = path.join(response_tiles_dir,
                                           path.basename(tile_filepath))
        dst_arr = ndi.binary_opening(
            arr >= high_veg_val).astype(dst_dtype) * dst_tree_val
        with rio.open(response_tile_filepath, 'w', **meta) as dst:
            dst.write(dst_arr, 1)
        logger.info("dumped response tile to %s", response_tile_filepath)

    if not keep_raw:
        shutil.rmtree(lidar_raw_dir)
        logger.info("deleted temp folder with raw LIDAR data at %s",
                    lidar_raw_dir)

    pd.Series(tile_filepaths).to_csv(dst_filepath, index=False, header=False)
    logger.info("dumped list of response tiles to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
