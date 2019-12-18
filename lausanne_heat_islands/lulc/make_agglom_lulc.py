import logging

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
import urban_footprinter as ufp
from rasterio import features, transform, windows

from lausanne_heat_islands import settings

AGGLOM_SLUG = 'lausanne'
CADASTRE_LULC_COLUMN = 'GENRE'
URBAN_CLASSES = list(range(8))
# CRS = {'init': 'epsg:2056'}


def lausanne_reclassify(value, output_nodata_val):
    if value < 0:
        return output_nodata_val
    if value >= 9:
        return value - 1
    else:
        return value


@click.command()
@click.argument('cadastre_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--kernel-radius', type=int, default=500, required=False)
@click.option('--urban-threshold', type=float, default=.15, required=False)
@click.option('--largest-patch-only', default=True, required=False)
@click.option('--buffer-dist', type=int, default=1000, required=False)
@click.option('--output-nodata-val', type=int, default=255, required=False)
def main(cadastre_filepath, dst_filepath, kernel_radius, urban_threshold,
         largest_patch_only, buffer_dist, output_nodata_val):
    logger = logging.getLogger(__name__)
    logger.info("preparing raster agglomeration LULC for %s", AGGLOM_SLUG)

    # hardcoded values extracted from the Swiss GMB agglomeration boundaries
    west, south, east, north = (2512518, 1146825, 2558887, 1177123)
    res = 10
    cadastre_transform = transform.from_origin(west + res // 2,
                                               north - res // 2, res, res)
    cadastre_gdf = gpd.read_file(cadastre_filepath,
                                 bbox=(west, south, east, north))
    cadastre_ser = cadastre_gdf[CADASTRE_LULC_COLUMN].apply(
        lausanne_reclassify, args=(output_nodata_val, ))

    cadastre_shape = ((north - south) // res, (east - west) // res)
    cadastre_arr = features.rasterize(
        ((geom, value)
         for geom, value in zip(cadastre_gdf['geometry'], cadastre_ser)),
        out_shape=cadastre_shape,
        fill=output_nodata_val,
        transform=cadastre_transform,
        dtype=np.uint8)
    logger.info("rasterized cadastre vector LULC dataset to shape %s",
                str(cadastre_shape))

    # get the urban extent mask according to the criteria used in the "Atlas
    # of Urban Expansion, The 2016 Edition" by Angel, S. et al.
    urban_mask = ufp.urban_footprint_mask(
        cadastre_arr,
        kernel_radius,
        urban_threshold,
        urban_classes=URBAN_CLASSES,
        largest_patch_only=largest_patch_only,
        buffer_dist=buffer_dist,
        res=res)
    logger.info("obtained extent of the largest urban cluster (%d pixels)",
                np.sum(urban_mask))

    # get window and transform of valid data points, i.e., the computed extent
    extent_window = windows.get_data_window(urban_mask, nodata=0)
    extent_transform = windows.transform(extent_window, cadastre_transform)

    # dump it
    with rio.open(dst_filepath,
                  'w',
                  driver='GTiff',
                  width=extent_window.width,
                  height=extent_window.height,
                  count=1,
                  crs=cadastre_gdf.crs,
                  transform=extent_transform,
                  dtype=np.uint8,
                  nodata=output_nodata_val) as dst:
        dst.write(
            np.where(urban_mask, cadastre_arr,
                     output_nodata_val)[windows.window_index(extent_window)],
            1)
    logger.info("dumped rasterized dataset to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
