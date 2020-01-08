import logging

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
import urban_footprinter as ufp
from rasterio import features, transform, windows
from scipy import ndimage as ndi
from shapely import geometry

from lausanne_heat_islands import settings

AGGLOM_SLUG = 'lausanne'
CADASTRE_LULC_COLUMN = 'GENRE'
URBAN_CLASSES = list(range(8))
LULC_WATER_VAL = 14
SIEVE_SIZE = 10
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
@click.argument('dst_geom_filepath', type=click.Path())
@click.option('--kernel-radius', type=int, default=500, required=False)
@click.option('--urban-threshold', type=float, default=.15, required=False)
@click.option('--largest-patch-only', default=True, required=False)
@click.option('--exclude-lake', default=True, required=False)
@click.option('--buffer-dist', type=int, default=1000, required=False)
@click.option('--output-nodata-val', type=int, default=255, required=False)
def main(cadastre_filepath, dst_filepath, dst_geom_filepath, kernel_radius,
         urban_threshold, largest_patch_only, exclude_lake, buffer_dist,
         output_nodata_val):
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
    uf = ufp.UrbanFootprinter(cadastre_arr,
                              urban_classes=URBAN_CLASSES,
                              res=res)
    urban_mask = uf.compute_footprint_mask(
        kernel_radius,
        urban_threshold,
        largest_patch_only=largest_patch_only,
        buffer_dist=buffer_dist)
    logger.info("obtained extent of the largest urban cluster (%d pixels)",
                np.sum(urban_mask))

    # exclude lake
    if exclude_lake:
        # TODO: arguments to customize `LULC_WATER_VAL` and `SIEVE_SIZE`
        label_arr = ndi.label(cadastre_arr == LULC_WATER_VAL,
                              ndi.generate_binary_structure(2, 2))[0]
        cluster_label = np.argmax(
            np.unique(label_arr, return_counts=True)[1][1:]) + 1
        largest_cluster = np.array(label_arr == cluster_label, dtype=np.uint8)
        urban_mask = features.sieve(
            np.array(urban_mask.astype(bool) & ~largest_cluster.astype(bool),
                     dtype=urban_mask.dtype), SIEVE_SIZE)

    # get window and transform of valid data points, i.e., the computed extent
    extent_window = windows.get_data_window(urban_mask, nodata=0)
    extent_transform = windows.transform(extent_window, cadastre_transform)
    dst_arr = np.where(urban_mask, cadastre_arr,
                       output_nodata_val)[windows.window_index(extent_window)]

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
        dst.write(dst_arr, 1)
    logger.info("dumped rasterized dataset to %s", dst_filepath)

    if dst_geom_filepath:
        # save the geometry extent
        # urban_mask_geom = uf.compute_footprint_mask_shp(
        #     kernel_radius,
        #     urban_threshold,
        #     largest_patch_only=largest_patch_only,
        #     buffer_dist=buffer_dist,
        #     transform=extent_transform)
        urban_mask_geom = geometry.shape([
            (geom, val) for geom, val in features.shapes(
                np.array(dst_arr != output_nodata_val, dtype=np.uint8),
                transform=extent_transform) if val == 1
        ][0][0])
        gpd.GeoSeries([urban_mask_geom],
                      crs=cadastre_gdf.crs).to_file(dst_geom_filepath)
        logger.info("dumped extent geometry to %s", dst_geom_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
