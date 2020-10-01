import logging
from os import environ

import click
import dotenv
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import salem
import swiss_uhi_utils as suhi

from lausanne_greening_scenarios import settings

# 46.519833 degrees in radians
LAUSANNE_LAT = 0.811924


@click.command()
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('agglom_extent_filepath', type=click.Path(exists=True))
@click.argument('station_t_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--buffer-dist', type=float, default=2000)
def main(agglom_lulc_filepath, agglom_extent_filepath, station_t_filepath,
         dst_filepath, buffer_dist):
    logger = logging.getLogger(__name__)

    # get the reference information: agglomeration extent (geom), raster
    # metadata (data array)
    agglom_extent_gdf = gpd.read_file(agglom_extent_filepath)
    crs = agglom_extent_gdf.crs
    agglom_geom = agglom_extent_gdf.loc[0]['geometry'].buffer(buffer_dist)
    # lake_geom = agglom_extent_gdf.loc[1]['geometry']
    agglom_lulc_da = salem.open_xr_dataset(agglom_lulc_filepath)['data']

    # preprocess air temperature station measurements data frame (here we just
    # need the dates)
    date = pd.to_datetime(
        pd.read_csv(station_t_filepath, index_col=0).iloc[:, 0].name)

    suhi.settings.METEOSWISS_S3_CLIENT_KWARGS = {
        'endpoint_url': environ.get('S3_ENDPOINT_URL')
    }
    # get the ref. evapotranpiration data array
    ref_eto_da = suhi.get_ref_et_da(pd.Series([date]), agglom_geom,
                                    LAUSANNE_LAT, crs)

    # align it to the reference raster (i.e., LULC)
    ref_eto_da = suhi.align_ds(ref_eto_da, agglom_lulc_da)

    # extract the raster from the single date of the data array
    ref_eto_arr = ref_eto_da.isel(time=0).data
    # dump it with the transform/projection metadata from the reference raster
    with rio.open(agglom_lulc_filepath) as src:
        meta = src.meta.copy()
    meta.update(dtype=ref_eto_arr.dtype, nodata=np.nan)
    with rio.open(dst_filepath, 'w', **meta) as dst:
        dst.write(ref_eto_arr, 1)
    logger.info("dumped reference evapotranspiration raster to %s",
                dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    dotenv.load_dotenv(dotenv.find_dotenv())

    main()
