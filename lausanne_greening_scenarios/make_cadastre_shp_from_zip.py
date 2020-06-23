import glob
import logging
import os
import re
import shutil
import zipfile
from os import path

import click
import geopandas as gpd
import pandas as pd

from lausanne_greening_scenarios import settings

LAUSANNE_LULC_FILE_REGEX_PATTERN = "Cadastre/(NPCS|MOVD)_CAD_TPR_(BATHS|" \
                           "CSBOIS|CSDIV|CSDUR|CSEAU|CSVERT)_S.*"


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
@click.argument('unzip_filepattern')
def main(input_filepath, output_filepath, unzip_filepattern):
    logger = logging.getLogger(__name__)

    # unzip all zip files to temp dirs
    output_dir = path.dirname(output_filepath)
    temp_dir = path.join(output_dir,
                         path.splitext(path.basename(input_filepath))[0])
    logger.info("Making temporal directory '%s' to extract the files",
                temp_dir)
    if not path.exists(temp_dir):  # and path.isdir(temp_dir):
        os.mkdir(temp_dir)

    with zipfile.ZipFile(input_filepath) as zf:
        zf.extractall(temp_dir)
    zip_filepaths = glob.glob(path.join(temp_dir, '*.zip'))
    logger.info("Extracted %d interim zips to %s", len(zip_filepaths),
                temp_dir)

    for zip_filepath in zip_filepaths:
        logger.info("Extracting files from %s matching %s", zip_filepath,
                    unzip_filepattern)
        p = re.compile(unzip_filepattern)

        with zipfile.ZipFile(zip_filepath) as zf:
            for file_info in zf.infolist():
                if p.match(file_info.filename):
                    filename = '_'.join([
                        path.splitext(path.basename(zip_filepath))[0],
                        path.basename(file_info.filename)
                    ])
                    # Trick from https://bit.ly/2KZkO9G to manipulate zipfile
                    # info and junk inner zip paths
                    file_info.filename = filename
                    zf.extract(file_info, temp_dir)

    shp_filepaths = glob.glob(path.join(output_dir, '**/*_S.shp'),
                              recursive=True)
    logger.info("Assembling single data frame from files: %s",
                ', '.join(shp_filepaths))
    # process 'divers' filepaths later so that the other (more specific) LULC
    # shapefiles take priority
    divers_filepaths = [
        divers_filepath for divers_filepath in shp_filepaths
        if divers_filepath.endswith('_CSDIV_S.shp')
    ]
    other_filepaths = [
        other_filepath for other_filepath in shp_filepaths
        if not other_filepath.endswith('_CSDIV_S.shp')
    ]
    # Based on https://bit.ly/2znOaIh
    gdf = pd.concat([
        gpd.read_file(shp_filepath)
        for shp_filepath in divers_filepaths + other_filepaths
    ],
                    sort=False).pipe(gpd.GeoDataFrame)
    gdf.crs = gpd.read_file(shp_filepaths[0]).crs

    # delete temp dir
    # for zip_filepath in zip_filepaths:
    #     zip_temp_dir = path.join(output_dir,
    #                         path.splitext(path.basename(zip_filepath))[0])

    #     logger.info("Deleting temporal directory '%s'", zip_temp_dir)
    #     shutil.rmtree(zip_temp_dir)
    logger.info("Deleting temporal directory '%s'", temp_dir)
    shutil.rmtree(temp_dir)

    logger.info("Dumping assembled data frame to '%s'", output_filepath)
    gdf.to_file(output_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
