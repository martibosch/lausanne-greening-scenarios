import datetime
import logging
from os import path

import click
import pandas as pd
import swiss_uhi_utils as suhi

from lausanne_greening_scenarios import settings

# some stations have problematic nan values (e.g., 23513847), so we need to
# filter them by setting a maximum valid temperature
T_VALID = 50


@click.command()
@click.argument('station_data_dir', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--date-start', default='2018-06-01')
@click.option('--date-end', default='2019-08-31')
@click.option('--hour', default=21)
@click.option('--t-min', default=20)
def main(station_data_dir, dst_filepath, date_start, date_end, hour, t_min):
    logger = logging.getLogger(__name__)

    # get the list of datetimes for which we will retrieve the station
    # measurements
    datetimes = pd.date_range(date_start, date_end,
                              freq='D') + datetime.timedelta(hours=hour)

    # assemble a data frame of station temperature measurements
    dfs = []

    # 1. MeteoSwiss
    for tair_column in ['tre000s0', 'tre200s0']:
        dfs.append(
            suhi.df_from_meteoswiss_zip(
                path.join(station_data_dir,
                          f'meteoswiss-lausanne-{tair_column}.zip'),
                tair_column).loc[datetimes].reset_index().groupby(
                    'time').first())

    # 2. VaudAir
    vaudair_df = pd.read_excel(path.join(
        station_data_dir,
        'VaudAir_EnvoiTemp20180101-20200128_EPFL_20200129.xlsx'),
                               index_col=0)
    vaudair_df = vaudair_df.iloc[3:]
    vaudair_df.index = pd.to_datetime(vaudair_df.index)
    for column in vaudair_df.columns:
        vaudair_df[column] = pd.to_numeric(vaudair_df[column])

    dfs.append(vaudair_df.loc[datetimes])

    # 3. Agrometeo
    dfs.append(
        suhi.df_from_agrometeo(
            path.join(station_data_dir,
                      'agrometeo-tre200s0.csv')).loc[datetimes])

    # 4. WSL
    dfs.append(
        suhi.df_from_wsl(path.join(station_data_dir, 'WSLLAF.txt'),
                         'WSLLAF').loc[datetimes])

    # assemble the dataframe
    df = pd.concat(dfs, axis=1)
    # keep only the dates in the index
    df.index = pd.Series(df.index).dt.date

    # filter days with only valid observations
    valid_df = df[(~df.isna().any(axis=1)) & (df.max(axis=1) < T_VALID) &
                  (df.min(axis=1) > t_min)]
    # dump the day with maximum UHI magnitude (need to dump the index in this
    # case)
    valid_df.iloc[(valid_df.max(axis=1) -
                   valid_df.min(axis=1)).argmax()].to_csv(dst_filepath)
    logger.info("dumped air temperature station measurements to %s",
                dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
