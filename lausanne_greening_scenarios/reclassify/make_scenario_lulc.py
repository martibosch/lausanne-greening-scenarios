import logging

import click
import numpy as np
import pandas as pd
import rasterio as rio
import xarray as xr
from rasterio import transform
from scipy import ndimage as ndi

from lausanne_heat_islands import settings

LULC_CODES = [0, 1, 2, 3, 7]
ROAD_CODE = 1

KERNEL_MOORE = ndi.generate_binary_structure(2, 2)


class ScenarioGenerator:
    def __init__(self, agglom_lulc_filepath, biophysical_table_filepath):
        # read the LULC raster
        with rio.open(agglom_lulc_filepath) as src:
            lulc_arr = src.read(1)
            lulc_meta = src.meta.copy()
            lulc_bounds = src.bounds

        # read the biophysical table
        biophysical_df = pd.read_csv(biophysical_table_filepath)

        # exclude inner road pixels (we cannot plant trees in the middle of a
        # highway)
        self.inner_road_mask = ndi.binary_erosion(
            np.isin(
                lulc_arr, biophysical_df[biophysical_df['lulc_code'] ==
                                         ROAD_CODE]['lucode']), KERNEL_MOORE)

        # increase the tree cover by changing the land cover code of
        # roads/paths, sidewalks, blocks and other impervious surfaces to the
        # equivalent code with greater tree cover
        next_code_dict = {}
        for lulc_code in LULC_CODES:
            shade_gb = biophysical_df[biophysical_df['lulc_code'] ==
                                      lulc_code].groupby('shade')
            if len(shade_gb) == 1:
                pass
            else:
                for i in range(len(shade_gb)):
                    # select lucodes that have the same base `lulc_code` and
                    # `building_intensity`
                    eligible_lucode_df = shade_gb.nth(i)
                    # filter the above data frame to enforce that the
                    # proportion tree canopy cover plus the proportion of
                    # building cover is not more than 1 (i.e., 100% of the
                    # pixel)
                    eligible_lucode_df = eligible_lucode_df[
                        eligible_lucode_df.index +
                        eligible_lucode_df['building_intensity'] <= 1]
                    # select the next lucode as the lucode with maximum
                    # possible tree canopy cover
                    next_lucode = eligible_lucode_df.iloc[-1]['lucode']
                    for lucode in eligible_lucode_df.iloc[:-1]['lucode']:
                        next_code_dict[lucode] = next_lucode
        self.next_code_dict = next_code_dict

        # save the LULC raster and biophysical table as attributes
        self.lulc_arr = lulc_arr
        self.lulc_meta = lulc_meta
        self.lulc_bounds = lulc_bounds
        self.biophysical_df = biophysical_df
        # use this in `generate_lulc_arr`
        self.change_df_idx = np.flatnonzero(self.lulc_arr)

    def generate_lulc_arr(self,
                          shade_threshold,
                          change_prop,
                          interaction='cluster'):
        if change_prop == 0:
            return self.lulc_arr.copy()

        # convolution
        conv_result = ndi.convolve(
            np.isin(
                self.lulc_arr,
                self.biophysical_df[self.biophysical_df['shade'] >=
                                    shade_threshold]['lucode']).astype(
                                        np.int32), KERNEL_MOORE)

        # build change data frame
        change_df = pd.DataFrame(index=self.change_df_idx)

        # get list of pixels that can be changed and their next code
        for lucode in self.next_code_dict:
            change_df.loc[np.flatnonzero(
                self.lulc_arr ==
                lucode), 'next_code'] = self.next_code_dict[lucode]

        change_df['conv_result'] = conv_result.flatten()[self.change_df_idx]
        change_df = change_df.dropna()
        # exclude the inner road pixels
        change_df = change_df.drop(np.flatnonzero(self.inner_road_mask),
                                   errors='ignore')
        change_df['next_code'] = change_df['next_code'].astype(np.int32)

        if change_prop == 1:
            pixels_to_change_df = change_df
        else:
            # decide which pixels will be changed (depending on desired
            # interaction between high tree cover pixels)
            num_to_change = int(len(change_df) * change_prop)

            if interaction == 'cluster':
                ascending = False
            else:  # scatter
                ascending = True

            sorted_df = change_df.sort_values('conv_result',
                                              ascending=ascending)
            last_lucode = sorted_df.iloc[:
                                         num_to_change]['conv_result'].iloc[-1]
            conv_result_ser = sorted_df['conv_result']
            if ascending:
                pixels_to_change_df = sorted_df[conv_result_ser < last_lucode]
            else:
                pixels_to_change_df = sorted_df[conv_result_ser > last_lucode]

            pixels_to_change_df = pd.concat([
                pixels_to_change_df,
                sorted_df[conv_result_ser == last_lucode].sample(
                    num_to_change - len(pixels_to_change_df))
            ])

        # now build the new LULC array and change the pixels
        new_lulc_arr = self.lulc_arr.copy()
        for next_code, next_code_df in pixels_to_change_df.groupby(
                'next_code'):
            new_lulc_arr.ravel()[next_code_df.index] = next_code

        return new_lulc_arr


@click.command()
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
@click.option('--num-scenario-runs', default=10)
@click.option('--change-prop-step', default=0.1)
def main(agglom_lulc_filepath, biophysical_table_filepath, dst_filepath,
         shade_threshold, num_scenario_runs, change_prop_step):
    logger = logging.getLogger(__name__)

    sg = ScenarioGenerator(agglom_lulc_filepath, biophysical_table_filepath)

    scenario_runs = range(num_scenario_runs)
    interactions = ['cluster', 'scatter']
    change_props = np.arange(0, 1 + change_prop_step, change_prop_step)

    rows = np.arange(sg.lulc_meta['height'])
    cols = np.arange(sg.lulc_meta['width'])
    lulc_transform = sg.lulc_meta['transform']
    xs, _ = transform.xy(lulc_transform, cols, cols)
    _, ys = transform.xy(lulc_transform, rows, rows)

    scenario_arr = np.array([[[
        sg.generate_lulc_arr(shade_threshold, change_prop, interaction)
        for scenario_run in scenario_runs
    ] for change_prop in change_props] for interaction in interactions])
    # update the raster meta so that it can be encoded as netcdf attributes
    attrs = sg.lulc_meta.copy()
    attrs.update(crs=f'epsg:{attrs["crs"].to_epsg()}',
                 transform=attrs['transform'].to_gdal())
    scenario_da = xr.DataArray(scenario_arr,
                               dims=('interaction', 'change_prop',
                                     'scenario_run', 'y', 'x'),
                               coords={
                                   'interaction': interactions,
                                   'change_prop': change_props,
                                   'scenario_run': scenario_runs,
                                   'y': ys,
                                   'x': xs
                               },
                               attrs=attrs)

    scenario_da.to_netcdf(dst_filepath, mode='w')
    logger.info("dumped scenario data array to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
