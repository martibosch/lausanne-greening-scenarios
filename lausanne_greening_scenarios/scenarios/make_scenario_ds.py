import json
import logging
import tempfile
from os import path

import click
import dask
import invest_ucm_calibration as iuc
import numpy as np
import pandas as pd
import rasterio as rio
import xarray as xr
from dask import diagnostics
from rasterio import transform
from scipy import ndimage as ndi

from lausanne_greening_scenarios import settings

ORIG_LULC_CODES = [
    0,  # building
    1,  # road
    2,  # sidewalk
    3,  # traffic island
    7,  # other impervious
    11,  # garden
]
ROAD_CODE = 1

KERNEL_MOORE = ndi.generate_binary_structure(2, 2)


class ScenarioGenerator:
    def __init__(self,
                 agglom_lulc_filepath,
                 biophysical_table_filepath,
                 orig_lulc_col='orig_lucode',
                 lulc_col='lucode'):
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
                lulc_arr, biophysical_df[biophysical_df[orig_lulc_col] ==
                                         ROAD_CODE][lulc_col]), KERNEL_MOORE)

        # increase the tree cover by changing the land cover code of
        # roads/paths, sidewalks, blocks and other impervious surfaces to the
        # equivalent code with greater tree cover
        next_code_dict = {}
        for orig_lulc_code in ORIG_LULC_CODES:
            shade_gb = biophysical_df[biophysical_df[orig_lulc_col] ==
                                      orig_lulc_code].groupby('shade')
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
                    next_lucode = eligible_lucode_df.iloc[-1][lulc_col]
                    for lucode in eligible_lucode_df.iloc[:-1][lulc_col]:
                        next_code_dict[lucode] = next_lucode
        self.next_code_dict = next_code_dict
        self.lulc_col = lulc_col

        # save the LULC raster and biophysical table as attributes
        self.lulc_arr = lulc_arr
        self.lulc_meta = lulc_meta
        self.lulc_bounds = lulc_bounds
        self.biophysical_df = biophysical_df
        # use this in `generate_lulc_arr`
        self.change_df_idx = np.flatnonzero(self.lulc_arr)

    def generate_lulc_arr(self, shade_threshold, change_prop,
                          interaction=None):
        if change_prop == 0:
            return self.lulc_arr.copy()

        # convolution
        conv_result = ndi.convolve(
            np.isin(
                self.lulc_arr,
                self.biophysical_df[self.biophysical_df['shade'] >=
                                    shade_threshold][self.lulc_col]).astype(
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
            # how many pixels will be changed
            num_to_change = int(len(change_df) * change_prop)

            if interaction is None:
                # just change pixels randomly
                pixels_to_change_df = change_df.sample(num_to_change)
            else:
                # decide which pixels will be changed (depending on desired
                # interaction between high tree cover pixels)
                if interaction == 'cluster':
                    ascending = False
                else:  # scatter
                    ascending = True

                change_df = change_df.sort_values('conv_result',
                                                  ascending=ascending)
                last_lucode = change_df.iloc[:num_to_change][
                    'conv_result'].iloc[-1]
                conv_result_ser = change_df['conv_result']
                if ascending:
                    pixels_to_change_df = change_df[
                        conv_result_ser < last_lucode]
                else:
                    pixels_to_change_df = change_df[
                        conv_result_ser > last_lucode]

                pixels_to_change_df = pd.concat([
                    pixels_to_change_df,
                    change_df[conv_result_ser == last_lucode].sample(
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
@click.argument('station_t_filepath', type=click.Path(exists=True))
@click.argument('ref_et_raster_filepath', type=click.Path(exists=True))
@click.argument('calibrated_params_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
@click.option('--num-scenario-runs', default=10)
@click.option('--change-prop-step', default=0.125)
@click.option('--include-endpoints/--no-endpoints', default=False)
@click.option('--interactions', is_flag=True)
@click.option('--dst-t-dtype', default='float32')
def main(agglom_lulc_filepath, biophysical_table_filepath, station_t_filepath,
         ref_et_raster_filepath, calibrated_params_filepath, dst_filepath,
         shade_threshold, num_scenario_runs, change_prop_step,
         include_endpoints, interactions, dst_t_dtype):
    logger = logging.getLogger(__name__)

    # 1. generate a data array with the scenario land use/land cover
    sg = ScenarioGenerator(agglom_lulc_filepath, biophysical_table_filepath)

    scenario_runs = range(num_scenario_runs)
    # change_props = rn.uniform(size=num_scenario_samples)
    # if include_endpoints:
    #     # the first and last positions of the `change_props` array will be
    #     # set to 0 and 1 respectively
    #     change_props[0] = 0
    #     change_props[-1] = 1
    change_props = np.arange(0, 1 + change_prop_step, change_prop_step)
    if not include_endpoints:
        change_props = change_props[1:-1]

    rows = np.arange(sg.lulc_meta['height'])
    cols = np.arange(sg.lulc_meta['width'])
    lulc_transform = sg.lulc_meta['transform']
    xs, _ = transform.xy(lulc_transform, cols, cols)
    _, ys = transform.xy(lulc_transform, rows, rows)
    # get the raster meta so that it can be encoded as netcdf attributes
    lulc_meta = sg.lulc_meta.copy()

    # prepare the dataset LULC arrays
    scenario_dims = ['change_prop', 'scenario_run']
    coords = {
        'change_prop': change_props,
        'scenario_run': scenario_runs,
        'y': ys,
        'x': xs
    }
    if interactions:
        _interactions = ['cluster', 'scatter']
        scenario_arr = np.array([[[
            sg.generate_lulc_arr(shade_threshold,
                                 change_prop,
                                 interaction=_interaction)
            for scenario_run in scenario_runs
        ] for change_prop in change_props] for _interaction in _interactions])
        scenario_dims = ['interaction'] + scenario_dims
        coords['interaction'] = _interactions
    else:
        scenario_arr = np.array([[
            sg.generate_lulc_arr(shade_threshold, change_prop)
            for scenario_run in scenario_runs
        ] for change_prop in change_props])
    dims = scenario_dims + ['y', 'x']
    # scenario_da = xr.DataArray(scenario_arr,
    #                            dims=dims,
    #                            coords=coords,
    #                            attrs=attrs)
    # scenario_da.to_netcdf(dst_filepath, mode='w')
    # logger.info("dumped scenario data array to %s", dst_filepath)
    scenario_ds = xr.Dataset(
        {
            'LULC':
            xr.DataArray(scenario_arr,
                         dims=dims,
                         coords=coords,
                         attrs={'nodata': lulc_meta['nodata']})
        },
        attrs=dict(pyproj_srs=f'epsg:{lulc_meta["crs"].to_epsg()}'))
    logger.info("generated %d scenario LULC arrays",
                np.prod(scenario_arr.shape[:-2]))

    # 2. simulate the air temperature (of the day with maximum UHI magnitude)
    #    for each scenario LULC array
    # 2.1 get the reference temperature and the UHI magnitude
    station_t_df = pd.read_csv(station_t_filepath, index_col=0)
    t_ref = station_t_df.min()
    uhi_max = station_t_df.max() - t_ref

    # 2.2 load the calibrated parameters of the UCM
    with open(calibrated_params_filepath) as src:
        ucm_params = json.load(src)

    # 2.3 execute (at scale) the model for each scenario LULC array
    rio_meta = sg.lulc_meta.copy()

    # define the function here so that the fixed arguments are curried
    def _t_from_lulc(lulc_arr):
        with tempfile.TemporaryDirectory() as tmp_dir:
            lulc_raster_filepath = path.join(tmp_dir, 'lulc.tif')
            with rio.open(lulc_raster_filepath, 'w', **rio_meta) as dst:
                dst.write(lulc_arr, 1)

            ucm_wrapper = iuc.UCMWrapper(lulc_raster_filepath,
                                         biophysical_table_filepath,
                                         'factors',
                                         ref_et_raster_filepath,
                                         t_ref,
                                         uhi_max,
                                         extra_ucm_args=ucm_params)
            return ucm_wrapper.predict_t_arr(0)

    stacked_da = scenario_ds['LULC'].stack(scenario=scenario_dims).transpose(
        'scenario', 'y', 'x')
    with diagnostics.ProgressBar():
        scenario_ds['T'] = xr.DataArray(
            np.array(
                dask.compute(*[
                    dask.delayed(_t_from_lulc)(scenario_lulc_da)
                    for scenario_lulc_da in stacked_da
                ],
                             scheduler='processes')).astype(dst_t_dtype),
            dims=stacked_da.dims,
            coords={dim: stacked_da.coords[dim]
                    for dim in stacked_da.dims},
            attrs=dict(dtype=dst_t_dtype)).unstack(dim='scenario').transpose(
                *scenario_dims, 'y', 'x')
    # replace nodata values - UCM/InVEST uses minus infinity, so we can use
    # temperatures lower than the absolute zero as a reference threshold which
    # (physically) makes sense
    scenario_ds['T'] = scenario_ds['T'].where(scenario_ds['T'] > -273.15,
                                              np.nan)
    logger.info("simulated air temperature rasters for the %d scenarios",
                len(scenario_arr))

    # 3. dump the dataset into a file
    scenario_ds.to_netcdf(dst_filepath, mode='w')
    logger.info("dumped scenario dataset to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
