import tempfile
from os import path

import dask
import invest_ucm_calibration as iuc
import numpy as np
import pandas as pd
import rasterio as rio
import xarray as xr
from dask import diagnostics
from rasterio import transform
from scipy import ndimage as ndi

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
        inner_road_mask = ndi.binary_erosion(
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

        # generate a dataframe of
        change_df = pd.DataFrame(index=np.flatnonzero(lulc_arr))
        # get list of pixels that can be changed and their next code
        for lucode in next_code_dict:
            change_df.loc[np.flatnonzero(lulc_arr == lucode),
                          'next_code'] = next_code_dict[lucode]
        # drop nan values
        change_df = change_df.dropna()
        # exclude the inner road pixels
        change_df = change_df.drop(np.flatnonzero(inner_road_mask),
                                   errors='ignore')
        change_df['next_code'] = change_df['next_code'].astype(np.int32)

        # prepare grid metadata for xarray
        rows = np.arange(lulc_meta['height'])
        cols = np.arange(lulc_meta['width'])
        lulc_transform = lulc_meta['transform']
        xs, _ = transform.xy(lulc_transform, cols, cols)
        _, ys = transform.xy(lulc_transform, rows, rows)
        coords = {'y': ys, 'x': xs}

        # save the LULC raster, biophysical table and the change data frame as
        # attributes so that they can be used in the methods below
        self.lulc_arr = lulc_arr
        self.lulc_meta = lulc_meta
        self.lulc_bounds = lulc_bounds
        self.biophysical_df = biophysical_df
        self.lulc_col = lulc_col
        # use this in `generate_lulc_arr`
        # self.change_df_idx = np.flatnonzero(self.lulc_arr)
        self.change_df = change_df
        self.coords = coords

    def generate_lulc_arr(self,
                          shade_threshold,
                          change_prop,
                          interaction='random'):
        if change_prop == 0:
            return self.lulc_arr.copy()
        elif change_prop == 1:
            pixels_to_change_df = self.change_df
        else:
            # how many pixels will be changed
            num_to_change = int(len(self.change_df) * change_prop)

            if interaction == 'random':
                # just change pixels randomly
                pixels_to_change_df = self.change_df.sample(num_to_change)
            else:
                # convolution
                conv_result = ndi.convolve(
                    np.isin(
                        self.lulc_arr, self.biophysical_df[
                            self.biophysical_df['shade'] >= shade_threshold][
                                self.lulc_col]).astype(np.int32),
                    KERNEL_MOORE).flatten()[self.change_df.index]

                # decide which pixels will be changed (depending on desired
                # interaction between high tree cover pixels)
                # by default, `argsort` sorts in ascending order, which in our
                # case corresponds to prioritizing scattering the pixels
                sorted_idx = conv_result.argsort()
                if interaction == 'scatter':
                    comparison_op = np.less
                else:  # 'cluster'
                    sorted_idx = sorted_idx[::-1]
                    comparison_op = np.greater

                conv_result_threshold = conv_result[sorted_idx[num_to_change -
                                                               1]]
                # print(interaction)
                # print(conv_result_threshold)
                pixels_to_change_df = self.change_df[comparison_op(
                    conv_result, conv_result_threshold)]
                # print(num_to_change, len(pixels_to_change_df))
                pixels_to_change_df = pd.concat([
                    pixels_to_change_df,
                    self.change_df[conv_result == conv_result_threshold].
                    sample(num_to_change - len(pixels_to_change_df))
                ])

        # now build the new LULC array and change the pixels
        new_lulc_arr = self.lulc_arr.copy()
        for next_code, next_code_df in pixels_to_change_df.groupby(
                'next_code'):
            new_lulc_arr.ravel()[next_code_df.index] = next_code

        return new_lulc_arr

    def generate_scenario_lulc_da(self,
                                  change_props,
                                  scenario_runs,
                                  shade_threshold,
                                  interactions=None):
        if interactions is None:
            interactions = ['random', 'cluster', 'scatter']

        # prepare the xarray data array
        coords = {
            'interaction': interactions,
            'change_prop': change_props,
            'scenario_run': scenario_runs,
            **self.coords
        }
        dims = ['interaction', 'change_prop', 'scenario_run', 'y', 'x']
        scenario_lulc_da = xr.DataArray(
            dims=dims,
            coords=coords,
            attrs=dict(nodata=self.lulc_meta['nodata'],
                       pyproj_srs=f'epsg:{self.lulc_meta["crs"].to_epsg()}'))

        # generate the arrays
        if change_props[0] == 0:
            # no pixels are changed, so we keep the starting LULC array and
            # repeat it for all scenario runs and interactions
            scenario_lulc_da.loc[dict(change_prop=0)] = np.array(
                [[self.lulc_arr for scenario_run in scenario_runs]
                 for interaction in interactions])
            change_props = change_props[1:]
        if change_props[-1] == 1:
            # we change all the candidate pixels only once and repeat the
            # resulting LULC array for all scenario runs and interactions
            end_lulc_arr = self.generate_lulc_arr(shade_threshold, 1)
            scenario_lulc_da.loc[dict(change_prop=1)] = np.array(
                [[end_lulc_arr for scenario_run in scenario_runs]
                 for interaction in interactions])
            change_props = change_props[:-1]
        scenario_lulc_da.loc[dict(change_prop=change_props)] = np.array([[[
            self.generate_lulc_arr(shade_threshold,
                                   change_prop,
                                   interaction=interaction)
            for scenario_run in scenario_runs
        ] for change_prop in change_props] for interaction in interactions])

        # return the data array
        return scenario_lulc_da


def simulate_scenario_T_da(scenario_lulc_da,
                           biophysical_table_filepath,
                           ref_et_raster_filepath,
                           t_ref,
                           uhi_max,
                           ucm_params,
                           dst_t_dtype,
                           rio_meta=None,
                           cc_method='factors'):
    if rio_meta is None:
        x = scenario_lulc_da['x'].values
        y = scenario_lulc_da['y'].values
        west = x[0]
        north = y[0]
        # TODO: does the method to get the transform work for all grids, i.e.,
        # regardless of whether the origin is in the upper left or lower left?
        rio_meta = dict(driver='GTiff',
                        dtype=scenario_lulc_da.dtype,
                        nodata=scenario_lulc_da.attrs['nodata'],
                        width=len(x),
                        height=len(y),
                        count=1,
                        crs=scenario_lulc_da.attrs['pyproj_srs'],
                        transform=transform.from_origin(
                            west, north, x[1] - west, north - y[1]))

    # define the function here so that the fixed arguments are curried
    def _t_from_lulc(lulc_arr):
        with tempfile.TemporaryDirectory() as tmp_dir:
            lulc_raster_filepath = path.join(tmp_dir, 'lulc.tif')
            with rio.open(lulc_raster_filepath, 'w', **rio_meta) as dst:
                dst.write(lulc_arr, 1)

            ucm_wrapper = iuc.UCMWrapper(lulc_raster_filepath,
                                         biophysical_table_filepath,
                                         cc_method,
                                         ref_et_raster_filepath,
                                         t_ref,
                                         uhi_max,
                                         extra_ucm_args=ucm_params)
            return ucm_wrapper.predict_t_arr(0)

    scenario_T_da = xr.DataArray(
        dims=scenario_lulc_da.dims,
        coords=scenario_lulc_da.coords,
        attrs=dict(nodata=np.nan,
                   pyproj_srs=scenario_lulc_da.attrs['pyproj_srs']))

    change_props = scenario_T_da['change_prop'].values
    scenario_runs = scenario_T_da['scenario_run']
    interactions = scenario_T_da['interaction']
    if change_props[0] == 0:
        # simulate once and repeat it for all scenario runs and interactions
        start_T_arr = _t_from_lulc(
            scenario_lulc_da.sel(change_prop=0).isel(interaction=0,
                                                     scenario_run=0))
        scenario_T_da.loc[dict(change_prop=0)] = np.array(
            [[start_T_arr for scenario_run in scenario_runs]
             for interaction in interactions])
        change_props = change_props[1:]
    if change_props[-1] == 1:
        # simulate once and repeat it for all scenario runs and interactions
        end_T_arr = _t_from_lulc(
            scenario_lulc_da.sel(change_prop=1).isel(interaction=0,
                                                     scenario_run=0))
        scenario_T_da.loc[dict(change_prop=1)] = np.array(
            [[end_T_arr for scenario_run in scenario_runs]
             for interaction in interactions])
        change_props = change_props[:-1]
    # TODO: use a set difference to get all dimensions but ('x', 'y')?
    scenario_dims = scenario_lulc_da.dims[:-2]
    stacked_da = scenario_lulc_da.sel(change_prop=change_props).stack(
        scenario=scenario_dims).transpose('scenario', 'y', 'x')
    with diagnostics.ProgressBar():
        scenario_T_da.loc[dict(change_prop=change_props)] = xr.DataArray(
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
    return scenario_T_da.where(scenario_T_da > -273.15, np.nan)
