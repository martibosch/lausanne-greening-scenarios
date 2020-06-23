import tempfile
from os import path

import invest_ucm_calibration as iuc
import numpy as np
import pandas as pd
import rasterio as rio
from affine import Affine
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

    def generate_lulc_arr(self,
                          shade_threshold,
                          change_prop,
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
            change_df.loc[np.flatnonzero(self.lulc_arr == lucode),
                          'next_code'] = self.next_code_dict[lucode]

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


class ScenarioWrapper:
    def __init__(
        self,
        scenario_lulc_da,
        t_obs_da,
        biophysical_table_filepath,
        ref_et_raster_filepath,
        ucm_params,
        # agglom_ldf=None,
        # statpop_year=19
    ):
        self.scenario_lulc_da = scenario_lulc_da
        self.scenario_dims = scenario_lulc_da.coords.dims[:-2]
        rio_meta = scenario_lulc_da.attrs.copy()
        rio_meta['transform'] = Affine.from_gdal(*rio_meta['transform'])
        self.rio_meta = rio_meta

        self.t_obs_da = t_obs_da
        self.t_ref = t_obs_da.min(dim=['x', 'y']).item()
        self.uhi_max = t_obs_da.max(dim=['x', 'y']).item() - self.t_ref

        self.biophysical_table_filepath = biophysical_table_filepath
        self.ref_et_raster_filepath = ref_et_raster_filepath

        self.ucm_params = ucm_params

        # if agglom_ldf is not None:
        #     vulnerable_columns = [
        #         f'B{statpop_year}B{sex}{age_group:02}' for sex in ['M', 'W']
        #         for age_group in list(range(1, 4)) + list(range(13, 20))
        #     ]
        #     agglom_ldf['vulnerable'] = agglom_ldf[vulnerable_columns].sum(
        #         axis=1)
        #     with tempfile.TemporaryDirectory() as tmp_dir:
        #         tmp_filepath = path.join(tmp_dir, 'vulnerable.tif')
        #         agglom_ldf.to_geotiff(tmp_filepath, 'vulnerable')
        #         vuln_da = salem.open_xr_dataset(tmp_filepath)['data']
        #     agglom_ldf.drop('vulnerable', axis=1)
        #     self.vuln_da = vuln_da

    # define the functions so that the fixed arguments are curried into them,
    # except for `metrics`
    def compute_t_da(self, scenario_dims_map):
        # landscape_arr = sg.generate_landscape_arr(shade_threshold,
        #                                           row['change_prop'],
        #                                           interaction=row['interaction'])
        lulc_arr = self.scenario_lulc_da.sel({
            scenario_dim: scenario_dims_map[scenario_dim]
            for scenario_dim in self.scenario_dims
        }).values

        with tempfile.TemporaryDirectory() as tmp_dir:
            lulc_raster_filepath = path.join(tmp_dir, 'lulc.tif')
            with rio.open(lulc_raster_filepath, 'w', **self.rio_meta) as dst:
                dst.write(lulc_arr, 1)

            ucm_wrapper = iuc.UCMWrapper(lulc_raster_filepath,
                                         self.biophysical_table_filepath,
                                         'factors',
                                         self.ref_et_raster_filepath,
                                         self.t_ref,
                                         self.uhi_max,
                                         extra_ucm_args=self.ucm_params)
            return ucm_wrapper.predict_t_da()

    def compute_t_avg(self, scenario_dims_map):
        return self.compute_t_da(scenario_dims_map).mean(skipna=True).item()

    # def compute_vuln_index(self, scenario_dims_map):
    #     t_da = suhi.align_ds(self.compute_t_da(scenario_dims_map),
    #                          self.vuln_da)
    #     t_min = t_da.min()
    #     vuln_index_da = self.vuln_da * (
    #         t_da - t_min) / (t_da.max() - t_min)
    #     return vuln_index_da.mean(skipna=True)

    def compute_t_diff_da(self, scenario_dims_map):
        return self.compute_t_da(scenario_dims_map) - self.t_obs_da
