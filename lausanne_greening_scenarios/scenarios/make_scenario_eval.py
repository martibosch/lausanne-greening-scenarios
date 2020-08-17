import itertools
import json
import logging
import tempfile
from os import path

import click
import invest_ucm_calibration as iuc
import numpy as np
import pandas as pd
import pylandstats as pls
import rasterio as rio
import xarray as xr
from affine import Affine
from tqdm import tqdm

from lausanne_greening_scenarios import settings
from lausanne_greening_scenarios.invest import utils as invest_utils

# register tqdm with pandas to be able to use `progress_apply`
tqdm.pandas()

HIGH_TREE_CLASS_VAL = 1
OTHER_CLASS_VAL = 2

# metrics = ('MPS', 'ED', 'MSI')
METRICS = ['area_mn', 'edge_density', 'shape_index_mn']


@click.command()
@click.argument('scenario_lulc_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('t_da_filepath', type=click.Path(exists=True))
@click.argument('ref_et_da_filepath', type=click.Path(exists=True))
@click.argument('calibrated_params_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
def main(scenario_lulc_filepath, biophysical_table_filepath, t_da_filepath,
         ref_et_da_filepath, calibrated_params_filepath, dst_filepath,
         shade_threshold):
    logger = logging.getLogger(__name__)

    # read scenario data
    scenario_lulc_da = xr.open_dataarray(scenario_lulc_filepath)
    scenario_dims = scenario_lulc_da.coords.dims[:3]
    res = scenario_lulc_da.attrs['transform'][0]
    nodata = scenario_lulc_da.attrs['nodata']
    change_props = scenario_lulc_da['change_prop'].values
    scenario_runs = scenario_lulc_da['scenario_run'].values
    num_scenario_runs = len(scenario_runs)
    rio_meta = scenario_lulc_da.attrs.copy()
    rio_meta['transform'] = Affine.from_gdal(*rio_meta['transform'])

    # read the biophysical table
    biophysical_df = pd.read_csv(biophysical_table_filepath)

    # read the calibrated parameters
    with open(calibrated_params_filepath) as src:
        model_params = json.load(src)

    # read the temperature data-array to get the hottest day, t_ref and uhi_max
    t_da = xr.open_dataarray(t_da_filepath)
    hottest_day = t_da.isel(time=t_da.groupby('time').max(
        dim=['x', 'y']).argmax())['time'].dt.strftime('%Y-%m-%d').item()
    # hottest_day = '2019-07-24'
    t_ref = t_da.sel(time=hottest_day).min(dim=['x', 'y']).item()
    uhi_max = t_da.sel(time=hottest_day).max(dim=['x', 'y']).item() - t_ref

    # read the ref. evapotranspiration for the hottest day and dump a
    # temporary raster to use it to run the urban cooling model for scenario
    # evaluation
    ref_et_da = xr.open_dataarray(ref_et_da_filepath).sel(time=hottest_day)
    with tempfile.TemporaryDirectory() as ref_et_raster_dir:
        ref_et_raster_filepath = invest_utils.dump_ref_et_raster(
            ref_et_da, hottest_day, ref_et_raster_dir,
            invest_utils.get_da_rio_meta(ref_et_da))

        # define the functions so that the fixed arguments are curried into
        # them, except for `metrics`
        def compute_metrics(row, metrics):
            lulc_arr = scenario_lulc_da.sel({
                scenario_dim: row[scenario_dim]
                for scenario_dim in scenario_dims
            }).values

            # compute metrics
            landscape_arr = np.full_like(lulc_arr, nodata)
            landscape_arr[lulc_arr != nodata] = OTHER_CLASS_VAL
            landscape_arr[np.isin(
                lulc_arr,
                biophysical_df[biophysical_df['shade'] >= shade_threshold]
                ['lucode'])] = HIGH_TREE_CLASS_VAL
            ls = pls.Landscape(landscape_arr, (res, res), nodata)
            # return [
            #     getattr(ls, metric)(
            #         high_tree_class_val) for metric in metrics
            # ]
            result_dict = {
                metric: getattr(ls, metric)(HIGH_TREE_CLASS_VAL)
                for metric in metrics
            }

            # average simulated temperature
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
                                             extra_ucm_args=model_params)
                result_dict['T_avg'] = ucm_wrapper.predict_t_da().mean(
                    skipna=True).item()

            return pd.Series(result_dict)

        def compute_endpoint_metrics(row, metrics):
            # interaction could be anything, since we are changing none or all
            # the changeable pixels
            _row = dict(change_prop=row['change_prop'],
                        interaction='cluster',
                        scenario_run=0)
            return compute_metrics(_row, metrics)

        # prepare the dataframe of metrics (except PLAND) for each scenario,
        # except for the endpoints (change proportion of 0 and 1, since these
        # will be the same for the cluster/scatter interactions)
        scenario_df = pd.DataFrame(
            list(
                itertools.product(scenario_lulc_da['interaction'].values,
                                  change_props[1:-1], scenario_runs)),
            columns=['interaction', 'change_prop', 'scenario_run'])
        scenario_eval_cols = METRICS + ['T_avg']
        for scenario_eval_col in scenario_eval_cols:
            scenario_df[scenario_eval_col] = np.nan
        # now fill it by computing the landscape metrics
        scenario_df[scenario_eval_cols] = scenario_df.progress_apply(
            compute_metrics, axis=1, args=(METRICS, ))

        # compute the metrics (including PLAND) for the endpoints
        endpoints = [change_props[0], change_props[-1]]
        endpoint_scenario_df = pd.DataFrame(endpoints, columns=['change_prop'])
        endpoint_metrics = ['proportion_of_landscape'] + METRICS
        for metric in endpoint_metrics:
            endpoint_scenario_df[metric] = np.nan
        endpoint_scenario_df[
            endpoint_metrics] = endpoint_scenario_df.progress_apply(
                compute_endpoint_metrics, axis=1, args=(endpoint_metrics, ))

    # the tempfile indent for the ref. evapotranspiration raster ends here
    endpoint_scenario_df = pd.concat(
        [endpoint_scenario_df for i in range(2 * num_scenario_runs)],
        ignore_index=True)
    endpoint_scenario_df['interaction'] = [
        'cluster'
    ] * 2 * num_scenario_runs + ['scatter'] * 2 * num_scenario_runs
    endpoint_scenario_df = endpoint_scenario_df.sort_values(
        ['change_prop', 'interaction'])
    endpoint_scenario_df['scenario_run'] = 4 * list(range(num_scenario_runs))

    # put it all together in a single data frame
    scenario_df = pd.concat([scenario_df, endpoint_scenario_df],
                            ignore_index=True)

    # get the PLAND by interpolating it from 'change_prop'
    pland_ser = scenario_df['proportion_of_landscape'].groupby(
        scenario_df['change_prop']).agg(lambda x: x.iloc[0]).interpolate(
            method='index')
    for (_, group_df), pland in zip(scenario_df.groupby('change_prop'),
                                    pland_ser):
        scenario_df.loc[group_df.index, 'proportion_of_landscape'] = pland

    # dump it
    scenario_df.to_csv(dst_filepath, index=False)
    logger.info("dumped scenario metrics data frame to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
