import itertools
import logging

import click
import numpy as np
import pandas as pd
import pylandstats as pls
import xarray as xr
from tqdm import tqdm

from lausanne_greening_scenarios import settings

# register tqdm with pandas to be able to use `progress_apply`
tqdm.pandas()

HIGH_TREE_CLASS_VAL = 1
OTHER_CLASS_VAL = 2

# metrics = ('MPS', 'ED', 'MSI')
METRICS = ['area_mn', 'edge_density', 'shape_index_mn']


@click.command()
@click.argument('scenario_lulc_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
def main(scenario_lulc_filepath, biophysical_table_filepath, dst_filepath,
         shade_threshold):
    logger = logging.getLogger(__name__)

    scenario_lulc_da = xr.open_dataarray(scenario_lulc_filepath)

    scenario_dims = scenario_lulc_da.coords.dims[:3]
    res = scenario_lulc_da.attrs['transform'][0]
    nodata = scenario_lulc_da.attrs['nodata']
    change_props = scenario_lulc_da['change_prop'].values
    scenario_runs = scenario_lulc_da['scenario_run'].values
    num_scenario_runs = len(scenario_runs)

    biophysical_df = pd.read_csv(biophysical_table_filepath)

    # define the functions so that the fixed arguments are curried into them,
    # except for `metrics`
    def compute_metrics(row, metrics):
        # landscape_arr = sg.generate_landscape_arr(shade_threshold,
        #                                           row['change_prop'],
        #                                           interaction=row['interaction'])
        lulc_arr = scenario_lulc_da.sel({
            scenario_dim: row[scenario_dim]
            for scenario_dim in scenario_dims
        }).values
        landscape_arr = np.full_like(lulc_arr, nodata)
        landscape_arr[lulc_arr != nodata] = OTHER_CLASS_VAL
        landscape_arr[np.isin(
            lulc_arr,
            biophysical_df[biophysical_df['shade'] >= shade_threshold]
            ['lucode'])] = HIGH_TREE_CLASS_VAL
        ls = pls.Landscape(landscape_arr, (res, res), nodata)
        # return [
        #     getattr(ls, metric)(high_tree_class_val) for metric in metrics]
        return pd.Series({
            metric: getattr(ls, metric)(HIGH_TREE_CLASS_VAL)
            for metric in metrics
        })

    def compute_endpoint_metrics(row, metrics):
        # interaction could be anything, since we are changing none or all the
        # changeable pixels
        _row = dict(change_prop=row['change_prop'],
                    interaction='cluster',
                    scenario_run=0)
        return compute_metrics(_row, metrics)

    # prepare the dataframe of metrics (except PLAND) for each scenario,
    # except for the endpoints (change proportion of 0 and 1, since these will
    # be the same for the cluster/scatter interactions)
    scenario_df = pd.DataFrame(
        list(
            itertools.product(scenario_lulc_da['interaction'].values,
                              change_props[1:-1], scenario_runs)),
        columns=['interaction', 'change_prop', 'scenario_run'])
    for metric in METRICS:
        scenario_df[metric] = np.nan
    # now fill it by computing the landscape metrics
    scenario_df[METRICS] = scenario_df.progress_apply(compute_metrics,
                                                      axis=1,
                                                      args=(METRICS, ))

    # compute the metrics (including PLAND) for the endpoints
    endpoints = [change_props[0], change_props[-1]]
    endpoint_scenario_df = pd.DataFrame(endpoints, columns=['change_prop'])
    endpoint_metrics = ['proportion_of_landscape'] + METRICS
    for metric in endpoint_metrics:
        endpoint_scenario_df[metric] = np.nan
    endpoint_scenario_df[
        endpoint_metrics] = endpoint_scenario_df.progress_apply(
            compute_endpoint_metrics, axis=1, args=(endpoint_metrics, ))
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
