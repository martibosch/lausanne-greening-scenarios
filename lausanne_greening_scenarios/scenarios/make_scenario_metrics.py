import itertools
import logging

import click
import numpy as np
import pandas as pd
import pylandstats as pls
import salem  # noqa: F401
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
@click.argument('scenario_ds_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
def main(scenario_ds_filepath, biophysical_table_filepath, dst_filepath,
         shade_threshold):
    logger = logging.getLogger(__name__)

    scenario_ds = xr.open_dataset(scenario_ds_filepath)
    scenario_lulc_da = scenario_ds['LULC']

    # scenario_dims = scenario_lulc_da.coords.dims[:2]
    scenario_dims = scenario_lulc_da.coords.dims[:-2]
    res = scenario_ds.salem.grid.dx
    nodata = scenario_lulc_da.attrs['nodata']
    interactions = scenario_lulc_da['interaction'].values
    change_props = scenario_lulc_da['change_prop'].values.copy()
    change_props.sort()
    scenario_runs = scenario_lulc_da['scenario_run'].values

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
        _row = dict(interaction='cluster',
                    change_prop=row['change_prop'],
                    scenario_run=0)
        return compute_metrics(_row, metrics)

    # prepare the dataframe of metrics (except PLAND) for each scenario,
    # except for the endpoints (change proportion of 0 and 1, since these will
    # be the same for the cluster/scatter interactions)
    scenario_df = pd.DataFrame(
        list(
            itertools.product(scenario_lulc_da['interaction'].values,
                              change_props[1:-1],
                              scenario_ds['scenario_run'].values)),
        columns=['interaction', 'change_prop', 'scenario_run'])
    scenario_df[METRICS] = np.nan
    # TODO: use dask here
    # now fill it by computing the landscape metrics
    scenario_df[METRICS] = scenario_df.progress_apply(compute_metrics,
                                                      axis=1,
                                                      args=(METRICS, ))

    # now compute the metrics (including PLAND) for the endpoints
    endpoint_metrics = ['proportion_of_landscape'] + METRICS
    endpoint_scenario_df = pd.DataFrame([0, 1], columns=['change_prop'])
    endpoint_scenario_df[
        endpoint_metrics] = endpoint_scenario_df.progress_apply(
            compute_endpoint_metrics, axis=1, args=(endpoint_metrics, ))
    # repeat the endpoint metrics accross `interactions` and `scenario_runs`
    # to have a consistent data frame structure with `scenario_df`
    num_interactions = len(interactions)
    num_scenario_runs = len(scenario_runs)
    endpoint_scenario_df = pd.concat(
        [endpoint_scenario_df] * num_interactions * num_scenario_runs,
        ignore_index=True).sort_values('change_prop')
    endpoint_scenario_df['interaction'] = np.tile(
        interactions,
        len(endpoint_scenario_df) // num_interactions)
    endpoint_scenario_df['scenario_run'] = np.tile(
        scenario_runs,
        len(endpoint_scenario_df) // num_scenario_runs)

    # put it all together in a single data frame
    scenario_df = pd.concat([scenario_df, endpoint_scenario_df],
                            ignore_index=True).sort_values('change_prop')

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
