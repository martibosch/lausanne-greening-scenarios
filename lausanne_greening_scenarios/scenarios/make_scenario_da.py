import logging

import click
import numpy as np
import xarray as xr
from rasterio import transform

from lausanne_greening_scenarios import settings
from lausanne_greening_scenarios.scenarios import utils as scenario_utils


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

    sg = scenario_utils.ScenarioGenerator(agglom_lulc_filepath,
                                          biophysical_table_filepath)

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
