import json
import logging
import warnings

import click
import numpy as np
import pandas as pd
import xarray as xr

from lausanne_greening_scenarios import settings
from lausanne_greening_scenarios.scenarios import utils as scenario_utils


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
@click.option('--dst-t-dtype', default='float32')
def main(agglom_lulc_filepath, biophysical_table_filepath, station_t_filepath,
         ref_et_raster_filepath, calibrated_params_filepath, dst_filepath,
         shade_threshold, num_scenario_runs, change_prop_step, dst_t_dtype):
    logger = logging.getLogger(__name__)
    # disable InVEST's logging
    for module in ('natcap.invest.urban_cooling_model', 'natcap.invest.utils',
                   'pygeoprocessing.geoprocessing'):
        logging.getLogger(module).setLevel(logging.WARNING)
    # ignore all warnings
    warnings.filterwarnings('ignore')

    # 1. generate a data array with the scenario land use/land cover
    sg = scenario_utils.ScenarioGenerator(agglom_lulc_filepath,
                                          biophysical_table_filepath)

    scenario_runs = range(num_scenario_runs)
    # change_props = rn.uniform(size=num_scenario_samples)
    # if include_endpoints:
    #     # the first and last positions of the `change_props` array will be
    #     # set to 0 and 1 respectively
    #     change_props[0] = 0
    #     change_props[-1] = 1
    change_props = np.arange(0, 1 + change_prop_step, change_prop_step)
    scenario_lulc_da = sg.generate_scenario_lulc_da(change_props,
                                                    scenario_runs,
                                                    shade_threshold)
    num_scenarios = np.prod(scenario_lulc_da.shape[:-2])
    logger.info("generated %d scenario LULC arrays", num_scenarios)

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
    # rio_meta = sg.lulc_meta.copy()
    scenario_T_da = scenario_utils.simulate_scenario_T_da(
        scenario_lulc_da, biophysical_table_filepath, ref_et_raster_filepath,
        t_ref, uhi_max, ucm_params, dst_t_dtype)
    logger.info("simulated air temperature rasters for the %d scenarios",
                num_scenarios)

    # 3. dump the dataset into a file
    xr.Dataset(
        {
            'LULC': scenario_lulc_da,
            'T': scenario_T_da
        },
        attrs=dict(pyproj_srs=scenario_lulc_da.attrs['pyproj_srs'])).to_netcdf(
            dst_filepath, mode='w')
    logger.info("dumped scenario dataset to %s", dst_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
