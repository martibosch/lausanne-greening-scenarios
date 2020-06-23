import json
import logging
import tempfile
from os import path

import click
import dask
import invest_ucm_calibration as iuc
import numpy as np
import rasterio as rio
import xarray as xr
from dask import diagnostics
from rasterio import transform

from lausanne_greening_scenarios import settings
from lausanne_greening_scenarios.invest import utils as invest_utils
from lausanne_greening_scenarios.scenarios import utils as scenario_utils


@click.command()
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('t_da_filepath', type=click.Path(exists=True))
@click.argument('ref_et_da_filepath', type=click.Path(exists=True))
@click.argument('calibrated_params_filepath', type=click.Path(exists=True))
@click.argument('dst_filepath', type=click.Path())
@click.option('--shade-threshold', default=0.75)
@click.option('--num-scenario-runs', default=10)
@click.option('--change-prop-step', default=0.125)
@click.option('--include-endpoints/--no-endpoints', default=False)
@click.option('--interactions', is_flag=True)
@click.option('--dst-t-dtype', default='float32')
def main(agglom_lulc_filepath, biophysical_table_filepath, t_da_filepath,
         ref_et_da_filepath, calibrated_params_filepath, dst_filepath,
         shade_threshold, num_scenario_runs, change_prop_step,
         include_endpoints, interactions, dst_t_dtype):
    logger = logging.getLogger(__name__)

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
    # 2.1 select the date of maximum UHI magnitude
    t_da = xr.open_dataarray(t_da_filepath)
    max_uhi_date = t_da.isel(time=(t_da.max(dim=['x', 'y']) - t_da.min(
        dim=['x', 'y'])).argmax())['time'].dt.strftime('%Y-%m-%d').item()
    t_da = t_da.sel(time=max_uhi_date)
    t_ref = t_da.min().item()
    uhi_max = t_da.max().item() - t_ref

    # 2.2 dump the ref. ET raster of the selected date to a tmp file
    ref_et_da = xr.open_dataarray(ref_et_da_filepath).sel(time=max_uhi_date)
    tmp_dir = tempfile.mkdtemp()
    ref_et_raster_filepath = invest_utils.dump_ref_et_raster(
        ref_et_da, max_uhi_date, tmp_dir,
        invest_utils.get_da_rio_meta(ref_et_da))

    # 2.3 load the calibrated parameters of the UCM
    with open(calibrated_params_filepath) as src:
        ucm_params = json.load(src)

    # 2.4 execute (at scale) the model for each scenario LULC array
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
