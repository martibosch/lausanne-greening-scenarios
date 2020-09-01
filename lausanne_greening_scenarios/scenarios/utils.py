import tempfile
from os import path

import invest_ucm_calibration as iuc
import rasterio as rio
from affine import Affine


class ScenarioWrapper:
    def __init__(self, scenario_lulc_da, biophysical_table_filepath,
                 ref_et_raster_filepath, t_ref, uhi_max, ucm_params):
        self.scenario_lulc_da = scenario_lulc_da
        self.scenario_dims = scenario_lulc_da.coords.dims[:-2]
        rio_meta = scenario_lulc_da.attrs.copy()
        rio_meta['transform'] = Affine.from_gdal(*rio_meta['transform'])
        self.rio_meta = rio_meta

        self.biophysical_table_filepath = biophysical_table_filepath
        self.ref_et_raster_filepath = ref_et_raster_filepath

        self.t_ref = t_ref
        self.uhi_max = uhi_max
        self.ucm_params = ucm_params

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
                                         extra_ucm_args=self.model_params)
            return ucm_wrapper.predict_t_da()
