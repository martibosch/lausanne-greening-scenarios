import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import pingouin as pg
from pysal.lib import weights
from pysal.model import spreg
from rasterio import transform
from scipy import stats
from sklearn import preprocessing

from lausanne_greening_scenarios import utils


class RegressionWrapper:
    def __init__(self, lulc_raster_filepath, biophysical_table_filepath,
                 metrics, tree_class):
        ls, ls_meta = utils.get_reclassif_landscape(
            lulc_raster_filepath, biophysical_table_filepath)

        self.ls = ls
        self.ls_meta = ls_meta

        self.metrics = metrics
        self.tree_class = tree_class

        self._zga_dict = {}

        self.feature_cols = metrics + ['dist_center']
        self._regr_df_index_names = ['scale', 'zone']
        self._regr_features_df = pd.DataFrame(index=pd.MultiIndex(
            levels=[[], []], codes=[[], []], names=self._regr_df_index_names),
                                              columns=self.feature_cols)

    def _get_zga(self, scale):
        try:
            return self._zga_dict[scale]
        except KeyError:
            zone_pixel_len = int(scale / self.ls_meta['transform'].a)
            zga = utils.get_zonal_grid_analysis(
                self.ls,
                self.ls_meta,
                zone_pixel_width=zone_pixel_len,
                zone_pixel_height=zone_pixel_len)
            self._zga_dict[scale] = zga
            return zga

    def _get_regr_features_df(self, scale):
        try:
            return self._regr_features_df.loc[scale]
        except KeyError:
            zga = self._get_zga(scale)
            metrics_df = zga.compute_class_metrics_df(
                metrics=self.metrics, classes=[self.tree_class])
            regr_features_df = metrics_df.iloc[
                metrics_df.index.get_level_values(
                    'class_val') == self.tree_class].reset_index(drop=True)

            # distance to the city center
            crs = zga.landscape_meta['crs']
            p_center = gpd.GeoSeries(
                [utils.BASE_MASK], crs=utils.BASE_MASK_CRS).to_crs(crs).iloc[0]
            regr_features_df['dist_center'] = gpd.GeoSeries(
                gpd.points_from_xy(
                    *transform.xy(zga.landscape_meta['transform'],
                                  *np.array(zga.zones).transpose())),
                crs=crs).apply(lambda p: p_center.distance(p))

            # TODO: elevation?

            # add a new level to the index (to have a multiindex)
            regr_features_df = pd.concat([regr_features_df],
                                         keys=[scale],
                                         names=self._regr_df_index_names)
            self._regr_features_df = pd.concat(
                [self._regr_features_df, regr_features_df])

            return regr_features_df

    def _get_regr_target_arr(self, scale, t_arr):
        return np.nanmean(utils.get_zonal_grid_t_arrs(t_arr,
                                                      self._get_zga(scale)),
                          axis=(1, 2))

    def get_multiscale_regr_results_df(self,
                                       scales,
                                       t_arr,
                                       w_threshold=2,
                                       standardize_features=True,
                                       spreg_model=spreg.ML_Error):
        if spreg_model == spreg.ML_Error:
            stat_attr = 'z_stat'
            r2_attr = 'pr2'
            moran = False
            spreg_model_kws = {}
            features_slice = slice(1, -1)
        elif spreg_model == spreg.OLS:
            stat_attr = 't_stat'
            r2_attr = 'r2'
            moran = True
            spreg_model_kws = dict(spat_diag=True, moran=True)
            features_slice = slice(1, None)

        def loop_body(scale):
            # use `copy` to avoid reference alias issues with
            # `self._regr_features_df`
            regr_df = self._get_regr_features_df(scale).copy()
            regr_df['T'] = self._get_regr_target_arr(scale, t_arr)
            zone_arr = np.array(
                self._get_zga(scale).zones)[~regr_df.isna().any(axis=1)]
            # drop rows with nan
            regr_df = regr_df.dropna()
            w = weights.DistanceBand.from_array(zone_arr, w_threshold)
            features_df = regr_df.drop('T', axis=1)
            X = features_df.values
            if standardize_features:
                X = preprocessing.StandardScaler().fit_transform(X)
            return spreg_model(regr_df['T'].values[:, None],
                               X,
                               w=w,
                               name_y='T',
                               name_x=list(features_df.columns),
                               **spreg_model_kws)

        lazy_results = []
        for scale in scales:
            lazy_results.append(dask.delayed(loop_body)(scale))
        ms = dask.compute(*lazy_results)
        regr_results_df = pd.DataFrame(index=scales)
        regr_results_df[list(map(lambda col: f'{col}_B',
                                 self.feature_cols))] = np.vstack(
                                     list(m.betas.flatten()
                                          for m in ms))[:, features_slice]
        regr_results_df[list(map(
            lambda col: f'{col}_p', self.feature_cols))] = np.vstack(
                list(
                    np.array(getattr(m, stat_attr))[features_slice, 1]
                    for m in ms))
        regr_results_df['R^2'] = list(getattr(m, r2_attr) for m in ms)
        if moran:
            regr_results_df['Moran'] = list(m.moran_res[0] for m in ms)
            regr_results_df['Moran_p'] = list(m.moran_res[2] for m in ms)
        regr_results_df['AIC'] = list(m.aic for m in ms)

        return regr_results_df

    def get_multiscale_corr_df(self, scales, t_arr):
        def loop_body(scale):
            scale_df = pd.DataFrame()
            regr_df = self._get_regr_features_df(scale).copy()
            regr_df['T'] = self._get_regr_target_arr(scale, t_arr)
            # drop rows with nan
            regr_df = regr_df.dropna()
            for x_col in regr_df.drop('T', axis=1).columns:
                scale_df.loc[scale,
                             [f'{x_col}_r', f'{x_col}_p']] = stats.pearsonr(
                                 regr_df[x_col], regr_df['T'])

            return scale_df

        lazy_results = []
        for scale in scales:
            lazy_results.append(dask.delayed(loop_body)(scale))
        scale_dfs = dask.compute(*lazy_results)

        return pd.concat(scale_dfs)

    def get_multiscale_partial_corr_df(self,
                                       scales,
                                       t_arr,
                                       comp_metrics=None,
                                       conf_metrics=None):
        if comp_metrics is None:
            comp_metrics = ['proportion_of_landscape']

        if conf_metrics is None:
            conf_metrics = list(set(self.metrics).difference(comp_metrics))

        other_features = list(
            set(self.feature_cols).difference(set(comp_metrics +
                                                  conf_metrics)))

        def loop_body(scale):
            scale_df = pd.DataFrame()
            regr_df = self._get_regr_features_df(scale).copy()
            regr_df['T'] = self._get_regr_target_arr(scale, t_arr)
            for x_col in comp_metrics:
                col_df = pg.partial_corr(data=regr_df,
                                         y='T',
                                         x=x_col,
                                         covar=conf_metrics)
                scale_df.loc[scale, f'{x_col}_r'] = col_df['r'].values
                scale_df.loc[scale, f'{x_col}_p'] = col_df['p-val'].values
            for x_col in conf_metrics + other_features:
                col_df = pg.partial_corr(data=regr_df,
                                         y='T',
                                         x=x_col,
                                         covar=comp_metrics)
                scale_df.loc[scale, f'{x_col}_r'] = col_df['r'].values
                scale_df.loc[scale, f'{x_col}_p'] = col_df['p-val'].values

            return scale_df

        lazy_results = []
        for scale in scales:
            lazy_results.append(dask.delayed(loop_body)(scale))
        scale_dfs = dask.compute(*lazy_results)

        return pd.concat(scale_dfs)
