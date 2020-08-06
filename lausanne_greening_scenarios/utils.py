import numpy as np
import pandas as pd
import pylandstats as pls
import rasterio as rio
import statsmodels.api as sm
import xarray as xr
from numpy.lib import stride_tricks
from shapely import geometry

NODATA_CLASS = 0
TREE_CLASS = 1
GREEN_CLASS = 2
IMPERVIOUS_CLASS = 3
HIGH_INTENSITY_CLASS = 4

LABEL_DICT = {
    TREE_CLASS: 'tree',
    GREEN_CLASS: 'green',
    IMPERVIOUS_CLASS: 'impervious',
    HIGH_INTENSITY_CLASS: 'high-intensity'
}

BASE_MASK = geometry.Point(6.6327025, 46.5218269)
BASE_MASK_CRS = 'epsg:4326'
BUFFER_DISTS = np.arange(2000, 16000, 2000)


def get_reclassif_landscape(lulc_raster_filepath, biophysical_table_filepath):
    # 1. read the data inputs
    with rio.open(lulc_raster_filepath) as src:
        lulc_arr = src.read(1)
        lulc_res = src.res
        lulc_meta = src.meta
    biophysical_df = pd.read_csv(biophysical_table_filepath)
    # 2. reclassify the LULC according to tree cover/impervious surfaces
    reclassif_arr = np.full_like(lulc_arr, NODATA_CLASS)
    for criterion, class_val in zip([
            biophysical_df['shade'] >= .75,
        (biophysical_df['green_area'] == 1) & (biophysical_df['shade'] < .75),
        (biophysical_df['green_area'] == 0) & (biophysical_df['shade'] < .75),
            biophysical_df['building_intensity'] >= .75
    ], [TREE_CLASS, IMPERVIOUS_CLASS, HIGH_INTENSITY_CLASS]):
        reclassif_arr[np.isin(lulc_arr,
                              biophysical_df[criterion]['lucode'])] = class_val

    return pls.Landscape(reclassif_arr, res=lulc_res,
                         lulc_nodata=NODATA_CLASS), lulc_meta


def get_buffer_analysis(landscape,
                        landscape_meta,
                        base_mask=None,
                        base_mask_crs=None,
                        buffer_dists=None,
                        buffer_rings=True):
    # process the kwargs
    if base_mask is None:
        base_mask = BASE_MASK
    if base_mask_crs is None:
        base_mask_crs = BASE_MASK_CRS
    if buffer_dists is None:
        buffer_dists = BUFFER_DISTS

    # buffer analysis of total area of each reclassified LULC class
    return pls.BufferAnalysis(landscape,
                              base_mask,
                              buffer_dists=buffer_dists,
                              buffer_rings=buffer_rings,
                              base_mask_crs='epsg:4326',
                              landscape_crs=landscape_meta['crs'],
                              landscape_transform=landscape_meta['transform'])


def _plot_class_cover(ba, ax):
    # class cover plot
    # 1. buffer analysis of total area of each reclassified LULC class
    total_area_df = ba.compute_class_metrics_df(
        metrics=['proportion_of_landscape']).reset_index(level=1)
    # 2. plot it
    for class_val, class_df in total_area_df.groupby(total_area_df.index):
        class_df.plot(x='buffer_dists',
                      y='proportion_of_landscape',
                      style='--o',
                      label=LABEL_DICT[class_val],
                      ax=ax)
    ax.set_xlabel('dist [m]')
    ax.set_ylabel('%')
    ax.legend()

    return ax


def get_buffer_composition_df(ba):
    # 1. buffer analysis of total area of each reclassified LULC class
    total_area_df = ba.compute_class_metrics_df(
        metrics=['proportion_of_landscape']).reset_index(level=1)
    # 2. reshape the data frame
    return total_area_df.reset_index(level=0).pivot(
        index='buffer_dists',
        columns='class_val',
        values='proportion_of_landscape').reset_index().rename(
            columns=LABEL_DICT)


def plot_buffer_composition(ba, ax):
    # class cover plot
    buffer_composition_df = get_buffer_composition_df(ba)
    buffer_composition_df.plot(
        x='buffer_dists',
        y=buffer_composition_df.columns.drop('buffer_dists'),
        kind='bar',
        stacked=True,
        ax=ax)

    ax.set_xlabel('dist [m]')
    ax.set_ylabel('%')
    ax.legend()

    return ax


def compute_buffer_mean(ba, da):
    if isinstance(ba.buffer_dists[0], str):
        buffer_dists = list(
            map(lambda x: float(x.split('-')[-1]), ba.buffer_dists))
    else:
        buffer_dists = ba.buffer_dists

    if 'time' in da.dims:
        ref_da = da.isel(time=0).drop('time')

        def _compute_buffer_mean(buffer_mask_da, da):
            buffer_mask_cond = buffer_mask_da == 1
            return da.groupby('time').apply(
                lambda day_da: day_da.where(buffer_mask_cond).mean())
    else:

        def _compute_buffer_mean(buffer_mask_da, da):
            buffer_mask_cond = buffer_mask_da == 1
            return da.where(buffer_mask_cond).mean()

        ref_da = da
    buffer_masks_da = xr.DataArray(ba.masks_arr.astype(np.uint8),
                                   dims=('buffer_dist', *ref_da.dims),
                                   coords={
                                       'buffer_dist': buffer_dists,
                                       **ref_da.coords
                                   },
                                   attrs=ref_da.attrs)

    return buffer_masks_da.groupby('buffer_dist').apply(_compute_buffer_mean,
                                                        args=(da, ))


def plot_mean_buffer_t(ba, t_da, ax):
    mean_buffer_t_da = compute_buffer_mean(ba, t_da)
    # plot it
    for date, group_da in mean_buffer_t_da.groupby('time'):
        group_da.plot(ax=ax,
                      label=np.datetime_as_string(date, unit='D'),
                      linestyle='--',
                      marker='o')
    ax.legend()

    return ax


def get_zonal_grid_analysis(landscape,
                            landscape_meta,
                            zone_pixel_width=60,
                            zone_pixel_height=60):
    return pls.ZonalGridAnalysis(
        landscape,
        zone_pixel_width=zone_pixel_width,
        zone_pixel_height=zone_pixel_height,
        landscape_crs=landscape_meta['crs'],
        landscape_transform=landscape_meta['transform'])


def get_zonal_grid_t_arrs(t_arr, zga):
    # TODO: use `skimage.util.view_as_blocks`?
    # zone_pixel_height, zone_pixel_width = zga.landscapes[
    #     0].landscape_arr.shape
    # zone_shape = np.array([zone_pixel_height, zone_pixel_width])
    zone_shape = zga.landscapes[0].landscape_arr.shape
    _, num_zone_rows, num_zone_cols = zga.masks_arr.shape
    return stride_tricks.as_strided(
        t_arr,
        shape=(num_zone_rows, num_zone_cols) + zone_shape,
        strides=tuple(t_arr.strides * np.array(zone_shape)) +
        t_arr.strides).reshape((num_zone_cols * num_zone_rows, ) +
                               zone_shape)[zga.data_zones]


def get_linear_regression_summary(metrics_df, class_val, zonal_t_arrs):
    df = metrics_df.iloc[metrics_df.index.get_level_values('class_val') ==
                         class_val].droplevel(0)
    df['t'] = np.mean(zonal_t_arrs, axis=(1, 2))
    df = df.dropna().apply(pd.to_numeric)

    X = sm.add_constant(df.drop('t', axis=1))
    y = df['t']

    return sm.OLS(y, X).fit().summary()
