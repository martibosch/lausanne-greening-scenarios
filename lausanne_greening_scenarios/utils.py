from os import path

import geopandas as gpd
import numpy as np
import salem  # noqa: F401
import xarray as xr
from rasterio import transform

PACKAGE_ROOT = path.dirname(path.abspath(__file__))

# GEO-OPERATIONS
CRS = 'epsg:2056'


def align_ds(ds, ref_ds, interp='linear'):
    if ds.name is None:
        ds.name = ''  # salem needs some name to align the ds/da
    return ref_ds.salem.transform(ds, interp=interp)


def salem_da_from_singleband(raster_filepath, name=None):
    if name is None:
        name = ''  # salem needs a ds/da name (even empty)
    raster_da = xr.open_rasterio(raster_filepath).isel(band=0)
    raster_da.name = name
    raster_da.attrs['pyproj_srs'] = raster_da.crs

    return raster_da


def clip_ds_to_extent(ds,
                      shape=None,
                      geometry=None,
                      crs=None,
                      roi=True,
                      subset_kws=None,
                      roi_kws=None):
    if subset_kws is None:
        subset_kws = {}
    if roi_kws is None:
        roi_kws = {}

    if shape is not None:
        subset_kws['shape'] = shape
        if roi:
            roi_kws['shape'] = shape
    elif geometry is not None:
        subset_kws['geometry'] = geometry
        subset_kws['crs'] = crs
        if roi:
            roi_kws['geometry'] = geometry
            roi_kws['crs'] = crs
    subset_ds = ds.salem.subset(**subset_kws)
    if roi:
        return subset_ds.salem.roi(**roi_kws)
    return subset_ds


def _calculate_transform(geometry, dst_res):
    west, south, east, north = geometry.bounds
    dst_height, dst_width = tuple(
        int(np.ceil(diff / dst_res)) for diff in [north - south, east - west])
    dst_transform = transform.from_origin(west, north, dst_res, dst_res)

    return dst_transform, (dst_height, dst_width)


def get_ref_da(ref_geom, dst_res, dst_fill=0, dst_crs=None):
    if dst_crs is None:
        dst_crs = CRS
    ref_transform, (ref_height,
                    ref_width) = _calculate_transform(ref_geom, dst_res)
    rows = np.arange(ref_height)
    cols = np.arange(ref_width)
    xs, _ = transform.xy(ref_transform, cols, cols)
    _, ys = transform.xy(ref_transform, rows, rows)
    ref_da = xr.DataArray(dst_fill, dims=('y', 'x'), coords={'y': ys, 'x': xs})
    ref_da.attrs['pyproj_srs'] = dst_crs

    return ref_da


def plot_T_maps(T_da,
                station_location_df,
                num_cols=3,
                comparison_df=None,
                err_classes=None,
                **plot_kws):
    g = T_da.rename({
        'time': 'date'
    }).plot(
        x='x',
        y='y',
        col='date',
        col_wrap=num_cols,
        # cbar_kwargs={
        #     'shrink': .2,
        #     'pad': 0.02,
        # }
        add_colorbar=False,
        **plot_kws)

    # post-processing
    fig = g.fig
    flat_axes = g.axes.flatten()

    # prepare last axis for the legend
    last_ax = flat_axes[-1]
    last_ax.set_visible(True)
    last_ax.axis('off')

    if comparison_df is not None:
        err_gdf = gpd.GeoDataFrame(
            comparison_df['date'],
            geometry=list(
                comparison_df['station'].map(lambda stn: geometry.Point(
                    *station_location_df.loc[stn][['x', 'y']]))))
        err_gdf['err'] = comparison_df['pred'] - comparison_df['obs']

        if err_classes is None:
            err_classes = ERR_CLASSES
        err_gdf['err_class'] = np.digitize(err_gdf['err'], err_classes) - 1

        palette = sns.color_palette('coolwarm', n_colors=len(err_classes) - 1)
        cmap = colors.ListedColormap(palette)

        # set black edge color for markers
        plt.rcParams.update(**{'scatter.edgecolors': 'k'})

        # plot the stations
        for (_, date_gdf), ax in zip(err_gdf.groupby('date'), flat_axes):
            date_gdf.plot(column='err_class', ax=ax, cmap=cmap)
            # ax.set_xticks([])
            # ax.set_yticks([])
        # generate a legend and place it in the last (empty) axis
        for start, end, color in zip(err_classes, err_classes[1:], palette):
            last_ax.plot(0, 0, 'o', c=color, label=f'[{start}, {end})')
        last_ax.legend(
            loc='center',
            facecolor='white',
            title='Regression error $\hat{T} - T_{obs}$ [$\degree$C]')
        fig.colorbar(g._mappables[-1],
                     ax=last_ax,
                     label='Map temperature $\hat{T}$ [$\degree$C]',
                     shrink=.45)

    else:
        station_gser = gpd.GeoSeries(
            gpd.points_from_xy(station_location_df['x'],
                               station_location_df['y']))

        # invisibly plot the stations in each map axis just so that the axis
        # limits and aspect ratio are set correctly
        for ax in flat_axes[:-1]:
            station_gser.plot(ax=ax, alpha=0)

        fig.colorbar(g._mappables[-1],
                     ax=last_ax,
                     label='$\hat{T}_{sr} - \hat{T}_{ucm}$ [$\degree$C]',
                     orientation='horizontal',
                     fraction=.55,
                     shrink=.8,
                     boundaries=ERR_BOUNDARIES)

    # g.add_colorbar()
    fig.subplots_adjust(hspace=-.5)
    # fig.savefig('../reports/figures/spatial-regression-maps.png')
    return g
