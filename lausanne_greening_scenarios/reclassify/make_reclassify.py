import itertools
import logging

import click
import numpy as np
import pandas as pd
import rasterio as rio

from lausanne_greening_scenarios import settings

INVEST_RENAME_COLUMNS_DICT = {
    'new_code': 'lucode',
    'tree_cover': 'shade',
    'building_cover': 'building_intensity',
    'crop_factor': 'kc'
}


def get_bin_edges(num_bins):
    return np.linspace(0, 1, num_bins + 1)


def get_reclassif_dict(num_bins):
    return {
        i: bin_center
        for i, bin_center in enumerate(
            np.linspace(0, 1, 2 * num_bins + 1)[1::2], start=1)
    }


def reclassify_by_cover(class_cond, cover_arr, nodata, bins):
    reclassif_arr = np.full_like(cover_arr, nodata)
    for i, upper_threshold in enumerate(bins[1:], start=1):
        # np.median(cover_arr[class_cond & (cover_arr >= bins[i - 1]) &
        #                     (cover_arr < upper_threshold)])
        reclassif_arr[class_cond & (cover_arr >= bins[i - 1]) &
                      (cover_arr < upper_threshold)] = i
    # for the cover values of 1
    reclassif_arr[class_cond & (cover_arr == upper_threshold)] = i

    return reclassif_arr


@click.command()
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('tree_cover_filepath', type=click.Path(exists=True))
@click.argument('bldg_cover_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('dst_tif_filepath', type=click.Path())
@click.argument('dst_csv_filepath', type=click.Path())
@click.option('--num-tree-bins', type=int, default=4)
@click.option('--num-bldg-bins', type=int, default=4)
@click.option('--dst-dtype', default='uint16')
@click.option('--dst-nodata', default=0)
def main(
    agglom_lulc_filepath,
    tree_cover_filepath,
    bldg_cover_filepath,
    biophysical_table_filepath,
    dst_tif_filepath,
    dst_csv_filepath,
    num_tree_bins,
    num_bldg_bins,
    dst_dtype,
    dst_nodata,
):
    logger = logging.getLogger(__name__)

    # read the raster datasets
    with rio.open(agglom_lulc_filepath) as src:
        lulc_arr = src.read(1)
        nodata = src.nodata
        meta = src.meta.copy()
    with rio.open(tree_cover_filepath) as src:
        tree_cover_arr = src.read(1)
    with rio.open(bldg_cover_filepath) as src:
        bldg_cover_arr = src.read(1)
    logger.info(
        "Read LULC, tree cover and building cover raster data from %s, %s, %s",
        agglom_lulc_filepath, tree_cover_filepath, bldg_cover_filepath)

    # reclassify
    classes = np.unique(lulc_arr[lulc_arr != nodata])
    # num_pixels = np.sum(lulc_arr != nodata)
    reclassif_classes = []
    reclassif_arr = np.full_like(lulc_arr, dst_nodata, dtype=dst_dtype)
    tree_reclassif_dict = get_reclassif_dict(num_tree_bins)
    tree_bins = get_bin_edges(num_tree_bins)
    bldg_reclassif_dict = get_reclassif_dict(num_bldg_bins)
    bldg_bins = get_bin_edges(num_bldg_bins)
    for class_val in classes:
        reclassif_class_val = len(reclassif_classes) + 1  # start by 1 (not 0)
        # if np.sum(lulc_arr == class_val) / num_pixels > min_reclassify_prop:
        class_cond = lulc_arr == class_val
        tree_reclassif_arr = reclassify_by_cover(class_cond, tree_cover_arr,
                                                 nodata, tree_bins)
        bldg_reclassif_arr = reclassify_by_cover(class_cond, bldg_cover_arr,
                                                 nodata, bldg_bins)
        for tree_class_val, bldg_class_val in itertools.product(
                tree_reclassif_dict, bldg_reclassif_dict):
            cond = (tree_reclassif_arr == tree_class_val) & (
                bldg_reclassif_arr == bldg_class_val)
            reclassif_arr[cond] = reclassif_class_val
            reclassif_classes.append([
                class_val, reclassif_class_val,
                tree_reclassif_dict[tree_class_val],
                bldg_reclassif_dict[bldg_class_val]
            ])
            reclassif_class_val += 1
        # else:
        #     class_cond = lulc_arr == class_val
        #     new_arr[class_cond] = new_class_val
        #     new_classes.append([
        #         class_val, new_class_val, tree_cover_arr[class_cond].mean(),
        #         building_cover_arr[class_cond].mean()
        #     ])
    logger.info(
        "Reclassified %d LULC classes into %d new classes based on "
        "tree and building cover", len(classes), len(reclassif_classes))

    # dump reclassified raster
    meta.update(dtype=dst_dtype, nodata=dst_nodata)
    with rio.open(dst_tif_filepath, 'w', **meta) as dst:
        dst.write(reclassif_arr, 1)
    logger.info("Dumped reclassif. raster dataset to %s", dst_tif_filepath)

    # adapt biophysical table to reclassification
    # 1. merge the two dataframes
    biophysical_df = pd.read_csv(biophysical_table_filepath, index_col=0)
    reclassif_df = pd.DataFrame(
        reclassif_classes,
        columns=['lucode', 'new_code', 'tree_cover',
                 'building_cover'])  # .to_csv(dst_csv_filepath)
    dst_df = reclassif_df.merge(biophysical_df, on='lucode')

    # 2. rescale albedo to distinguish high/low density urban classes
    bin_centers = dst_df[dst_df['lucode'] == 0]['building_cover'].unique()
    bin_min = bin_centers.min()
    bin_diff = bin_centers.max() - bin_min
    dst_df['albedo'] = dst_df.apply(
        lambda row: row['albedo_max'] -
        (row['albedo_max'] - row['albedo_min']) *
        (row['building_cover'] - bin_min) / bin_diff,
        axis=1)

    # 3. drop unnecessary columns and rename remaining columns to match InVEST
    # save the original LULC code
    dst_df['orig_lucode'] = dst_df['lucode']
    dst_df = dst_df.drop(['lucode', 'albedo_min', 'albedo_max'], axis=1)
    dst_df = dst_df.rename(columns=INVEST_RENAME_COLUMNS_DICT)

    # 4. dump it
    dst_df.to_csv(dst_csv_filepath, index=False)
    logger.info(
        "Dumped reclassif. table with tree and building cover values to %s",
        dst_csv_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
