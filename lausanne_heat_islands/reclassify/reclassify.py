import itertools
import logging

import click
import numpy as np
import pandas as pd
import rasterio as rio

from lausanne_heat_islands import settings

# we could DRY this by taking this constant from `zurich_heat_islands.extracts`
LAUSANNE_URBAN_CLASSES = list(range(8))

INVEST_RENAME_COLUMNS_DICT = {
    'new_code': 'lucode',
    'tree_cover': 'shade',
    'crop_factor': 'kc'
}


@click.command()
@click.argument('agglom_lulc_filepath', type=click.Path(exists=True))
@click.argument('tree_cover_filepath', type=click.Path(exists=True))
@click.argument('building_cover_filepath', type=click.Path(exists=True))
@click.argument('biophysical_table_filepath', type=click.Path(exists=True))
@click.argument('dst_tif_filepath', type=click.Path())
@click.argument('dst_csv_filepath', type=click.Path())
@click.option('--min-reclassify-prop', type=float, default=.01)
@click.option('--extra-bin-threshold', type=float, default=.03)
@click.option('--albedo-min', type=float, default=.133)
@click.option('--albedo-max', type=float, default=.153)
def main(agglom_lulc_filepath, tree_cover_filepath, building_cover_filepath,
         biophysical_table_filepath, dst_tif_filepath, dst_csv_filepath,
         min_reclassify_prop, extra_bin_threshold, albedo_min, albedo_max):
    logger = logging.getLogger(__name__)

    # read the raster datasets
    with rio.open(agglom_lulc_filepath) as src:
        lulc_arr = src.read(1)
        nodata = src.nodata
        meta = src.meta
    with rio.open(tree_cover_filepath) as src:
        tree_cover_arr = src.read(1)
    with rio.open(building_cover_filepath) as src:
        building_cover_arr = src.read(1)
    logger.info(
        "Read LULC, tree cover and building cover raster data from %s, %s, %s",
        agglom_lulc_filepath, tree_cover_filepath, building_cover_filepath)

    def reclassify_by_cover(class_val, cover_arr):
        # get the number of bins for which adding an extra bin would result in
        # having a bin with less than `threshold` samples
        num_bins = 1
        class_cond = lulc_arr == class_val
        class_arr = cover_arr[class_cond]
        _, bin_edges = np.histogram(class_arr, bins=num_bins)
        while True:
            num_bins += 1
            this_hist, this_bin_edges = np.histogram(class_arr, bins=num_bins)

            if np.min(this_hist / len(class_arr)) < extra_bin_threshold:
                break
            else:
                # hist, bin_edges = this_hist, this_bin_edges
                bin_edges = this_bin_edges

        new_arr = np.full_like(cover_arr, nodata)
        cover_dict = {}
        if num_bins > 1:
            for i, upper_bin_edge in enumerate(bin_edges[1:], start=1):
                # lower_bin_edge = bin_edges[i - 1]
                bin_cond = class_cond & (cover_arr >= bin_edges[i - 1]) & (
                    cover_arr <= upper_bin_edge)
                new_arr[bin_cond] = i
                # (upper_bin_edge - lower_bin_edge) / 2
                cover_dict[i] = cover_arr[bin_cond].mean()
        else:
            new_arr[class_cond] = 1
            cover_dict[1] = class_arr.mean()

        return new_arr, cover_dict

    # reclassify
    classes = np.unique(lulc_arr[lulc_arr != nodata])
    num_pixels = np.sum(lulc_arr != nodata)
    new_classes = []
    new_arr = np.full_like(lulc_arr, nodata)
    for class_val in classes:
        new_class_val = len(new_classes) + 1  # start by 1 (not 0)
        if np.sum(lulc_arr == class_val) / num_pixels > min_reclassify_prop:
            tree_new_arr, tree_dict = reclassify_by_cover(
                class_val, tree_cover_arr)
            building_new_arr, building_dict = reclassify_by_cover(
                class_val, building_cover_arr)
            for tree_class_val, building_class_val in itertools.product(
                    tree_dict, building_dict):
                cond = (tree_new_arr == tree_class_val) & (
                    building_new_arr == building_class_val)
                new_arr[cond] = new_class_val
                new_classes.append([
                    class_val, new_class_val, tree_dict[tree_class_val],
                    building_dict[building_class_val]
                ])
                new_class_val += 1
        else:
            class_cond = lulc_arr == class_val
            new_arr[class_cond] = new_class_val
            new_classes.append([
                class_val, new_class_val, tree_cover_arr[class_cond].mean(),
                building_cover_arr[class_cond].mean()
            ])
    logger.info(
        "Reclassified %d LULC classes into %d new classes based on " +
        "tree and building cover", len(classes), len(new_classes))

    # dump reclassified raster
    with rio.open(dst_tif_filepath, 'w', **meta) as dst:
        dst.write(new_arr, 1)
    logger.info("Dumped reclassif. raster dataset to %s", dst_tif_filepath)

    # adapt biophysical table to reclassification
    # 1. merge the two dataframes
    biophysical_df = pd.read_csv(biophysical_table_filepath, index_col=0)
    reclassif_df = pd.DataFrame(
        new_classes,
        columns=['lulc_code', 'new_code', 'tree_cover',
                 'building_cover'])  # .to_csv(dst_csv_filepath)
    dst_df = reclassif_df.merge(biophysical_df, on='lulc_code')

    # 2. rescale albedo to distinguish high/low density urban classes
    building_cover_ser = reclassif_df[reclassif_df['lulc_code'].isin(
        LAUSANNE_URBAN_CLASSES)]['building_cover']
    rescaled_albedo_ser = pd.Series(
        (building_cover_ser - building_cover_ser.min()) /
        (building_cover_ser.max() - building_cover_ser.min()) *
        (albedo_max - albedo_min) + albedo_min,
        name='albedo')
    dst_df.update(rescaled_albedo_ser)

    # 3. rename columns to match InVEST
    dst_df = dst_df.rename(columns=INVEST_RENAME_COLUMNS_DICT)

    # 4. dump it
    dst_df.to_csv(dst_csv_filepath)
    logger.info(
        "Dumped reclassif. table with tree and building cover values to %s",
        dst_csv_filepath)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.DEFAULT_LOG_FMT)

    main()
