import itertools

import matplotlib.pyplot as plt
import seaborn as sns


def plot_approach_pairwise_comparison(df,
                                      x,
                                      y,
                                      hue,
                                      approaches=None,
                                      base_figsize=None,
                                      **barplot_kws):

    if approaches is None:
        approaches = ['random', 'scatter', 'cluster']

    sortby = ['change_prop']
    if hue != 'change_prop':
        sortby += [hue]

    if base_figsize is None:
        figwidth, figheight = plt.rcParams['figure.figsize']
    else:
        figwidth, figheight = base_figsize

    approach_pairs = list(itertools.combinations(approaches, 2))
    num_pairs = len(approach_pairs)

    fig, axes = plt.subplots(1,
                             num_pairs,
                             figsize=(num_pairs * figwidth, figheight),
                             sharex=True,
                             sharey=True)

    # _lulc_df = lulc_df[lulc_df['change_prop'] < 1]

    def _get_approach_df(approach):
        return df[df['interaction'] == approach].sort_values(sortby)

    for (first_approach, second_approach), ax in zip(approach_pairs,
                                                     axes.flat):
        comparison_df = _get_approach_df(first_approach)
        comparison_df[y] -= _get_approach_df(second_approach)[y].values
        sns.barplot(x=x,
                    y=y,
                    hue=hue,
                    data=comparison_df,
                    ax=ax,
                    **barplot_kws)
        ax.set_ylabel('$N_{%s} - N_{%s}$' % (first_approach, second_approach))
        ax.set_xlabel('Prop. transformed pixels')
        ax.get_legend().remove()
        ax.axhline(0, color='gray', linestyle='--')

    return fig


# def plot_approach_pairwise_comparison(df,
#                                       var_name,
#                                       hue,
#                                       approaches=None,
#                                       orientation='horizontal',
#                                       base_figsize=None):

#     if approaches is None:
#         approaches = ['random', 'scatter', 'cluster']

#     sortby = ['change_prop']
#     if hue != 'change_prop':
#         sortby += [hue]

#     if base_figsize is None:
#         figwidth, figheight = plt.rcParams['figure.figsize']
#     else:
#         figwidth, figheight = base_figsize

#     approach_pairs = list(itertools.combinations(approaches, 2))
#     num_pairs = len(approach_pairs)

#     if orientation == 'horizontal':
#         num_rows, num_cols = 1, num_pairs
#         figsize = (num_pairs * figwidth, figheight)
#         x = 'change_prop'
#         y = var_name
#         orient = 'v'
#         ax_xlabel_method = 'set_ylabel'
#         ax_ylabel_method = 'set_xlabel'
#         ax_line_method = 'axhline'
#     else:
#         num_rows, num_cols = num_pairs, 1
#         figsize = (figwidth, num_pairs * figheight)
#         x = var_name
#         y = 'change_prop'
#         orient = 'h'
#         ax_xlabel_method = 'set_xlabel'
#         ax_ylabel_method = 'set_ylabel'
#         ax_line_method = 'axvline'
#     fig, axes = plt.subplots(num_rows,
#                              num_cols,
#                              figsize=figsize,
#                              sharex=True,
#                              sharey=True)

#     # _lulc_df = lulc_df[lulc_df['change_prop'] < 1]

#     def _get_approach_df(approach):
#         return df[df['interaction'] == approach].sort_values(sortby)

#     for (first_approach, second_approach), ax in zip(approach_pairs,
#                                                      axes.flat):
#         comparison_df = _get_approach_df(first_approach)
#         comparison_df[var_name] -= _get_approach_df(
#             second_approach)[var_name].values
#         sns.barplot(x=x,
#                     y=y,
#                     hue=hue,
#                     data=comparison_df,
#                     orient=orient,
#                     ax=ax)
#         getattr(ax, ax_xlabel_method)('$N_{%s} - N_{%s}$' %
#                                       (first_approach, second_approach))
#         getattr(ax, ax_ylabel_method)('Prop. transformed pixels')
#         ax.get_legend().remove()
#         getattr(ax, ax_line_method)(0, color='gray', linestyle='--')

#     return fig
