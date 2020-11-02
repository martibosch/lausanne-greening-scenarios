import itertools

import matplotlib.pyplot as plt
import seaborn as sns

APPROACHES = ['random', 'scatter', 'cluster']


def _get_approach_comparison_cols(approaches):
    return [
        f'{first_approach}-{second_approach}'
        for first_approach, second_approach in itertools.combinations(
            approaches, 2)
    ]


def get_comparison_df(df, y, approaches=None, sortby=None):
    if approaches is None:
        # approaches = df['interaction'].unique()
        approaches = APPROACHES

    if sortby is None:
        sortby = ['change_prop']

    df = df[df['change_prop'] < 1]

    def _get_approach_df(approach):
        return df[df['interaction'] == approach].sort_values(sortby)

    approach_pairs = itertools.combinations(approaches, 2)
    first_approach, second_approach = next(approach_pairs)
    comparison_df = _get_approach_df(first_approach).drop('interaction',
                                                          axis=1)
    comparison_df[f'{first_approach}-{second_approach}'] = comparison_df[
        y] - _get_approach_df(second_approach)[y].values
    for first_approach, second_approach in approach_pairs:
        comparison_df[
            f'{first_approach}-{second_approach}'] = _get_approach_df(
                first_approach)[y].values - _get_approach_df(
                    second_approach)[y].values

    return comparison_df


def get_absolute_comparison_df(comparison_df,
                               groupby,
                               y=None,
                               approaches=None,
                               agg='mean'):
    if approaches is None:
        approaches = APPROACHES
    cols = groupby + _get_approach_comparison_cols(approaches)
    if y is not None:
        cols = [y] + cols
    return comparison_df[cols].groupby(groupby).agg(agg)


def get_relative_comparison_df(comparison_df,
                               groupby,
                               y,
                               approaches=None,
                               by_group_totals=False):
    if approaches is None:
        approaches = APPROACHES
    approach_comparison_cols = _get_approach_comparison_cols(approaches)
    divisor = comparison_df[y]
    if by_group_totals:
        divisor = divisor.groupby(
            comparison_df['change_prop']).transform('sum')
    return comparison_df[[y] + approach_comparison_cols].div(
        divisor, axis='rows').assign(
            **{col: comparison_df[col]
               for col in groupby}).groupby(groupby).mean().drop(y, axis=1)


def plot_approach_comparison(comparison_df,
                             x,
                             hue,
                             approaches=None,
                             base_figsize=None,
                             **barplot_kws):

    if approaches is None:
        approaches = APPROACHES

    if base_figsize is None:
        figwidth, figheight = plt.rcParams['figure.figsize']
    else:
        figwidth, figheight = base_figsize

    if barplot_kws is None:
        barplot_kws = {}

    approach_pairs = list(itertools.combinations(approaches, 2))
    num_pairs = len(approach_pairs)

    fig, axes = plt.subplots(1,
                             num_pairs,
                             figsize=(num_pairs * figwidth, figheight),
                             sharex=True,
                             sharey=True)

    for (first_approach, second_approach), ax in zip(approach_pairs,
                                                     axes.flat):
        sns.barplot(x=x,
                    y=f'{first_approach}-{second_approach}',
                    hue=hue,
                    data=comparison_df,
                    ax=ax,
                    **barplot_kws)
        ax.set_ylabel('$N_{%s} - N_{%s}$' % (first_approach, second_approach))
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
