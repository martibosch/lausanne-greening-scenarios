import matplotlib.pyplot as plt
import seaborn as sns

REF_APPROACH = 'random'
OTHER_APPROACHES = ['scatter', 'cluster']


def _get_approach_comparison_cols(ref_approach, other_approaches):
    return [
        f'{ref_approach}-{other_approach}'
        for other_approach in other_approaches
    ]


def get_comparison_df(df,
                      y,
                      ref_approach=None,
                      other_approaches=None,
                      sortby=None):
    if ref_approach is None:
        ref_approach = REF_APPROACH
    if other_approaches is None:
        # approaches = df['interaction'].unique()
        other_approaches = OTHER_APPROACHES

    if sortby is None:
        sortby = ['change_prop']

    df = df[df['change_prop'] < 1]

    def _get_approach_df(approach):
        return df[df['interaction'] == approach].sort_values(sortby)

    comparison_df = _get_approach_df(ref_approach).drop('interaction', axis=1)
    for other_approach in other_approaches:
        comparison_df[f'{ref_approach}-{other_approach}'] = comparison_df[
            y].values - _get_approach_df(other_approach)[y].values

    return comparison_df


def get_absolute_comparison_df(comparison_df,
                               groupby,
                               y=None,
                               ref_approach=None,
                               other_approaches=None,
                               agg='mean'):
    if ref_approach is None:
        ref_approach = REF_APPROACH
    if other_approaches is None:
        other_approaches = OTHER_APPROACHES
    cols = groupby + _get_approach_comparison_cols(ref_approach,
                                                   other_approaches)
    if y is not None:
        cols = [y] + cols
    return comparison_df[cols].groupby(groupby).agg(agg)


def get_relative_comparison_df(comparison_df,
                               groupby,
                               y,
                               ref_approach=None,
                               other_approaches=None,
                               by_group_totals=False):
    if ref_approach is None:
        ref_approach = REF_APPROACH
    if other_approaches is None:
        other_approaches = OTHER_APPROACHES
    approach_comparison_cols = _get_approach_comparison_cols(
        ref_approach, other_approaches)
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
                             ref_approach=None,
                             other_approaches=None,
                             base_figsize=None,
                             **barplot_kws):
    if ref_approach is None:
        ref_approach = REF_APPROACH
    if other_approaches is None:
        other_approaches = OTHER_APPROACHES

    if base_figsize is None:
        figwidth, figheight = plt.rcParams['figure.figsize']
    else:
        figwidth, figheight = base_figsize

    if barplot_kws is None:
        barplot_kws = {}

    num_pairs = len(other_approaches)
    fig, axes = plt.subplots(1,
                             num_pairs,
                             figsize=(num_pairs * figwidth, figheight),
                             sharex=True,
                             sharey=True)

    for other_approach, ax in zip(other_approaches, axes.flat):
        sns.barplot(x=x,
                    y=f'{ref_approach}-{other_approach}',
                    hue=hue,
                    data=comparison_df,
                    ax=ax,
                    **barplot_kws)
        ax.set_ylabel('$N_{%s} - N_{%s}$' % (ref_approach, other_approach))
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
