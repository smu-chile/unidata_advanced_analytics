"""Recomendation systems module.

Contains functions usefull when building recomendation systems
"""
from __future__ import annotations

import logging

import pandas as pd
from sklearn.metrics.pairwise import cosine_distances


def meltedCosineDistance(
        embedding_matrix_a: pd.DataFrame, embedding_matrix_b: pd.DataFrame = None,
        antiparallel_filter: bool = True, verbose: bool = False
    ) -> pd.DataFrame:
    """Compute the cosine distance between embedding matrixes.

    Returns a melted cosine distance between two embedding matrixes.

    Parameters
    ----------
    embedding_matrix_a : pd.DataFrame
        DataFrame with only embedding dimensions as columns and
        identificator as index
    embedding_matrix_b : pd.DataFrame, optional
        DataFrame with only embedding dimensions as columns and
        identificator as index. If not given, the function returns the
        autodistance matrix for embedding_matrix_a
    antiparallel_filter : bool, default=True
        Whether to remove or not vectors with an angle greater than 90°
    verbose : bool, default=True
        Controls the verbosity

    Returns
    -------
    melted_distance_matrix : pd.DataFrame
        Melted distance matrix between embedding_matrix_a and
        embedding_matrix_b, or if embedding_matrix_b is `None` then the
        autodistance matrix for embedding_matrix_a
    """
    # Computes autodistance
    if embedding_matrix_b is None:
        embedding_matrix_b = embedding_matrix_a
        var_name = 'other_' + embedding_matrix_b.index.name
    # Set up var name to avoid column name conflicts
    if embedding_matrix_a.index.name == embedding_matrix_b.index.name:
        var_name = 'other_' + embedding_matrix_b.index.name
    else:
        var_name = embedding_matrix_b.index.name

    # Compute distance. This step ends with a distance matrix
    # with dimension #(embedding matrix a) x #(embedding matrix b)
    distances = pd.DataFrame(
        data=cosine_distances(
            embedding_matrix_a,
            embedding_matrix_b,
        ),
        index=embedding_matrix_a.index,
        columns=embedding_matrix_b.index.to_list(),
    )
    if verbose:
        logging.debug(f'OK: Compute distance matrix. Dimension {distances.shape}')

    # Melt the rectangular distance matrix into three columns:
    # [ a_index_col | b_index_col | cosine distance ]
    # So the distance matrix have dim:
    # (#(embedding matrix a) x #(embedding matrix b)) x 3
    distances = pd.melt(
        distances.reset_index(),
        id_vars=embedding_matrix_a.index.name,
        var_name=var_name,
        value_name='cosine_distance',
    )
    if verbose:
        logging.debug(f'OK: Melt matrix. Dimension {distances.shape}')

    # Filter that removes all antiparallel elements
    if antiparallel_filter:
        distances = distances[distances['cosine_distance'] < 1]
        if verbose:
            logging.debug(f'OK: Filter vectors with +90°. New dim {distances.shape}')

    return distances


def filterRecommendations(
    ranked_recommendations: pd.DataFrame, filter_df: pd.DataFrame,
    ranking_col_name: str = 'cosine_distance', max_recommendations: int = 1,
    problem_type: str = 'minimization'
) -> pd.DataFrame:
    """Filter a recommendation DataFrame using another filter DataFrame.

    Returns a top recommendations DataFrame builded by grouping the
    recommendation DataFrame by one of the columns on the filter DataFrame

    Parameters
    ----------
    ranked_recommendations: pd.DataFrame
        DataFrame with ranked recommendations. Can have any number of
        columns but at least one of them must have a ranking of them
    filter_df: pd.DataFrame
        DataFrame with only two columns. One must merge with the
        ranked_recommendations DataFrame and the other must contain some
        id for filtering
    ranking_col_name: str, default='cosine_distance'
        Ranking column name
    max_recommendations: int, default=1
        Top max_recommendations will be preserved when filtering
    problem_type: {'minimization', 'maximization'}, default='minimization'
        Whether the ranking col should be minimized or maxmized when
        filtering. Minimization would preserve the top minimum ranking
        values and viceversa

    Returns
    -------
    filtered_df : pd.DataFrame
        Original DataFrame with filtered rows. It will have the same
        columns as ranked_recommendations
    """
    # Get the name of the common column between frames
    common_column = list(
        set(ranked_recommendations.columns).intersection(set(filter_df.columns))
    )
    # Verify one column joins between the two dfs
    if len(common_column) != 1:
        err_msg = (
            'The recommendation and filter dataframes have more than 1 column in common'
            if len(common_column) > 1
            else 'The recommendation and filter dataframes have no column in common'
        )
        raise Exception(err_msg)

    # Filter recommendations...
    return ranked_recommendations.merge(
        filter_df
    # minimazing/maximizing the ranking column
    ).sort_values(
        ranking_col_name,
        ascending=problem_type == 'minimization'
    # for every column in the recommendation df + the filtering column...
    ).groupby(
        list(
            set(ranked_recommendations.columns).union(
                set(filter_df.columns)
            ).difference(
                {ranking_col_name, common_column[0]}
            )
        ),
        sort=False
    # get only the top elements...
    ).head(
        max_recommendations
    # and return only the recommendation DataFrame columns
    )[ranked_recommendations.columns]


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
