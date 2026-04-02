"""Metrics module.

Contains several metrics functions usefull when evaluating ML models
"""
import pandas as pd


def precisionatK(
        predictions: pd.DataFrame, query_col: str, ranking_col: str,
        ground_truth_col: str, k: int
    ) -> pd.Series:
    """Precision@K

    Computes Precision@K for every query inside a DataFrame in the context
    of a recommendation model.

    Parameters
    ----------
    predictions : pd.DataFrame
        DataFrame with the queries IDs, ranking of the recommendations and
        its relevance
    query_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        query IDs
    ranking_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        ranking of the recommendations made by the model. This column must
        contain numbers from 1 to inf for recommended docs and ``pd.NA``
        for docs relevant but not recommended
    ground_truth_col : str
        Name of the column in the ``predictions`` DataFrame containing if
        the recommendations made by the model were relevant or not (1 for
        relevant, 0 for irrelevant)
    k : int
        Top number of docs for which the precision will be computed. Must
        be 1 or higher

    Returns
    -------
    precision_at_k : pd.Series
        Pandas Series object with the Precision@K for every query
    """  # noqa: D400
    # Validate k > 1
    if k < 1:
        err_msg = 'k must be 1 or higher.'
        raise ValueError(err_msg)
    # Filters top K recommendations
    # It's safe to say that all elements now are recommendations, so the
    # relevant column also represent the relevant recommended products
    k_filtered_preds = predictions[predictions[ranking_col] <= k].copy()
    # Group by query IDs
    query_grouped = k_filtered_preds.groupby(query_col)
    # Return precision at K per query
    return (
        # Relevant recommended docs / All recommended docs
        query_grouped[ground_truth_col].sum()
        / query_grouped[ranking_col].count()
    ).rename(f'precision_at_{k}')


def recallatK(
        predictions: pd.DataFrame, query_col: str, ranking_col: str,
        ground_truth_col: str, k: int
    ) -> pd.Series:
    """Recall@K

    Computes Recall@K for every query inside a DataFrame in the context of
    a recommendation model.

    Parameters
    ----------
    predictions : pd.DataFrame
        DataFrame with the queries IDs, ranking of the recommendations and
        its relevance
    query_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        query IDs
    ranking_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        ranking of the recommendations made by the model. This column must
        contain numbers from 1 to inf for recommended docs and ``pd.NA``
        for docs relevant but not recommended
    ground_truth_col : str
        Name of the column in the ``predictions`` DataFrame containing if
        the recommendations made by the model were relevant or not (1 for
        relevant, 0 for irrelevant)
    k : int
        Top number of docs for which the precision will be computed. Must
        be 1 or higher

    Returns
    -------
    recall_at_k : pd.Series
        Pandas Series object with the Recall@K for every query
    """  # noqa: D400
    # Validate k > 1
    if k < 1:
        err_msg = 'k must be 1 or higher.'
        raise ValueError(err_msg)
    # DataFrame with [total relevant docs, total recommended relevant docs]
    aux = pd.merge(  # noqa: PD015
        # Total relevant elements
        left=predictions.groupby(
            [query_col]
        )[ground_truth_col].sum(),

        # Total recommended relevant elements
        right=predictions[
            # Top K recommended elements
            # (Now all elements in the df are recommended)
            predictions[ranking_col] <= k
        ].groupby(query_col)[ground_truth_col].sum(),

        how='inner',
        on=query_col,
        suffixes=('_total', '_recommended')
    )

    # Return Recall per query
    return (
        aux[f'{ground_truth_col}_recommended']
        / aux[f'{ground_truth_col}_total']
    ).rename(f'recall_at_{k}')


def APatK(
        predictions: pd.DataFrame, query_col: str, ranking_col: str,
        ground_truth_col: str, k: int
    ) -> pd.Series:
    """AP@K

    Computes AveragePrecision@K for every query inside a DataFrame in the
    context of a recommendation model. If a query contains less
    recommendations than K, the precision is filled with 0's when computing
    the AP.

    Parameters
    ----------
    predictions : pd.DataFrame
        DataFrame with the queries IDs, ranking of the recommendations and
        its relevance
    query_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        query IDs
    ranking_col : str
        Name of the column in the ``predictions`` DataFrame containing the
        ranking of the recommendations made by the model. This column must
        contain numbers from 1 to inf for recommended docs and ``pd.NA``
        for docs relevant but not recommended
    ground_truth_col : str
        Name of the column in the ``predictions`` DataFrame containing if
        the recommendations made by the model were relevant or not (1 for
        relevant, 0 for irrelevant)
    k : int
        Top number of docs for which the average precision will be computed

    Returns
    -------
    ap_at_k : pd.Series
        Pandas Series object with the AP@K for every query
    """  # noqa: D400
    # Validate k > 1
    if k < 1:
        err_msg = 'k must be 1 or higher.'
        raise ValueError(err_msg)
    # Extract query names
    ap_df = predictions[query_col].to_frame().drop_duplicates()

    # Iterate 0:k
    for k_i in range(1, k + 1):
        # Get k_i recommendation relevance per query
        k_i_computation = predictions[
            predictions[ranking_col] == k_i
        ][
            [query_col, ground_truth_col]
        ].rename(columns={ground_truth_col: f'{ground_truth_col}_k_{k_i}'})

        # Compute precision for this k_i
        k_i_computation = k_i_computation.merge(
            precisionatK(
                predictions=predictions,
                query_col=query_col,
                ranking_col=ranking_col,
                ground_truth_col=ground_truth_col,
                k=k_i
            ),
            how='inner',
            on=query_col,
        )

        # Compute "relevant precision" as:
        # P@k_i * R_i, with R e {0, 1} relevance of item i
        k_i_computation[f'relevant_precision_at_{k_i}'] = (
            k_i_computation[f'precision_at_{k_i}']
            * k_i_computation[f'{ground_truth_col}_k_{k_i}']
        )

        ap_df = ap_df.merge(
            k_i_computation[[query_col, f'relevant_precision_at_{k_i}']],
            on=query_col,
            how='outer'
        )


    # Add column with total number of relevant docs per query
    ap_df = ap_df.fillna(
        value=0,
    ).merge(
        predictions.groupby(query_col)[[ground_truth_col]].sum(),
        on=query_col,
        how='inner'
    ).rename(columns={ground_truth_col: 'total_relevant'})

    # Calculate AP as sum(P@k_i * R_i) / relevant_items
    ap_df[f'ap_at_{k}'] = (
        ap_df[
            [f'relevant_precision_at_{k_i}' for k_i in range(1, k + 1)]
        ].sum(axis=1)
        / ap_df['total_relevant']
    )
    return ap_df.set_index(query_col)[f'ap_at_{k}']


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
