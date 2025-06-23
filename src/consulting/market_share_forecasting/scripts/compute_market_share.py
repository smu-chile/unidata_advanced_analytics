from __future__ import annotations

# Default
import logging
import argparse
from logging import config
from collections import defaultdict

# pip
import pandas as pd
import pendulum
import awswrangler as wr
from boto3 import Session
from prophet import Prophet

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.aws_extended.athena import readAthenaQuery
from common.utils.data_transform import batchList
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
# Package config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_name', type=str, required=True,
    help='Name fo the Advanced Analytics project executed'
)
parser.add_argument(
    '--gcp_project', type=str, required=True,
    help='Name of the GCP project billed. Used to differenciate dev from prod'
)
parser.add_argument(
    '--execution_date', type=str, required=True,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'nielsen_data':
    """
    SELECT *
    FROM dev_perm.TMP_LAB_SMU_FACT_WEEK_NIELSEN_VENTA_CATEGORIA
    """,

    'holidays':
    """
    SELECT *
    FROM dev_perm.tmp_lab_smu_dim_holidays
    WHERE p_year != '{p_year}'

    UNION ALL

    SELECT *
    FROM dev_perm.tmp_lab_smu_dim_holidays
    WHERE
        p_year = '{p_year}'
        AND title NOT LIKE '%elecciones%'
    """
})


def fixFinPeriodo(row):
    if row['fin_periodo']:
        return row['fin_periodo']

    date = pendulum.from_format(row['strdate'].strftime('%Y-%m-%d'), 'YYYY-MM-DD').date()

    if date.day_of_week == pendulum.SUNDAY:
        return date

    return date.next(pendulum.SUNDAY)


# -------------------------------------------------------------------------
# Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())

    # Environment
    user: str = args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']

    # Automatic
    boto3_session = Session(**getSecret(
        project=gcp_project,
        secret_name='bdaa_aws_credentials'  # noqa: S106
    ))

    # Constants
    db_temp = 'dev_temp'

    # ------------
    # Data loading
    # ------------
    logging.info('Loading data...')
    nielsen_data = readAthenaQuery(
        user=user,
        query=SQL_QUERIES['nielsen_data'].substitute(),
        database=db_temp,
        boto3_session=boto3_session
    )

    holidays = readAthenaQuery(
        user=user,
        query=SQL_QUERIES['holidays'].substitute(
            p_year=execution_date[:4]
        ),
        database=db_temp,
        boto3_session=boto3_session
    )
    logging.info('Loading data...')

    holidays = pd.concat(
        [
            holidays[
                # TODO(ecastrot): Bad fix
                holidays['strdate'] != '2025-06-29'
            ],
            pd.DataFrame({
                'title': ['huelga_lider'],
                'strdate': ['2024-07-14'],
                'essential': [False],
                'p_year': ['2024'],
            })
        ],
        axis=0,
        ignore_index=True
    )

    nielsen_data['p_week'] = (
        nielsen_data['fin_periodo'].astype(str).str[:4]
        + pd.to_datetime(nielsen_data['fin_periodo']).dt.isocalendar()['week'].astype(str).str.zfill(2)  # noqa: E501
    )

    nielsen_data['p_year'] = nielsen_data['fin_periodo'].astype(str).str[:4]
    nielsen_data.head()

    holidays['strdate'] = pd.to_datetime(holidays['strdate'])
    holidays['p_week'] = (
        holidays['strdate'].astype(str).str[:4]
        + pd.to_datetime(holidays['strdate']).dt.isocalendar()['week'].astype(str).str.zfill(2)
    )

    holidays = holidays.merge(
        nielsen_data[['p_week', 'fin_periodo']].drop_duplicates(),
        how='left',
        on='p_week'
    )

    holidays['fin_periodo'] = holidays['fin_periodo'].fillna('')
    holidays['fin_periodo'] = holidays.apply(fixFinPeriodo, axis=1)
    holidays = holidays.sort_values('title').drop_duplicates(subset='fin_periodo', keep='first')

    nielsen_data['fin_periodo'] = pd.to_datetime(nielsen_data['fin_periodo'])

    logging.info(f'min p_week: {nielsen_data['p_week'].min()}')
    logging.info(f'max p_week: {nielsen_data['p_week'].max()}')

    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').disabled = True

    regressors: dict[str, dict[str, Prophet]] = defaultdict(dict)
    final_pred = pd.DataFrame()

    target_values = [
                'mercado_vtas_valor',
                'mercado_vtas_unit',
                'unimarc_vtas_valor',
                'unimarc_vtas_unit'
            ]
    logging.info('Start trainning')
    for category_names in batchList(
        nielsen_data['cl_xc_categoria'].drop_duplicates().to_list(),
        batch_size=10
    ):
        for category_name in category_names:
            for target_value in target_values:
                # Handle missing values
                if nielsen_data[
                    nielsen_data['cl_xc_categoria'] == category_name
                ][target_value].isna().all():
                    regressors[category_name][target_value] = None
                    continue

                print(f'Trainning {category_name} {target_value} regressor')
                regressor = Prophet(
                    yearly_seasonality=True,
                    weekly_seasonality=False,
                    daily_seasonality=False,
                    interval_width=.95,
                    mcmc_samples=500,
                    holidays=holidays[[
                        'fin_periodo', 'title'
                    ]].sort_values(
                        'fin_periodo'
                    ).rename(columns={
                        'fin_periodo': 'ds',
                        'title': 'holiday'
                    })
                ).add_seasonality(
                    name='monthly',
                    period=30.5,
                    fourier_order=5,
                )

                regressor = regressor.fit(
                    nielsen_data[
                        nielsen_data['cl_xc_categoria'] == category_name
                    ][[
                        'fin_periodo', target_value
                    ]].sort_values(
                        'fin_periodo'
                    ).rename(columns={
                        'fin_periodo': 'ds',
                        target_value: 'y'
                    }),
                )

                regressors[category_name][target_value] = regressor

            periods = regressor.make_future_dataframe(
                periods=8,
                freq='W',
                include_history=True
            ).rename(columns={
                'ds': 'fin_periodo',
            })

            for target_value in regressors[category_name]:
                # Set current regressor
                print(f'Predicting {category_name} {target_value} regressor')
                regressor = regressors[category_name][target_value]

                if regressor is None:
                    pred = periods['fin_periodo'].to_frame().copy()
                    pred[f'{target_value}_proyectado'] = None
                    pred[f'{target_value}_proyectado_min'] = None
                    pred[f'{target_value}_proyectado_max'] = None

                else:
                    future = regressor.make_future_dataframe(
                        periods=8,
                        freq='W',
                        include_history=True
                    )
                    pred = regressor.predict(
                        df=future
                    )[[
                        'ds', 'yhat', 'yhat_lower', 'yhat_upper'
                    ]].rename(columns={
                        'ds': 'fin_periodo',
                        'yhat': f'{target_value}_proyectado',
                        'yhat_lower': f'{target_value}_proyectado_min',
                        'yhat_upper': f'{target_value}_proyectado_max',
                    })

                pred['fin_periodo'] = pd.to_datetime(pred['fin_periodo'])

                pred = pred.merge(
                    nielsen_data[
                        nielsen_data['cl_xc_categoria'] == category_name
                    ][[
                        'fin_periodo', target_value,
                    ]],
                    on='fin_periodo',
                    how='left'
                )

                periods = periods.merge(
                    pred,
                    on='fin_periodo',
                    how='inner'
                )

            periods['cl_xc_categoria'] = category_name
            periods = periods.merge(
                nielsen_data[
                    nielsen_data['cl_xc_categoria'] == category_name
                ][[
                    'departamento', 'cl_xc_categoria', 'negocio'
                ]].drop_duplicates(),
                on='cl_xc_categoria',
                how='inner'
            )

            final_pred = pd.concat(
                [final_pred, periods],
                axis=0,
                ignore_index=True
            )


        final_pred['p_week'] = (
            final_pred['fin_periodo'].astype(str).str[:4]
            + pd.to_datetime(final_pred['fin_periodo']).dt.isocalendar()['week'].astype(str).str.zfill(2)  # noqa: E501
        )

        final_pred['inicio_periodo'] = final_pred['fin_periodo'] + pd.to_timedelta(-7, 'days')

    logging.info('Trainning ended')

    logging.info('Updating temporal table')
    wr.s3.to_csv(
        df=final_pred.sort_values('fin_periodo')[[
            'departamento', 'cl_xc_categoria', 'negocio',
            *[
                x
                for target_value in target_values
                for x in (
                    target_value,
                    f'{target_value}_proyectado',
                    f'{target_value}_proyectado_min',
                    f'{target_value}_proyectado_max'
                )
            ],
            'inicio_periodo', 'fin_periodo', 'p_week'
        ]].astype({
            'cl_xc_categoria': 'string',
            'inicio_periodo': 'string',
            'fin_periodo': 'string',
            'p_week': 'string',
        }),
        path=(
            's3://smu-datalake-test-athena-query-results/'
            'ecastrot/'
            'tmp_market_share_proyection/'
            'proyection.csv'
        ),
        index=None,
        header=None,
        sep='|',
        boto3_session=boto3_session,
        use_threads=True,
    )


if __name__ == '__main__':
    main()
