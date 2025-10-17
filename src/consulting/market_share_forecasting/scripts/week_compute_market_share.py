from __future__ import annotations

# Default
import os
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
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
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
    FROM ${gcp_project}.MARKET_SHARE.NIELSEN_SEMANAL_VENTA_CATEGORIA

    UNION ALL

    SELECT
        'SIN CLASIFICAR' AS departamento
        ,'SIN CLASIFICAR' AS cl_xc_categoria
        ,'SIN CLASIFICAR' AS negocio
        ,'' AS periodos
        ,c.total_mercado_vtas_valor - COALESCE(cat.mercado_vtas_valor, 0) AS total_mercado_vtas_valor
        ,c.total_mercado_vtas_unit - COALESCE(cat.mercado_vtas_unit, 0) AS total_mercado_vtas_unit
        ,c.unimarc_vtas_valor - COALESCE(cat.unimarc_vtas_valor, 0) AS unimarc_vtas_valor
        ,c.unimarc_vtas_unit - COALESCE(cat.unimarc_vtas_unit, 0) AS unimarc_vtas_unit
        ,c.m10s10_vtas_valor - COALESCE(cat.m10s10_vtas_valor, 0) AS m10s10_vtas_valor
        ,c.m10s10_vtas_unit - COALESCE(cat.m10s10_vtas_unit, 0) AS m10s10_vtas_unit
        ,c.unimarc_internet_vtas_valor - COALESCE(cat.unimarc_internet_vtas_valor, 0) AS unimarc_internet_vtas_valor
        ,c.unimarc_internet_vtas_unit - COALESCE(cat.unimarc_internet_vtas_unit, 0) AS unimarc_internet_vtas_unit
        ,c.total_internet_vtas_valor - COALESCE(cat.total_internet_vtas_valor, 0) AS total_internet_vtas_valor
        ,c.total_internet_vtas_unit - COALESCE(cat.total_internet_vtas_unit, 0) AS total_internet_vtas_unit
        ,c.total_mercado_internet_vtas_valor - COALESCE(cat.total_mercado_internet_vtas_valor, 0) AS total_mercado_internet_vtas_valor
        ,c.total_mercado_internet_vtas_unit - COALESCE(cat.total_mercado_internet_vtas_unit, 0) AS total_mercado_internet_vtas_unit
        ,c.fin_periodo
        ,id_semana AS p_week

    FROM (
        SELECT
            *
            ,FORMAT_DATE('%G%V', DATE(fin_periodo)) AS id_semana

        FROM ${gcp_project}.MARKET_SHARE.NIELSEN_SEMANAL_VENTA_NEGOCIO

        WHERE negocio=''
    ) c

    LEFT JOIN (
        SELECT
            FORMAT_DATE('%G%V', DATE(fin_periodo)) AS id_semana
            ,SUM(total_mercado_vtas_valor) AS mercado_vtas_valor
            ,SUM(total_mercado_vtas_unit) AS mercado_vtas_unit
            ,SUM(unimarc_vtas_valor) AS unimarc_vtas_valor
            ,SUM(unimarc_vtas_unit) AS unimarc_vtas_unit
            ,SUM(m10s10_vtas_valor) AS m10s10_vtas_valor
            ,SUM(m10s10_vtas_unit) AS m10s10_vtas_unit
            ,SUM(unimarc_internet_vtas_valor) AS unimarc_internet_vtas_valor
            ,SUM(unimarc_internet_vtas_unit) AS unimarc_internet_vtas_unit
            ,SUM(total_internet_vtas_valor) AS total_internet_vtas_valor
            ,SUM(total_internet_vtas_unit) AS total_internet_vtas_unit
            ,SUM(total_mercado_internet_vtas_valor) AS total_mercado_internet_vtas_valor
            ,SUM(total_mercado_internet_vtas_unit) AS total_mercado_internet_vtas_unit

        FROM ${gcp_project}.MARKET_SHARE.NIELSEN_SEMANAL_VENTA_CATEGORIA

        GROUP BY 1
    ) CAT
    USING (id_semana)
    """,  # noqa: E501

    'holidays':
    """
    SELECT title, date
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE EXTRACT(YEAR FROM date) != ${year}

    UNION ALL

    SELECT title, date
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE
        EXTRACT(YEAR FROM date) = ${year}
        AND NOT REGEXP_CONTAINS(title, '(?i)eleccion')
    """
})


def fixFinPeriodo(row):
    if not pd.isna(row['fin_periodo']):
        return row['fin_periodo']

    date = pendulum.from_format(row['date'].strftime('%Y-%m-%d'), 'YYYY-MM-DD').date()

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
    user: str = 'week_' + args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']

    # Automatic
    boto3_session = Session(**getSecret(
        project=gcp_project,
        secret_name='bdaa_aws_credentials'  # noqa: S106
    ))

    # Constants
    gbq_client = Client()

    # ------------
    # Data loading
    # ------------
    logging.info('Loading data...')
    nielsen_data = gbq_extended.readBigQuery(
        query=SQL_QUERIES['nielsen_data'].substitute(
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    )

    holidays = gbq_extended.readBigQuery(
        query=SQL_QUERIES['holidays'].substitute(
            year=execution_date[:4],
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    )
    logging.info('Loading data...')

    holidays = pd.concat(
        [
            holidays[
                # TODO(ecastrot): Bad fix
                holidays['date'] != '2025-06-29'
            ],
            pd.DataFrame({
                'title': ['huelga_lider'],
                'date': ['2024-07-14'],
            })
        ],
        axis=0,
        ignore_index=True
    )

    nielsen_data['p_week'] = (
        pd.to_datetime(nielsen_data['fin_periodo']).dt.isocalendar()['year'].astype(str)
        + pd.to_datetime(nielsen_data['fin_periodo']).dt.isocalendar()['week'].astype(str).str.zfill(2)  # noqa: E501
    )

    holidays['date'] = pd.to_datetime(holidays['date'])
    holidays['p_week'] = (
        pd.to_datetime(holidays['date']).dt.isocalendar()['year'].astype(str)
        + pd.to_datetime(holidays['date']).dt.isocalendar()['week'].astype(str).str.zfill(2)
    )

    holidays = holidays.merge(
        nielsen_data[['p_week', 'fin_periodo']].drop_duplicates(),
        how='left',
        on='p_week'
    )

    holidays['fin_periodo'] = holidays.apply(fixFinPeriodo, axis=1)
    holidays = holidays.sort_values('title').drop_duplicates(subset='fin_periodo', keep='first')
    # TODO(ecastrot): Temporal fix holiday names
    holidays['title'] = holidays['title'].str.replace('yprotestantes', 'y_protestantes')

    nielsen_data['fin_periodo'] = pd.to_datetime(nielsen_data['fin_periodo'])

    logging.info(f'min p_week: {nielsen_data['p_week'].min()}')
    logging.info(f'max p_week: {nielsen_data['p_week'].max()}')

    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').disabled = True

    logging.info('Removing previous table from GCP')
    gbq_extended.createTableFromJSON(
        table_ddl_json_path=os.path.join('gbq_objects', 'week_prophet_sales_forecasting.json'),  # noqa: E501
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='rebuild',
    )


    target_values = [
        f'{x}_{y}'
        for x
        in [
            'total_mercado',
            'unimarc',
            'm10s10',
            'unimarc_internet',
            'total_internet',
            'total_mercado_internet'
        ]
        for y in [
            'vtas_valor',
            'vtas_unit'
        ]
    ]
    logging.info('Start trainning')
    for category_names in batchList(
        nielsen_data['cl_xc_categoria'].drop_duplicates().to_list(),
        batch_size=10
    ):
        regressors: dict[str, dict[str, Prophet]] = defaultdict(dict)
        final_pred = pd.DataFrame()

        for category_name in category_names:
            for target_value in target_values:
                # Handle missing values
                if nielsen_data[
                    nielsen_data['cl_xc_categoria'] == category_name
                ][target_value].notna().sum() < 50:
                    print(f'Skipping {category_name} {target_value} regressor')
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

                # Save last valid regressor for later
                last_valid_regressor = regressor

            periods = last_valid_regressor.make_future_dataframe(
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

                    # Boundary condition: values < 0 go to 0
                    pred[f'{target_value}_proyectado'] = pred[f'{target_value}_proyectado'].where(
                        pred[f'{target_value}_proyectado'] >= 0,
                        0
                    )
                    pred[f'{target_value}_proyectado_min'] = pred[f'{target_value}_proyectado_min'].where(  # noqa: E501
                        pred[f'{target_value}_proyectado_min'] >= 0,
                        0
                    )
                    pred[f'{target_value}_proyectado_max'] = pred[f'{target_value}_proyectado_max'].where(  # noqa: E501
                        pred[f'{target_value}_proyectado_max'] >= 0,
                        0
                    )

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

        logging.info('Updating temporal tables to AWS')
        for category_name in category_names:
            wr.s3.to_csv(
                df=final_pred[
                    final_pred['cl_xc_categoria'] == category_name
                ].sort_values(
                    'fin_periodo'
                )[[
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
                    'fact_week_market_share_proyection/'
                    f'proyection_{category_name}.csv'
                ),
                index=None,
                header=None,
                sep='|',
                boto3_session=boto3_session,
                use_threads=True,
            )

        logging.info('Updating temporal tables to GCP')
        for category_name in category_names:
            gbq_extended.uploadFrame(
                df=final_pred[
                    final_pred['cl_xc_categoria'] == category_name
                ].sort_values(
                    'fin_periodo'
                )[[
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
                table_ddl_json_path=os.path.join('gbq_objects', 'week_prophet_sales_forecasting.json'),  # noqa: E501
                project=gcp_project,
                gbq_client=gbq_client,
                if_exists='append',
            )

    logging.info('Trainning ended')


if __name__ == '__main__':
    main()
