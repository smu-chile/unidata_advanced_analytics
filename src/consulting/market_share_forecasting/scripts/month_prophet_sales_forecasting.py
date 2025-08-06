from __future__ import annotations

# Default
import os
import logging
import argparse
from logging import config
from collections import defaultdict

# pip
import pandas as pd
from boto3 import Session
from prophet import Prophet
from google.cloud.bigquery import Client
from pandas.tseries.offsets import MonthEnd

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.aws_extended.athena import readAthenaQuery
from common.utils.data_transform import batchList
from common.gcp_extended.bigquery import uploadFrame, createTableFromJSON
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
    'month_transactions_aws':
    """
    SELECT
        SUBSTR(transaction_date, 1, 7) AS fin_periodo,
        category_description,
        SUM(CASE
            WHEN store_banner = 'Unimarc' THEN value ELSE 0
        END) AS unimarc,
        SUM(CASE
            WHEN store_banner = 'Alvi' THEN value ELSE 0
        END) AS alvi,
        SUM(CASE
            WHEN store_banner = 'Super 10' THEN value ELSE 0
        END) AS s10,
        SUM(CASE
            WHEN store_banner = 'Mayorista' THEN value ELSE 0
        END) AS m10

    FROM dev_perm.TMP_LAB_SMU_SALES_ITEM sales_item

    INNER JOIN dev_perm.TMP_LAB_SMU_DIM_STORE dim_store
    USING (store_id)

    INNER JOIN (
        SELECT
            product_id,
            MAX(business_name) AS business_name,
            MAX(category_description) AS category_description
        FROM dev_perm.TMP_LAB_SMU_DIM_PRODUCTS
        GROUP BY 1
        HAVING MAX(business_name) NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
    ) e
    USING (product_id)

    LEFT JOIN (
        SELECT
            market_basket_key,
            TRUE AS from_other_ecommerce
        FROM dev_perm.TMP_LAB_SMU_FACT_MARKET_BASKET_E_COMMERCE
        WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
    ) external_ecommerce_filter
    USING (market_basket_key)

    WHERE
        transaction_date >= CAST(DATE_ADD('MONTH', -24, DATE('${execution_month}-01')) AS VARCHAR)
        AND transaction_date < CAST(DATE('${execution_month}-01') AS VARCHAR)
        AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
        AND itm_txn_fcn_tp_dsc = 'V'
        AND from_other_ecommerce IS NULL

    GROUP BY 1,2
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


# -------------------------------------------------------------------------
# Functions and classes
# -------------------------------------------------------------------------
def fixFinPeriodo(row):
    return row['fin_periodo'] if row['fin_periodo'] else row['strdate'].strftime('%Y-%m')


# -------------------------------------------------------------------------
# Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())

    # Environment
    user: str = 'month_' + args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']

    # Constants
    db_temp = 'dev_temp'
    gbq_client = Client()
    boto3_session = Session(**getSecret(
        project=gcp_project,
        secret_name='bdaa_aws_credentials'  # noqa: S106
    ))

    # Build output table
    createTableFromJSON(
        os.path.join('gbq_objects', 'month_prophet_sales_forecasting.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='rebuild'
    )

    # ------------
    # Data loading
    # ------------
    logging.info('Loading data...')
    market_data = readAthenaQuery(
        user=user,
        query=SQL_QUERIES['month_transactions_aws'].substitute(
            execution_month='2025-07',
        ),
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

    market_data['p_month'] = market_data['fin_periodo'].astype(str).str.replace('-', '').str[:6]
    market_data['p_year'] = market_data['fin_periodo'].astype(str).str[:4]

    holidays['strdate'] = pd.to_datetime(holidays['strdate'])
    holidays['p_month'] = holidays['strdate'].astype(str).str.replace('-', '').str[:6]

    holidays = holidays.merge(
        market_data[['p_month', 'fin_periodo']].drop_duplicates(),
        how='left',
        on='p_month'
    )

    holidays['fin_periodo'] = holidays['fin_periodo'].fillna('')
    holidays['fin_periodo'] = pd.to_datetime(holidays.apply(fixFinPeriodo, axis=1)) + MonthEnd(0)
    holidays = holidays.sort_values('title').drop_duplicates(subset='fin_periodo', keep='first')

    market_data['fin_periodo'] = pd.to_datetime(market_data['fin_periodo']) + MonthEnd(0)

    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').disabled=True

    target_values = ['unimarc', 'alvi', 's10', 'm10']

    for category_names in batchList(
        market_data['category_description'].drop_duplicates().to_list(),
        batch_size=10
    ):
        regressors: dict[str, dict[str, Prophet]] = defaultdict(dict)
        final_pred = pd.DataFrame()

        for category_name in category_names:
            for target_value in target_values:
                # Handle missing values
                if market_data[
                    market_data['category_description'] == category_name
                ][target_value].notna().sum() < 13:
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
                    # Leave commented. Holidays do not give any info to the
                    # model
                    #holidays=holidays[[  # noqa: ERA001
                    #    'fin_periodo', 'title'
                    #]].sort_values(
                    #    'fin_periodo'
                    #).rename(columns={
                    #    'fin_periodo': 'ds',  # noqa: ERA001
                    #    'title': 'holiday'
                    #})  # noqa: ERA001
                )

                regressor = regressor.fit(
                    market_data[
                        market_data['category_description'] == category_name
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
                periods=18,
                freq='M',
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
                        periods=18,
                        freq='M',
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
                    market_data[
                        market_data['category_description'] == category_name
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

            periods['category_description'] = category_name
            periods = periods.merge(
                market_data[
                    market_data['category_description'] == category_name
                ][[
                    'category_description'
                ]].drop_duplicates(),
                on='category_description',
                how='inner'
            )

            final_pred = pd.concat(
                [final_pred, periods],
                axis=0,
                ignore_index=True
            )


        final_pred['inicio_periodo'] = final_pred['fin_periodo'].astype(str).str[:8] + '01'

        logging.info('Updating temporal tables')

        uploadFrame(
            final_pred[[
                'category_description',
                'unimarc',
                'unimarc_proyectado',
                'unimarc_proyectado_min',
                'unimarc_proyectado_max',
                'alvi',
                'alvi_proyectado',
                'alvi_proyectado_min',
                'alvi_proyectado_max',
                's10',
                's10_proyectado',
                's10_proyectado_min',
                's10_proyectado_max',
                'm10',
                'm10_proyectado',
                'm10_proyectado_min',
                'm10_proyectado_max',
                'inicio_periodo',
                'fin_periodo'
            ]],
            table_ddl_json_path=os.path.join('gbq_objects', 'month_prophet_sales_forecasting.json'),  # noqa: E501
            project=gcp_project,
            gbq_client=gbq_client,
            if_exists='append',
        )

    logging.info('Trainning ended')


if __name__ == '__main__':
    main()
