from __future__ import annotations

# Default
import os
import logging
import argparse
from logging import config
from collections import defaultdict

# pip
import pandas as pd
import awswrangler as wr
from boto3 import Session
from prophet import Prophet
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
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
    'day_transactions':
    """
    SELECT
        transaction_date AS fin_periodo,
        SUM(value) AS venta_unimarc

    FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

    INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
    USING (store_id)

    INNER JOIN (
        SELECT sku_product
        FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        GROUP BY 1
        HAVING MAX(neg_dsc) NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
    ) dim_product
    USING (sku_product)

    LEFT JOIN (
        SELECT
            market_basket_key,
            TRUE AS from_other_ecommerce
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
        WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
    ) external_ecommerce_filter
    USING (market_basket_key)

    WHERE
        transaction_date >= DATE('${execution_date}') - INTERVAL 2 YEAR
        AND transaction_date < DATE('${execution_date}')
        AND itm_txn_fcn_tp_dsc = 'V'
        AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
        AND store_banner = '${store_banner}'
        AND from_other_ecommerce IS NULL

    GROUP BY 1
    """,

    'holidays':
    """
    SELECT title, strdate
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE p_year != '${p_year}'

    UNION ALL

    SELECT title, strdate
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE
        p_year != '${p_year}'
        AND title NOT LIKE '%eleccion%'
    """
})


# -------------------------------------------------------------------------
# Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())

    # Environment
    user: str = 'day_' + args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']

    # Constants
    gbq_client = Client()

    # ------------
    # Data loading
    # ------------
    logging.info('Loading data...')
    market_data = gbq_extended.readBigQuery(
        query=SQL_QUERIES['day_transactions'].substitute(
            execution_date=execution_date,
            store_banner='Unimarc',
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    )
    market_data['fin_periodo'] = pd.to_datetime(market_data['fin_periodo'])

    holidays = gbq_extended.readBigQuery(
        query=SQL_QUERIES['holidays'].substitute(
            p_year=execution_date[:4]
        ),
        user=user,
        gbq_client=gbq_client,
    )

    holidays = pd.concat(
        [
            holidays[
                # TODO(ecastrot): Bad fix
                holidays['strdate'] != '2025-06-29'
            ],
            pd.DataFrame({
                'title': ['huelga_lider'],
                'strdate': ['2024-07-14'],
            })
        ],
        axis=0,
        ignore_index=True
    )

    holidays['fin_periodo'] = pd.to_datetime(holidays['strdate'])

    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').disabled=True

    regressors: dict[str, dict[str, Prophet]] = defaultdict(dict)
    final_pred = pd.DataFrame()

    target_values = ['venta_unimarc']

    for target_value in target_values:
        print(f'Trainning {target_value} regressor')
        regressor = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
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
            market_data[[
                'fin_periodo', target_value
            ]].sort_values(
                'fin_periodo'
            ).rename(columns={
                'fin_periodo': 'ds',
                target_value: 'y'
            }),
        )

        regressors[target_value] = regressor

    periods = regressor.make_future_dataframe(
        periods=60,
        freq='D',
        include_history=True
    ).rename(columns={
        'ds': 'fin_periodo',
    })

    for target_value in regressors:
        # Set current regressor
        print(f'Predicting {target_value} regressor')
        regressor = regressors[target_value]

        if regressor is None:
            pred = periods['fin_periodo'].to_frame().copy()
            pred[f'{target_value}_proyectado'] = None
            pred[f'{target_value}_proyectado_min'] = None
            pred[f'{target_value}_proyectado_max'] = None

        else:
            future = regressor.make_future_dataframe(
                periods=60,
                freq='D',
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
            pred[f'{target_value}_proyectado_min'] = pred[f'{target_value}_proyectado_min'].where(
                pred[f'{target_value}_proyectado_min'] >= 0,
                0
            )
            pred[f'{target_value}_proyectado_max'] = pred[f'{target_value}_proyectado_max'].where(
                pred[f'{target_value}_proyectado_max'] >= 0,
                0
            )

        pred['fin_periodo'] = pd.to_datetime(pred['fin_periodo'])

        pred = pred.merge(
            market_data[[
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

    final_pred = pd.concat(
        [final_pred, periods],
        axis=0,
        ignore_index=True
    )

    logging.info('Trainning ended')

    logging.info('Updating table to AWS')
    wr.s3.to_csv(
        df=final_pred.sort_values('fin_periodo')[[
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
            'fin_periodo'
        ]].astype({
            'fin_periodo': 'string',
        }),
        path=(
            's3://smu-datalake-test-athena-query-results/'
            'ecastrot/'
            'fact_day_market_share_proyection/'
            'proyection.csv'
        ),
        index=None,
        header=None,
        sep='|',
        boto3_session=Session(**getSecret(
            project=gcp_project,
            secret_name='bdaa_aws_credentials'  # noqa: S106
        )),
        use_threads=True,
    )

    logging.info('Updating table to GCP')
    gbq_extended.uploadFrame(
        df=final_pred.sort_values('fin_periodo')[[
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
            'fin_periodo'
        ]].astype({
            'fin_periodo': 'string',
        }),
        table_ddl_json_path=os.path.join('gbq_objects', 'day_prophet_sales_forecasting.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='replace',
    )


if __name__ == '__main__':
    main()
