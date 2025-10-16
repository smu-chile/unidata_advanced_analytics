from __future__ import annotations

# Default
import os
import logging
import argparse
from logging import config
from collections import defaultdict

# pip
import pandas as pd
from prophet import Prophet
from google.cloud.bigquery import Client
from pandas.tseries.offsets import MonthEnd

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.utils.data_transform import batchList


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
    'month_transactions':
    """
    SELECT
        FORMAT_DATE('%Y-%m', transaction_date) AS fin_periodo,
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

    FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

    INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
    USING (store_id)

    INNER JOIN (
        SELECT
            sku_product,
            MAX(neg_dsc) AS business_name,
            MAX(cat_dsc) AS category_description
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
        transaction_date >= DATE('${execution_month}-01') - INTERVAL 2 YEAR
        AND transaction_date < DATE('${execution_month}-01')
        AND itm_txn_fcn_tp_dsc = 'V'
        AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
        AND from_other_ecommerce IS NULL

    GROUP BY 1,2
    """,

    'holidays':
    """
    SELECT title, date
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE EXTRACT(YEAR FROM date) = ${year}

    UNION ALL

    SELECT title, date
    FROM `${gcp_project}.DATOS_GENERALES.DIM_HOLIDAYS` dim_holidays
    WHERE
        EXTRACT(YEAR FROM date) != ${year}
        AND NOT REGEXP_CONTAINS(title, '(?i)eleccion')
    """
})


# -------------------------------------------------------------------------
# Functions and classes
# -------------------------------------------------------------------------
def fixFinPeriodo(row):
    return row['fin_periodo'] if row['fin_periodo'] else row['date'].strftime('%Y-%m')


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
    gbq_client = Client()

    # ------------
    # Data loading
    # ------------
    logging.info('Loading data...')
    market_data = gbq_extended.readBigQuery(
        query=SQL_QUERIES['month_transactions'].substitute(
            execution_month=execution_date[:7],
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

    holidays = pd.concat(
        [
            holidays[
                # TODO(ecastrot): Bad fix
                holidays['date'] != '2025-06-29'
            ],
            pd.DataFrame({
                'title': ['huelga_lider'],
                'date': ['2024-07-14'],
                'essential': [False],
                'p_year': ['2024'],
            })
        ],
        axis=0,
        ignore_index=True
    )

    market_data['p_month'] = market_data['fin_periodo'].astype(str).str.replace('-', '').str[:6]
    market_data['p_year'] = market_data['fin_periodo'].astype(str).str[:4]

    holidays['date'] = pd.to_datetime(holidays['date'])
    holidays['p_month'] = holidays['date'].astype(str).str.replace('-', '').str[:6]

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

    gbq_extended.createTableFromJSON(
        table_ddl_json_path=os.path.join('gbq_objects', 'month_prophet_sales_forecasting.json'),  # noqa: E501
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='rebuild',
    )

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
                    logging.info(f'Skipping {category_name} {target_value} regressor')
                    regressors[category_name][target_value] = None
                    continue

                logging.info(f'Trainning {category_name} {target_value} regressor')
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
                logging.info(f'Predicting {category_name} {target_value} regressor')
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

        logging.info('Updating table to GCP')
        logging.info(', '.join(final_pred['category_description'].unique().tolist()))
        gbq_extended.uploadFrame(
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
