# Default
from __future__ import annotations

# Pip
import logging
import argparse
from io import BytesIO
from logging import config

import pandas as pd
import pendulum  # noqa: F401
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

import common.gcp_extended.bigquery as gbq_extended  # noqa: F401

# Own
import common.office365_extended.sharepoint as sp
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import readBigQuery, createTableAsSelect
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)
parser.add_argument(
    '--store_banner', type=str,
    help='Store banner'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'get_dates':
    """
    SELECT
        MIN(FECHA_INICIO_DE_PROMOCION) AS FECHA_MIN,
        MAX(FECHA_FIN_DE_PROMOCION) AS FECHA_MAX,
        MAX(MES_PROMOCION) AS MES_PROMOCION_MAX
    FROM `${gcp_project}.${schema}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}`
    """,  # noqa: E501

    'subcat_um':
    """
    select
        sub_category_code,
        weight_um,
        sub_cat_UM,
        Venta_Bruta,
        ranking
    from (
        select
            sub_category_code,
            weight_um,
            sub_cat_UM,
            Venta_Bruta,
            rank() over (partition by sub_category_code order by Venta_Bruta desc) as ranking

    from ( --A
        select
            sub_category_code,
            weight_um,
            PRODUCT_H.sub_cat_UM,
            sum(SALES_ITEM.value) as Venta_Bruta
        FROM `${gcp_project}.${schema}.VW_SALES_ITEM` AS SALES_ITEM

        INNER JOIN (
            select distinct
                --PRODUCT_H.product_id
                PRODUCT_H.EAN as upc,
                LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code,
                PRODUCT_H.NM as product_description,
                CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight,
                CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit,
                PRODUCT_H.GRUPO_ID as sub_category_code,
                PRODUCT_H.UM_CONTENIDO as weight_um,
                concat(GRUPO_ID, UM_CONTENIDO) as sub_cat_UM,
                PRODUCT_H.GRUPO_DSC as sub_category_description,
                PRODUCT_H.CAT_ID as category_code,
                PRODUCT_H.CAT_DSC as category_description,
                PRODUCT_H.LIN_DESC as department_description,
                PRODUCT_H.LIN_H_DSC as department_description_h
            from `${gcp_project}.${schema}.VW_DIM_PRODUCT` as PRODUCT_H
        ) AS PRODUCT_H
        ON SALES_ITEM.ean = PRODUCT_H.upc

        left join `${gcp_project}.${schema}.VW_DIM_STORE_HIERARCHY` st
        on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

        where --CAMBIAR FECHAS
            cast(SALES_ITEM.transaction_date as DATE) >= cast('${result_date}' as date)
            and cast(SALES_ITEM.transaction_date as DATE) < cast('${min_date}' as date)
            and SALES_ITEM.value > 0
            and cast(coalesce(st.org_ip_id, '0') as int) = ${banner_nro}
            and st.org_ip_id <> 'None'
        group by 1,2,3
        ) a
    ) B
    where ranking = 1
    """,

    'clientes_subcat_unidades_sin_promo':
    """
    WITH SALES_DISCOUNT AS (
        SELECT
            A.TXN_KEY AS BASKET_ID,
            A.EAN AS UPC,
            A.DPR_CODDCTO AS GEOPROMOTION_ID,
            A.DPR_MONTO AS DISCOUNT_VALUE,
            A.FECHA_DESCUENTO
        FROM (
            SELECT
                A.TXN_KEY,
                TRIM(D.EAN) AS EAN,
                C.PROM_GROUP_ID AS DPR_CODDCTO,
                DATE(A.ITM_TXN_TMS) AS FECHA_DESCUENTO,
                SUM(B.DCN_AMT) AS DPR_MONTO
            FROM ${gcp_project_1}.${schema_1}.DW_VW_FACT_ITM_TXN A

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_FACT_ITEM_TRANSACTION_DISCT B
            ON A.ITEM_TRANSACTION_KEY = B.ITEM_TRANSACTION_KEY

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_DIM_PROMOTIONAL_GROUP C
            ON B.PROM_CODE_KEY = C.PROM_GROUP_KEY

            JOIN (
                SELECT PRODUCT_KEY, EAN
                FROM ${gcp_project_1}.${schema_1}.DW_VW_DIM_PRODUCT
            ) D
            ON A.PRODUCT_KEY_1 = D.PRODUCT_KEY

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_FACT_MKT_BSKT E
            ON
                A.MARKET_BASKET_KEY = E.MARKET_BASKET_KEY
                AND SUBSTRING(A.TXN_KEY, INSTR(A.TXN_KEY, '-', -1) + 1)  = E.POS_HEX

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_DIM_STORE F
            ON
                A.STORE_KEY = F.STORE_KEY
                AND F.ORG_IP_ID IN ('01', '04', '09', '02', '08', '06')

            WHERE
                DATE(A.ITM_TXN_TMS) >= '${result_date}'
                AND DATE(A.ITM_TXN_TMS) <= '${max_date}'
                AND D.EAN IS NOT NULL
            GROUP BY A.TXN_KEY, TRIM(D.EAN), C.PROM_GROUP_ID, DATE(A.ITM_TXN_TMS)
        ) A
    ),
    PROMO_LOOKUP AS (
        SELECT
            ID as GEO_PROMOTION_ID,
            ALTERNATIVEID AS WKF_PROMOTION_ID,
            LPAD(CAST(FORMATO AS STRING), 2, '0') AS FORMATO
        FROM `${gcp_project_1}.${schema_1}.DW_VW_FACT_GP_PROMOTIONS`
        WHERE
            CAST(FECHA_CARGA AS DATE) = '${fecha_carga}'
            AND FORMATO = ${banner_nro}
    ),
    clientes_promo as (
        select customer_key
        from ( --a
            select
                SALES_ITEM.customer_key,
                product_code as material,
                SALES_ITEM.transaction_date
            FROM `${gcp_project_2}.${schema_2}.VW_SALES_ITEM` AS SALES_ITEM

            INNER JOIN (
            select
                PRODUCT_H.EAN as upc,
                LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code
            from `${gcp_project_2}.${schema_2}.VW_DIM_PRODUCT` as PRODUCT_H
            group by 1,2
            ) AS PRODUCT_H
            ON SALES_ITEM.ean = PRODUCT_H.upc
            where
                cast(SALES_ITEM.transaction_date as date) >= cast('${min_date}' as date)
                and cast(SALES_ITEM.transaction_date as date) <= cast('${max_date}' as date) --Periodo de promociones
            group by 1,2,3
        ) a

        join (
            SELECT
                distinct cast(material as STRING) as material,
                nombre_promocion as nombre_apo,
                cast(fecha_inicio_de_promocion as date) as inicio_promo,
                cast(fecha_fin_de_promocion as date) as fin_promo
            FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW`
            WHERE registro_valido = 'X'
                and organizacion_ventas = '3000'
                and canal_distribucion = '10'
                and cast(fecha_fin_de_promocion as date) >= cast('${month_date}' as date)
        ) apo
        on
            apo.material = a.material
            and a.transaction_date >= apo.inicio_promo
            and a.transaction_date <= apo.fin_promo
        group by 1
    )

    select
        customer_key,
        sub_category_description,
        category_description,
        department_description,
        department_description_h,
        Unidades_Medida_subcat,
        Venta_Bruta_subcat,
        Cantidad_subcat,
        Trx_sin_Promo,
        Trx_con_Promo,
        Compra_en_Promo_y_no_Promo,
        trx_subcat,
        trx_cat,
        Mean_U_sin_Promo,
        Stddev_U_sin_Promo,
        Mean_U_con_Promo,
        Stddev_U_con_Promo

    from ( --C
        select
            customer_key,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            --transaction_date,
            TIENE_PROMO,
            --Unidades_Medida,
            --Venta_Bruta,
            --Cantidad,
            Unidades_Medida_subcat,
            Venta_Bruta_subcat,
            Cantidad_subcat,
            sum(case when TIENE_PROMO = '0' then 1 else 0 end) OVER (PARTITION BY customer_key,sub_category_description) as Trx_sin_Promo,
            sum(case when TIENE_PROMO = '1' then 1 else 0 end) OVER (PARTITION BY customer_key,sub_category_description) as Trx_con_Promo,
            case
                when sum(case when TIENE_PROMO = '0' then 1 else 0 end) OVER (PARTITION BY customer_key,sub_category_description) > 0
                and sum(case when TIENE_PROMO = '1' then 1 else 0 end) OVER (PARTITION BY customer_key,sub_category_description) > 0 then 'Si' else 'No'
            end as Compra_en_Promo_y_no_Promo,
            sum(is_trx_subcat) over (partition by customer_key,sub_category_description) as trx_subcat,
            sum(is_trx_subcat) over (partition by customer_key, category_description) as trx_cat,
            Mean_U,
            Stddev_U,
            max(case when TIENE_PROMO = '0' then Mean_U end) over (Partition by customer_key,sub_category_description) as Mean_U_sin_Promo,
            max(case when TIENE_PROMO = '0' then Stddev_U end) over (Partition by customer_key,sub_category_description) as Stddev_U_sin_Promo,
            max(case when TIENE_PROMO = '1' then Mean_U end) over (Partition by customer_key,sub_category_description) as Mean_U_con_Promo,
            max(case when TIENE_PROMO = '1' then Stddev_U end) over (Partition by customer_key,sub_category_description) as Stddev_U_con_Promo,
            case
                when Unidades_Medida >= max(case when TIENE_PROMO = '0' then Mean_U end)
                    over (Partition by customer_key,sub_category_description,category_description,department_description,department_description_h)
                    + max(case when TIENE_PROMO = '0' then Stddev_U end)
                    over (Partition by customer_key,sub_category_description,category_description,department_description,department_description_h)
                and TIENE_PROMO = '1' then 'Si' else 'No'
            end as SobreAbastecimiento,
            case
                when row_number() over (
                    partition by customer_key,
                    sub_category_description,
                    category_description,
                    department_description,
                    department_description_h
                    order by transaction_date
                ) = 1 then 1 else 0
            end as fila

    from (--B
        Select
            customer_key,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            transaction_date,
            TIENE_PROMO,
            Unidades_Medida,
            Venta_Bruta,
            Cantidad,
            sum(Unidades_Medida) OVER (PARTITION BY customer_key,sub_category_description) as Unidades_Medida_subcat,
            --row_number() over (PARTITION BY customer_key,sub_category_description,TIENE_PROMO,transaction_date ORDER BY transaction_date) as row,
            sum(Venta_Bruta) OVER (PARTITION BY customer_key,sub_category_description) as Venta_Bruta_subcat,
            sum(Cantidad) OVER (PARTITION BY customer_key,sub_category_description) as Cantidad_subcat,
            case when row_number() over (PARTITION BY customer_key,transaction_date,sub_category_description) = 1 then 1 else 0 end as is_trx_subcat,
            case
                when row_number() over (
                    PARTITION BY customer_key,
                    sub_category_description,
                    TIENE_PROMO,
                    transaction_date
                    ORDER BY transaction_date
                ) = 1 then avg(Unidades_Medida) over (
                    Partition by customer_key,
                    sub_category_description,
                    TIENE_PROMO
                )
            end as Mean_U,
            case
                when row_number() over (
                    PARTITION BY customer_key,
                    sub_category_description,
                    TIENE_PROMO,
                    transaction_date
                    ORDER BY transaction_date
                ) = 1 then stddev(Unidades_Medida) over (
                    Partition by customer_key,
                    sub_category_description,
                    TIENE_PROMO
                )
            end as Stddev_U

    from ( --A2
        Select
            customer_key,
            transaction_date,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            case
                when TIENE_PROMO_WF = '1' then '1'
                when discount_value = 0
                and material_sin_geo is not null
                and producto_pesable = 'Si' then '1'
                else '0'
            end as TIENE_PROMO,
            sum(
                case
                    when sales_weight > 0 then CAST(sales_weight AS NUMERIC) * CAST(SALES_UNIT AS NUMERIC) * quantity_su
                    when weight > 0 then CAST(weight AS NUMERIC) * CAST(SALES_UNIT AS NUMERIC) * quantity_su
                    else quantity_su
                    end
                ) as Unidades_Medida,
            sum(value) as Venta_Bruta,
            sum(discount_value) as Descuento,
            sum(quantity) as Cantidad

    from ( --A1
        select
            customer_key,
            transaction_date,
            a.material,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            weight,
            sales_weight,
            SALES_UNIT,
            quantity,
            quantity_su,
            value,
            discount_value,
            TIENE_PROMO_WF,
            producto_pesable,
            sin_geo.material as material_sin_geo,
            rank() over (
                partition by customer_key,
                basket_id,
                transaction_date,
                a.material
                order by sin_geo.nombre_promocion
            ) as rnk

    from ( --A
        Select
            SALES_ITEM.customer_key,
            SALES_ITEM.transaction_date,
            product_code as material,
            SALES_ITEM.txn_key as basket_id,
            PRODUCT_H.sub_category_description,
            PRODUCT_H.category_description as category_description,
            PRODUCT_H.department_description,
            PRODUCT_H.department_description_h,
            cast(coalesce(st.org_ip_id, '0') as int) as org_id,
            CASE
                WHEN WORKFLOW.descripcion_evento_promocional is null then '0' else '1'
            end as TIENE_PROMO_WF,
            PRODUCT_H.weight,
            SALES_ITEM.weight as sales_weight,
            PRODUCT_H.SALES_UNIT,
            SALES_ITEM.quantity,
            SALES_ITEM.quantity_su,
            SALES_ITEM.value,
            SALES_ITEM.discount_value,
            case
                when SALES_ITEM.weight > 0 then 'Si' else 'No'
            end as producto_pesable
        FROM `${gcp_project_2}.${schema_2}.VW_SALES_ITEM` AS SALES_ITEM

        LEFT JOIN SALES_DISCOUNT AS SALES_DSCT
        ON (
            SALES_ITEM.txn_key = SALES_DSCT.basket_id
            AND SALES_ITEM.ean = SALES_DSCT.upc
        )

        LEFT JOIN PROMO_LOOKUP
        ON SALES_DSCT.geopromotion_id = PROMO_LOOKUP.GEO_PROMOTION_ID

        INNER JOIN (
            select distinct
                -- PRODUCT_H.product_id
                PRODUCT_H.EAN as upc,
                LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code,
                PRODUCT_H.NM as product_description,
                CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight,
                CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit,
                concat(GRUPO_ID, UM_CONTENIDO) as sub_cat_UM,
                PRODUCT_H.GRUPO_DSC as sub_category_description,
                PRODUCT_H.CAT_ID as category_code,
                PRODUCT_H.CAT_DSC as category_description,
                PRODUCT_H.LIN_DESC as department_description,
                PRODUCT_H.LIN_H_DSC as department_description_h
            from `${gcp_project_2}.${schema_2}.VW_DIM_PRODUCT` as PRODUCT_H
        ) AS PRODUCT_H
        ON SALES_ITEM.ean = PRODUCT_H.upc

        join `${gcp_project_2}.${schema_3}.TMP_SUBCAT_UM_${upper_store_banner}` um
        on um.sub_cat_UM = PRODUCT_H.sub_cat_UM

        LEFT JOIN (
            SELECT
                id_workflow,
                ean,
                nombre_promocion,
                descripcion_evento_promocional,
                descripcion_mecanica
            FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW`
            WHERE registro_valido = 'X'
            GROUP BY 1,2,3,4,5
        ) AS WORKFLOW
        ON (
            PROMO_LOOKUP.WKF_PROMOTION_ID = WORKFLOW.id_workflow
            AND SALES_ITEM.ean = WORKFLOW.ean
        )

        left join `${gcp_project_2}.${schema_2}.VW_FACT_MARKET_BASKET_E_COMMERCE` e
        on e.market_basket_key = SALES_ITEM.market_basket_key

        left join `${gcp_project_2}.${schema_2}.VW_DIM_STORE_HIERARCHY` st
        on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

        left join (
            select --es necesario sacar outlier?
                Customer_id,
                organization_id,
                mes
            from `${gcp_project_2}.${schema_2}.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER` o

        inner join (
            select
                date_trunc(cast(date_value as date),MONTH) as mes,
                MAX(FORMAT_DATE('%G%V', DATE_VALUE)) as Semana
            from `${gcp_project_2}.${schema_2}.VW_DIM_DATE`
            group by 1
        ) s
        on cast(o.week_iso_id as STRING) = s.Semana
        where organization_id=${org_bdf}

        ) o
        on
            o.customer_id = SALES_ITEM.customer_key
            and date_trunc(cast(transaction_date as date),MONTH) = o.mes --CAMBIAR TABLA

        inner join (
            select
                Customer_key from clientes_promo
        ) apo
        on apo.customer_key=SALES_ITEM.customer_key

        where --CAMBIAR FECHAS
            cast(SALES_ITEM.transaction_date as DATE)>=cast('${result_date}' as date)
            and cast(SALES_ITEM.transaction_date as DATE)<cast('${min_date}' as date)--1 año antes de Promo a analizar
            and e.market_basket_key is null
            and SALES_ITEM.value > 0
            and cast(coalesce(st.org_ip_id, '0') as int) = ${banner_nro} --M10
            and st.org_ip_id <> 'None'
            and o.Customer_id is null
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 --limit 20
    ) a

    left join (
        SELECT
            cast(w.material as STRING) as material,
            nombre_promocion,
            cast(fecha_inicio_de_promocion as date) as inicio_promo,
            cast(fecha_fin_de_promocion as date) as fin_promo
        FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w --limit 100

        LEFT JOIN PROMO_LOOKUP
        on PROMO_LOOKUP.WKF_PROMOTION_ID = w.id_workflow

        WHERE
            registro_valido = 'X'
            and organizacion_ventas = '3000'
            and canal_distribucion = '10'
            and w.descripcion_mecanica not in (
                'APP UNI MASIVAS',
                'APP UNI PERSONALIZADA',
                'APP UNI PROFUNDIZACION'
            )
            and w.descripcion_evento_promocional not in (
                'SOLPROM/LIQUIDACION',
                'SOLPROM/LIQUIDACION_FUERA_DE_SURTIDO',
                'SOLPROM/SOBRESTOCK'
            )
            and w.desc_promocion = 'PRECIO FIJO'
        group by 1,2,3,4
    ) sin_geo
    on
        sin_geo.material = a.material
        and a.transaction_date >= sin_geo.inicio_promo
        and a.transaction_date <= sin_geo.fin_promo
    ) a1
    where rnk = 1
    group by 1,2,3,4,5,6,7,8
        ) a2
    ) b
    ) C
    where fila = 1
    """, # noqa: E501

    'sobrestock':
    """
    WITH SALES_DISCOUNT AS (
        SELECT
            A.TXN_KEY AS BASKET_ID,
            A.EAN AS UPC,
            A.DPR_CODDCTO AS GEOPROMOTION_ID,
            A.DPR_MONTO AS DISCOUNT_VALUE,
            A.FECHA_DESCUENTO
        FROM (
            SELECT
                A.TXN_KEY,
                TRIM(D.EAN) AS EAN,
                C.PROM_GROUP_ID AS DPR_CODDCTO,
                DATE(A.ITM_TXN_TMS) AS FECHA_DESCUENTO,
                SUM(B.DCN_AMT) AS DPR_MONTO
            FROM ${gcp_project_1}.${schema_1}.DW_VW_FACT_ITM_TXN A

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_FACT_ITEM_TRANSACTION_DISCT B
            ON A.ITEM_TRANSACTION_KEY = B.ITEM_TRANSACTION_KEY

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_DIM_PROMOTIONAL_GROUP C
            ON B.PROM_CODE_KEY = C.PROM_GROUP_KEY

            JOIN (
                SELECT PRODUCT_KEY, EAN
                FROM ${gcp_project_1}.${schema_1}.DW_VW_DIM_PRODUCT
            ) D
            ON A.PRODUCT_KEY_1 = D.PRODUCT_KEY

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_FACT_MKT_BSKT E
            ON
                A.MARKET_BASKET_KEY = E.MARKET_BASKET_KEY
                AND SUBSTRING(A.TXN_KEY, INSTR(A.TXN_KEY, '-', -1) + 1)  = E.POS_HEX

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_DIM_STORE F
            ON A.STORE_KEY = F.STORE_KEY AND F.ORG_IP_ID IN ('01', '04', '09', '02', '08', '06')

            WHERE
                DATE(A.ITM_TXN_TMS) >= '${min_date}'
                AND DATE(A.ITM_TXN_TMS) <= '${max_date}'
                AND D.EAN IS NOT NULL
            GROUP BY A.TXN_KEY, TRIM(D.EAN), C.PROM_GROUP_ID, DATE(A.ITM_TXN_TMS)
        ) A
    ),
    PROMO_LOOKUP AS (
        SELECT
            ID as GEO_PROMOTION_ID,
            ALTERNATIVEID AS WKF_PROMOTION_ID,
            LPAD(CAST(FORMATO AS STRING), 2, '0') AS FORMATO
        FROM `${gcp_project_1}.${schema_1}.DW_VW_FACT_GP_PROMOTIONS`
        WHERE
            CAST(FECHA_CARGA AS DATE) = '${fecha_carga}'
            AND FORMATO = ${banner_nro}
    )
    select
        Mes_promocion,
        TIPO_PROMOCION,
        nombre_promocion_ppal,
        nombre_promocion,
        descripcion_mecanica,
        sub_category_description,
        category_description,
        department_description,
        department_description_h,
        org_id,
        SobreAbastecimiento,
        sum(Unidades_Medida) as Unidades_Medida,
        sum(Unidades_Medida_SobreAbastecimiento) as Unidades_Medida_SobreAbastecimiento,
        sum(Venta_Bruta) as Venta_Bruta,
        sum(Descuento) as Descuento,
        sum(Cantidad) as Cantidad,
        --sum(Clientes_mes_nombre_promo) as Clientes_mes_nombre_promo,
        --sum(Clientes_mes_tipo_promo) as Clientes_mes_tipo_promo,
        --sum(Clientes_mes) as Clientes_mes,
        count(distinct customer_key) as Clientes

    from ( --B
        select
            customer_key,
            transaction_date,
            REPLACE(nombre_promocion, ',', '.') as nombre_promocion,
            TIPO_PROMOCION,
            --nombre_promocion_ppal,
            case
                when nombre_promocion_ppal is not null then nombre_promocion_ppal
                when nombre_promocion_ppal is null and nombre_promocion_ppal_per_mat is not null then nombre_promocion_ppal_per_mat
                when nombre_promocion_ppal is null and nombre_promocion_ppal_apo_mat is not null then nombre_promocion_ppal_apo_mat
                when nombre_promocion_ppal is null and nombre_promocion_ppal_EA_mat is not null then nombre_promocion_ppal_EA_mat
                when nombre_promocion_ppal is null and nombre_promocion_ppal_cat_mat is not null then nombre_promocion_ppal_cat_mat
            end as nombre_promocion_ppal,
            Mes_promocion,
            descripcion_mecanica,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            Mean_U_sin_Promo,
            Stddev_U_sin_Promo,
            Unidades_Medida,
            Venta_Bruta,
            Descuento,
            Cantidad,
            case
                when Unidades_Medida > Mean_U_sin_Promo + coalesce(Stddev_U_sin_Promo, 0) and TIENE_PROMO = '1' then 'Si' else 'No'
            end as SobreAbastecimiento,
            case
                when Unidades_Medida > Mean_U_sin_Promo + coalesce(Stddev_U_sin_Promo, 0)
                and TIENE_PROMO = '1' then Unidades_Medida -(Mean_U_sin_Promo + coalesce(Stddev_U_sin_Promo, 0)) else 0
            end as Unidades_Medida_SobreAbastecimiento,
            case
                when row_number() over (partition by customer_key,Mes_promocion,nombre_promocion) = 1 then 1 else 0
            end as Clientes_mes_nombre_promo,
            case
                when row_number() over (partition by customer_key,Mes_promocion,descripcion_mecanica) = 1 then 1 else 0
            end as Clientes_mes_tipo_promo,
            case
                when row_number() over (partition by customer_key, Mes_promocion) = 1 then 1 else 0
            end as Clientes_mes

    from ( --A2
        Select
            customer_key,
            transaction_date,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            case
                when TIENE_PROMO_WF = '1' then '1'
                when discount_value = 0 and material_sin_geo is not null and producto_pesable = 'Si' then '1'
                when material_apo is not null
                /*and discount_value = 0
                and producto_pesable = 'Si'*/ then '1'
                else '0'
            end as TIENE_PROMO,
            --APO,
            Mean_U_sin_Promo,
            Stddev_U_sin_Promo,
            nombre_promocion,
            descripcion_mecanica,
            TIPO_PROMOCION,
            nombre_promocion_ppal,
            Mes_promocion,
            nombre_promocion_ppal_apo_mat,
            nombre_promocion_ppal_cat_mat,
            nombre_promocion_ppal_EA_mat,
            nombre_promocion_ppal_per_mat,
            case
                when sales_weight > 0 then CAST(sales_weight AS NUMERIC) * CAST(SALES_UNIT AS NUMERIC) * quantity
                when weight > 0 then CAST(weight AS NUMERIC) * CAST(SALES_UNIT AS NUMERIC) * quantity
                else quantity
            end as Unidades_Medida,
            value as Venta_Bruta,
            discount_value as Descuento,
            quantity as Cantidad

    from ( --A1
        select
            customer_key,
            transaction_date,
            basket_id,
            a.material,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            Mean_U_sin_Promo,
            Stddev_U_sin_Promo,
            weight,
            sales_weight,
            SALES_UNIT,
            quantity,
            value,
            discount_value,
            TIENE_PROMO_WF,
            --APO,
            a.nombre_promocion,
            a.nombre_promocion_ppal,
            a.descripcion_mecanica,
            TIPO_PROMOCION,
            a.nombre_promocion_apo,
            a.nombre_promocion_ppal_apo,
            descripcion_mecanica_apo,
            nombre_promocion_M10_CICLO,
            nombre_promocion_ppal_M10_CICLO,
            nombre_promocion_ELEGIDOS_AHORRO_M10,
            nombre_promocion_ppal_ELEGIDOS_AHORRO_M10,
            nombre_promocion_PERECIBLES_M10,
            nombre_promocion_ppal_PERECIBLES_M10,
            Mes_promocion,
            producto_pesable,
            APO,
            M10_CICLO,
            ELEGIDOS_AHORRO_M10,
            PERECIBLES_M10,
            sin_geo.material as material_sin_geo,
            apo_table.material as material_apo,
            max(apo_table.nombre_promocion_ppal_apo_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_apo_mat,
            max(apo_table.nombre_promocion_ppal_cat_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_cat_mat,
            max(apo_table.nombre_promocion_ppal_EA_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_EA_mat,
            max(apo_table.nombre_promocion_ppal_per_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_per_mat,
            dense_rank() over (
            partition by customer_key,
            basket_id,
            transaction_date,
            a.material
            order by sin_geo.nombre_promocion,TIENE_PROMO_WF desc, discount_value desc,a.nombre_promocion desc
            ) as rnk,
            rnk_1

    from (--a1
        select *
        from (--a1_1
            select
                a1_1.*,
                dense_rank() over (partition by customer_key,basket_id,transaction_date,material order by nombre_promocion desc) as rnk_1
            from ( --A
                Select
                    SALES_ITEM.customer_key,
                    SALES_ITEM.transaction_date,
                    product_code as material,
                    SALES_ITEM.txn_key as basket_id,
                    PRODUCT_H.sub_category_description,
                    PRODUCT_H.category_description as category_description,
                    PRODUCT_H.department_description,
                    PRODUCT_H.department_description_h,
                    cast(coalesce(st.org_ip_id, '0') as int) as org_id,
                    Mean_U_sin_Promo,
                    Stddev_U_sin_Promo,
                    CASE
                        WHEN WORKFLOW.descripcion_evento_promocional is null then '0' else '1'
                    end as TIENE_PROMO_WF,
                    /*CASE
                    WHEN WORKFLOW.descripcion_evento_promocional LIKE '%APOTEOSICO%'
                    and WORKFLOW.DESCRIPCION_EVENTO_PROMOCIONAL LIKE '%UNI%' then 1 else 0
                    end as APO,*/
                    nombre_promocion,
                    descripcion_mecanica,
                    TIPO_PROMOCION,
                    nombre_promocion_ppal,
                    Mes_promocion,
                    PRODUCT_H.weight,
                    SALES_ITEM.weight as sales_weight,
                    PRODUCT_H.SALES_UNIT,
                    SALES_ITEM.quantity,
                    SALES_ITEM.value,
                    SALES_ITEM.discount_value,
                    case
                        when SALES_ITEM.weight > 0 then 'Si' else 'No'
                    end as producto_pesable,
                    max(CASE WHEN TIPO_PROMOCION='M10 10 DE M10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_apo,
                    max(CASE WHEN TIPO_PROMOCION='M10 10 DE M10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_apo,
                    max(CASE WHEN TIPO_PROMOCION='M10 10 DE M10' then WORKFLOW.descripcion_mecanica else null end) as descripcion_mecanica_apo,
                    max(CASE WHEN TIPO_PROMOCION='M10 CICLO' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_M10_CICLO,
                    max(CASE WHEN TIPO_PROMOCION='M10 CICLO' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_M10_CICLO,
                    max(CASE WHEN TIPO_PROMOCION='ELEGIDOS AHORRO M10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_ELEGIDOS_AHORRO_M10,
                    max(CASE WHEN TIPO_PROMOCION='ELEGIDOS AHORRO M10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_ELEGIDOS_AHORRO_M10,
                    max(CASE WHEN TIPO_PROMOCION='PERECIBLES M10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_PERECIBLES_M10,
                    max(CASE WHEN TIPO_PROMOCION='PERECIBLES M10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_PERECIBLES_M10,
                    max(CASE WHEN TIPO_PROMOCION='M10 10 DE M10' then 1 else 0 end) as APO,
                    max(CASE WHEN TIPO_PROMOCION='M10 CICLO' then 1 else 0 end) as M10_CICLO,
                    max(CASE WHEN TIPO_PROMOCION='ELEGIDOS AHORRO M10' then 1 else 0 end) as ELEGIDOS_AHORRO_M10,
                    max(CASE WHEN TIPO_PROMOCION='PERECIBLES M10' then 1 else 0 end) as PERECIBLES_M10
                FROM `${gcp_project_2}.${schema_2}.VW_SALES_ITEM` AS SALES_ITEM

                LEFT JOIN SALES_DISCOUNT AS SALES_DSCT
                ON (
                SALES_ITEM.txn_key = SALES_DSCT.basket_id
                AND SALES_ITEM.ean = SALES_DSCT.upc
                )

                LEFT JOIN PROMO_LOOKUP
                ON SALES_DSCT.geopromotion_id = PROMO_LOOKUP.GEO_PROMOTION_ID

                INNER JOIN (
                select distinct
                    -- PRODUCT_H.product_id
                    PRODUCT_H.EAN as upc,
                    LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code,
                    PRODUCT_H.NM as product_description,
                    CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight,
                    CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit,
                    concat(GRUPO_ID, UM_CONTENIDO) as sub_cat_UM,
                    PRODUCT_H.GRUPO_DSC as sub_category_description,
                    PRODUCT_H.CAT_ID as category_code,
                    PRODUCT_H.CAT_DSC as category_description,
                    PRODUCT_H.LIN_DESC as department_description,
                    PRODUCT_H.LIN_H_DSC as department_description_h
                from `${gcp_project_2}.${schema_2}.VW_DIM_PRODUCT` as PRODUCT_H
                ) AS PRODUCT_H
                ON SALES_ITEM.ean = PRODUCT_H.upc --ARREGLO GCP

                join `${gcp_project_2}.${schema_3}.TMP_SUBCAT_UM_${upper_store_banner}` um
                on um.sub_cat_UM = PRODUCT_H.sub_cat_UM

                left JOIN (
                SELECT id_workflow,
                    ean,
                    w.nombre_promocion,
                    w.descripcion_evento_promocional,
                    w.descripcion_mecanica,
                    p.TIPO_PROMOCION,
                    nombre_promocion_ppal,
                    Mes_promocion,
                    w.fecha_inicio_de_promocion,
                    w.fecha_fin_de_promocion
                FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w

                join `${gcp_project_2}.${schema_3}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}` p
                on p.n_promocion = w.n_promocion

                WHERE registro_valido = 'X'
                --and Mes_promocion = cast('${month_date}' as date)
                GROUP BY 1,2,3,4,5,6,7,8,9,10
                ) AS WORKFLOW
                ON (
                    PROMO_LOOKUP.WKF_PROMOTION_ID = WORKFLOW.id_workflow
                    AND SALES_ITEM.ean = WORKFLOW.ean
                )

                left join `${gcp_project_2}.${schema_2}.VW_FACT_MARKET_BASKET_E_COMMERCE` e
                on e.market_basket_key = SALES_ITEM.market_basket_key

                left join `${gcp_project_2}.${schema_2}.VW_DIM_STORE_HIERARCHY` st
                on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

                left join (
                select --es necesario sacar outlier?
                    Customer_id,
                    organization_id,
                    mes
                from `${gcp_project_2}.${schema_2}.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER` o

                inner join (
                select
                    date_trunc(cast(date_value as date),MONTH) as mes,
                    MAX(FORMAT_DATE('%G%V', DATE_VALUE)) as Semana --ARREGLO GCP
                from `${gcp_project_2}.${schema_2}.VW_DIM_DATE`
                group by 1
                ) s
                on cast(o.week_iso_id as STRING) = s.Semana
                where organization_id=${org_bdf}
                ) o
                on
                    o.customer_id = SALES_ITEM.customer_key
                    and date_trunc(cast(transaction_date as date),MONTH) = o.mes --CAMBIAR FECHAS

                left join (
                select distinct
                    customer_key,
                    sub_category_description,
                    category_description,
                    Mean_U_sin_Promo,
                    Stddev_U_sin_Promo
                from `${gcp_project_2}.${schema_3}.TMP_CLIENTES_SUBCAT_UNIDADES_SIN_PROMO_${upper_store_banner}`
                ) sin_promo
                on
                    sin_promo.customer_key = SALES_ITEM.CUSTOMER_KEY
                    and PRODUCT_H.sub_category_description = sin_promo.sub_category_description
                    and PRODUCT_H.category_description = sin_promo.category_description
                where
                    cast(SALES_ITEM.transaction_date as DATE) >= cast('${min_date}' as date)
                    and cast(SALES_ITEM.transaction_date as DATE) < cast('${max_date}' as date) --1 año antes de Promo a analizar
                    and e.market_basket_key is null
                    and SALES_ITEM.value > 0
                    and cast(coalesce(st.org_ip_id, '0') as int) = ${banner_nro} --M10
                    and st.org_ip_id <> 'None'
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
            ) a1_1
        ) a1_2
    where rnk_1=1
    ) a

    left join (
    SELECT
        cast(w.material as STRING) as material,
        nombre_promocion,
        cast(fecha_inicio_de_promocion as date) as inicio_promo,
        cast(fecha_fin_de_promocion as date) as fin_promo
    FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w --limit 100

    LEFT JOIN PROMO_LOOKUP
    on PROMO_LOOKUP.WKF_PROMOTION_ID = w.id_workflow
    WHERE
        registro_valido = 'X'
        and organizacion_ventas = '3000'
        and canal_distribucion = '10'
        and w.descripcion_mecanica not in (
            'APP UNI MASIVAS',
            'APP UNI PERSONALIZADA',
            'APP UNI PROFUNDIZACION'
        )
        and w.descripcion_evento_promocional not in (
            'SOLPROM/LIQUIDACION',
            'SOLPROM/LIQUIDACION_FUERA_DE_SURTIDO',
            'SOLPROM/SOBRESTOCK'
        )
        and w.desc_promocion = 'PRECIO FIJO'
    group by 1,2,3,4
    ) sin_geo
    on
        sin_geo.material = a.material
        and a.transaction_date >= sin_geo.inicio_promo
        and a.transaction_date <= sin_geo.fin_promo

    left join (
    SELECT
        cast(material as STRING) as material,
        cast(w.fecha_inicio_de_promocion as date) as inicio_promo,
        cast(w.fecha_fin_de_promocion as date) as fin_promo,
        max(CASE WHEN p.TIPO_PROMOCION='M10 10 DE M10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_apo_mat,
        max(CASE WHEN p.TIPO_PROMOCION='M10 CICLO' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_cat_mat,
        max(CASE WHEN p.TIPO_PROMOCION='ELEGIDOS AHORRO M10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_EA_mat,
        max(CASE WHEN p.TIPO_PROMOCION='PERECIBLES M10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_per_mat
    FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w

    join `${gcp_project_2}.${schema_3}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}` p
    on p.n_promocion=w.n_promocion

    WHERE
        registro_valido = 'X'
        and organizacion_ventas='3000'
    group by 1,2,3--,4,5,6
    ) apo_table
    on
        apo_table.material=a.material
        and a.transaction_date>=apo_table.inicio_promo
        and a.transaction_date<=apo_table.fin_promo
    ) a1
    where rnk = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23--,24,25,26,27,28,29,30,31,32,33,34,35,36--,37
    ) a2
    ) b
    --where APO=1
    where nombre_promocion_ppal is not null
    group by 1,2,3,4,5,6,7,8,9,10,11
    """, # noqa: E501

    'pull_foward':
    """
    SELECT *
    FROM `${gcp_project}.${schema}.TMP_SOBRESTOCK_${upper_store_banner}`
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'halo_efect'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')

    gbq_client = Client()

    upper_store_banner = {
    'Alvi': 'ALVI',
    'Unimarc': 'UNIMARC',
    'Super 10': 'S10',
    'Mayorista': 'M10'
    }[store_banner]

    banner_nro = 2
    org_bdf = 4
    fecha_carga = execution_date

    logging.info(f'upper_store_banner: {upper_store_banner}')

    dates_df = readBigQuery(SQL_QUERIES['get_dates'].substitute(
        gcp_project = proyecto,
        schema = 'TMP',
        upper_store_banner = upper_store_banner
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    min_date, max_date, month_date = dates_df.iloc[0].to_list()

    result_date = pendulum.from_format(
        min_date.isoformat(),
        'YYYY-MM-DD'
    ).date().add(
        days=0,
        years=-1,
    ).strftime(
        '%Y-%m-%d'
    )

    min_date = min_date.strftime('%Y-%m-%d')
    max_date = max_date.strftime('%Y-%m-%d')
    month_date = month_date.strftime('%Y-%m-%d')

    logging.info(f'min_date: {min_date}')
    logging.info(f'max_date: {max_date}')
    logging.info(f'result_date: {result_date}')
    logging.info(f'month_date: {month_date}')

    _ = createTableAsSelect(
    query=SQL_QUERIES['subcat_um'].substitute(
        gcp_project = 'cl-bigdata-analytics-preprod',
        schema = 'CDA_VISTAS',
        banner_nro = banner_nro,
        result_date = result_date,
        min_date = min_date
    ),
    table_ref=f'{proyecto}.TMP.TMP_SUBCAT_UM_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    _ = createTableAsSelect(
    query=SQL_QUERIES['clientes_subcat_unidades_sin_promo'].substitute(
        gcp_project_1 = 'cl-cda-prod',
        schema_1 = 'DS_CDA_VW_SMU',
        gcp_project_2 = 'cl-bigdata-analytics-preprod',
        schema_2 = 'CDA_VISTAS',
        schema_3 = 'TMP',
        banner_nro = banner_nro,
        org_bdf = org_bdf,
        fecha_carga = fecha_carga,
        result_date = result_date,
        min_date = min_date,
        max_date = max_date,
        month_date = month_date,
        upper_store_banner = upper_store_banner
    ),
    table_ref=f'{proyecto}.TMP.TMP_CLIENTES_SUBCAT_UNIDADES_SIN_PROMO_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    _ = createTableAsSelect(
    query=SQL_QUERIES['sobrestock'].substitute(
        gcp_project_1 = 'cl-cda-prod',
        schema_1 = 'DS_CDA_VW_SMU',
        gcp_project_2 = 'cl-bigdata-analytics-preprod',
        schema_2 = 'CDA_VISTAS',
        schema_3 = 'TMP',
        banner_nro = banner_nro,
        org_bdf = org_bdf,
        fecha_carga = fecha_carga,
        min_date = min_date,
        max_date = max_date,
        month_date = month_date,
        upper_store_banner = upper_store_banner
    ),
    table_ref=f'{proyecto}.TMP.TMP_SOBRESTOCK_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    pull_foward = readBigQuery(SQL_QUERIES['pull_foward'].substitute(
        gcp_project = 'cl-bigdata-analytics-preprod',
        schema = 'TMP',
        upper_store_banner = upper_store_banner
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    excel_content = BytesIO()
    excel_writer = pd.ExcelWriter(
        excel_content,
    )

    pull_foward.to_excel(
        excel_writer,
        sheet_name='Sobrestock',
        index=False,
        header=True
    )

    excel_writer.close()
    excel_content.seek(0)

    sp.SharePointFile(
        **getSecret(
            'bdaa_sharepoint_credentials',
            proyecto,
        ),
        server_relative_path=(
            '/sites/'
            'BigDatayAdvancedAnalytics/'
            'Documentos%20compartidos/'
            'Evaluate_Promotions/'
            'sobrestock_m10.xlsx'
        )
    ).upload(excel_content)
    logging.info('Tabla subida en Sharepoint')

if __name__ == '__main__':
    main()
