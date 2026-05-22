# Default
from __future__ import annotations

# Pip
import logging
import argparse
from io import BytesIO
from logging import config

import pandas as pd
import pendulum
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

import common.gcp_extended.bigquery as gbq_extended  # noqa: F401

# Own
import common.office365_extended.sharepoint as sp
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    readBigQuery,
    setTableExpiration,
    createTableAsSelect,
)
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

    'promo_eval_behavior_last_12_months':
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
            ) D ON A.PRODUCT_KEY_1 = D.PRODUCT_KEY

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
                AND DATE(A.ITM_TXN_TMS) <= '${min_date}'
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
            AND FORMATO = ${banner_nro} --S10
    )

    select
        customer_key
        ,sub_category_description
        ,category_description
        ,department_description
        ,department_description_h
        ,Unidades_Medida_subcat
        ,Venta_Bruta_subcat
        ,Cantidad_subcat
        ,Trx_sin_Promo
        ,Trx_con_Promo
        ,Compra_en_Promo_y_no_Promo
        ,trx_subcat
        ,trx_cat
        ,Mean_U
        ,Stddev_U
        ,Mean_U_sin_Promo
        ,Stddev_U_sin_Promo
        ,Mean_U_con_Promo
        ,Stddev_U_con_Promo
        ,cast(Primera_compra_subcat as date) as Primera_compra_subcat
        ,cast(Ult_compra_subcat as date) as Ult_compra_subcat
        ,days_difference_subcat
        ,cast(days_difference_subcat as FLOAT64)/(cast(trx_subcat as FLOAT64)) as dias_entre_trx_subcat
        --,cast(days_difference_subcat as FLOAT64)/(cast(trx_subcat as FLOAT64)*cast(Mean_U as FLOAT64)) as dias_entre_UMxtrx_subcat
        ,cast(days_difference_subcat as FLOAT64)/(cast(Unidades_Medida_subcat as FLOAT64)) as dias_entre_UMxtrx_subcat
        ,DATE_TRUNC(cast(Ult_compra_subcat as date),MONTH) as Ult_compra_subcat_mes
        ,cast(Ult_compra_cat as date) as Ult_compra_cat
        ,DATE_TRUNC(cast(Ult_compra_cat as date),MONTH) as Ult_compra_cat_mes
        ,cast(Ult_compra_dep as date) as Ult_compra_dep
        ,DATE_TRUNC(cast(Ult_compra_dep as date),MONTH) as Ult_compra_dep_mes
        ,cast(Ult_compra_dep_h as date) as Ult_compra_dep_h
        ,DATE_TRUNC(cast(Ult_compra_dep_h as date),MONTH) as Ult_compra_dep_h_mes
        ,cast(Ult_compra_cliente as date) as Ult_compra_cliente
        ,DATE_TRUNC(cast(Ult_compra_cliente as date),MONTH) as Ult_compra_cliente_mes

    from (--C

    select
        customer_key
        ,sub_category_description
        ,category_description
        ,department_description
        ,department_description_h
        ,Primera_compra_subcat
        ,Ult_compra_subcat
        ,date_diff(cast(Ult_compra_subcat as date),cast(Primera_compra_subcat as date),DAY) AS days_difference_subcat
        ,Ult_compra_cat
        ,Ult_compra_dep
        ,Ult_compra_dep_h
        ,Ult_compra_cliente
        ,TIENE_PROMO
        --,Unidades_Medida
        --,Venta_Bruta
        --,Cantidad
        ,Unidades_Medida_subcat
        ,Venta_Bruta_subcat
        ,Cantidad_subcat

        ,sum(case when TIENE_PROMO='0' then 1 else 0 end)  OVER (PARTITION BY customer_key,sub_category_description,category_description) as Trx_sin_Promo
        ,sum(case when TIENE_PROMO='1' then 1 else 0 end)  OVER (PARTITION BY customer_key,sub_category_description,category_description) as Trx_con_Promo
        ,case when sum(case when TIENE_PROMO='0' then 1 else 0 end)  OVER (PARTITION BY customer_key,sub_category_description,category_description)>0 and sum(case when TIENE_PROMO='1' then 1 else 0 end)  OVER (PARTITION BY customer_key,sub_category_description,category_description)>0 then 'Si' else 'No' end as Compra_en_Promo_y_no_Promo
        ,sum(is_trx_subcat) over (partition by customer_key,sub_category_description,category_description) as trx_subcat
        ,sum(is_trx_subcat) over (partition by customer_key,category_description) as trx_cat
        ,Mean_U_subcat as Mean_U
        ,Stddev_U_subcat as Stddev_U
        ,max(case when TIENE_PROMO='0' then Mean_U end) over (Partition by customer_key, sub_category_description,category_description)  as Mean_U_sin_Promo
        ,max(case when TIENE_PROMO='0' then Stddev_U end) over (Partition by customer_key, sub_category_description,category_description) as Stddev_U_sin_Promo
        ,max(case when TIENE_PROMO='1' then Mean_U end) over (Partition by customer_key, sub_category_description,category_description)  as Mean_U_con_Promo
        ,max(case when TIENE_PROMO='1' then Stddev_U end) over (Partition by customer_key, sub_category_description,category_description) as Stddev_U_con_Promo

        ,case when Unidades_Medida>=max(case when TIENE_PROMO='0' then Mean_U end) over (Partition by customer_key, sub_category_description,category_description,department_description,department_description_h)+max(case when TIENE_PROMO='0' then Stddev_U end) over (Partition by customer_key, sub_category_description,category_description,department_description,department_description_h) and TIENE_PROMO='1' then 'Si' else 'No' end as SobreAbastecimiento
        ,case when row_number() over (partition by customer_key, sub_category_description,category_description,department_description,department_description_h order by transaction_date)=1 then 1 else 0 end as fila

    from ( --B
        Select
            customer_key
            ,sub_category_description
            ,category_description
            ,department_description
            ,department_description_h
            ,transaction_date
            ,TIENE_PROMO
            ,Unidades_Medida
            ,Venta_Bruta
            ,Cantidad
            ,sum(Unidades_Medida) OVER (PARTITION BY customer_key,sub_category_description,category_description) as Unidades_Medida_subcat
            ,row_number() over (PARTITION BY customer_key,sub_category_description,category_description,TIENE_PROMO,transaction_date ORDER BY transaction_date) as row
            ,sum(Venta_Bruta) OVER (PARTITION BY customer_key,sub_category_description,category_description) as Venta_Bruta_subcat
            ,sum(Cantidad) OVER (PARTITION BY customer_key,sub_category_description,category_description) as Cantidad_subcat
            ,case when row_number() over (PARTITION BY customer_key,transaction_date,sub_category_description,category_description)=1 then 1 else 0 end as is_trx_subcat

            , case when row_number() over (PARTITION BY customer_key,sub_category_description,category_description,TIENE_PROMO,transaction_date ORDER BY transaction_date)=1 then avg(Unidades_Medida) over ( Partition by customer_key, sub_category_description,TIENE_PROMO,category_description) end as Mean_U

            ,case when row_number() over (PARTITION BY customer_key,sub_category_description,category_description,TIENE_PROMO,transaction_date ORDER BY transaction_date)=1 then stddev(Unidades_Medida) over ( Partition by customer_key,sub_category_description,TIENE_PROMO,category_description) end as Stddev_U

            , case when row_number() over (PARTITION BY customer_key,sub_category_description,category_description,transaction_date ORDER BY transaction_date)=1 then avg(Unidades_Medida) over ( Partition by customer_key, sub_category_description,category_description) end as Mean_U_subcat

            ,case when row_number() over (PARTITION BY customer_key,sub_category_description,category_description,transaction_date ORDER BY transaction_date)=1 then stddev(Unidades_Medida) over ( Partition by customer_key,sub_category_description,category_description) end as Stddev_U_subcat

            ,max(transaction_date) over (PARTITION BY customer_key,sub_category_description,category_description) as Ult_compra_subcat
            ,min(transaction_date) over (PARTITION BY customer_key,sub_category_description,category_description) as Primera_compra_subcat
            ,max(transaction_date) over (PARTITION BY customer_key,category_description) as Ult_compra_cat
            ,max(transaction_date) over (PARTITION BY customer_key,department_description) as Ult_compra_dep
            ,max(transaction_date) over (PARTITION BY customer_key,department_description_h) as Ult_compra_dep_h
            ,max(transaction_date) over (PARTITION BY customer_key) as Ult_compra_cliente

    from ( -- A2
        Select
            customer_key,
            transaction_date,
            sub_category_description,
            category_description,
            department_description,
            department_description_h,
            org_id,
            TIENE_PROMO,
            sum(case when sales_weight>0 then CAST(sales_weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity
            when weight>0 then CAST(weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity else quantity end) as Unidades_Medida,
            sum(value) as Venta_Bruta,
            sum(quantity) as Cantidad

    from ( --A
        Select
            SALES_ITEM.customer_key,
            SALES_ITEM.transaction_date,
            PRODUCT_H.product_code as material,
            PRODUCT_H.product_description,
            PRODUCT_H.sub_category_description,
            PRODUCT_H.category_description_h as category_description,
            -- PRODUCT_H.category_description_h
            PRODUCT_H.department_description,
            PRODUCT_H.department_description_h,
            cast(coalesce(st.org_ip_id,'0') as int) as org_id,
            PRODUCT_H.weight,
            SALES_ITEM.weight as sales_weight,
            PRODUCT_H.SALES_UNIT,
            SALES_ITEM.quantity,
            SALES_ITEM.value,
            SALES_ITEM.discount_value,
            CASE WHEN WORKFLOW.descripcion_evento_promocional is null then '0' else '1' end as TIENE_PROMO,
            sum(
            case
                when SALES_ITEM.weight>0 then CAST(SALES_ITEM.weight AS NUMERIC)*CAST(PRODUCT_H.SALES_UNIT AS NUMERIC)*SALES_ITEM.quantity
                when PRODUCT_H.weight>0 then CAST(PRODUCT_H.weight AS NUMERIC)*CAST(PRODUCT_H.SALES_UNIT AS NUMERIC
                    )*SALES_ITEM.quantity
                else SALES_ITEM.quantity end) as Unidades_Medida,
            --sum(SALES_ITEM.value) as Venta_Bruta,
            sum(SALES_ITEM.quantity) as Cantidad,
            count(distinct WORKFLOW.id_workflow ) as cantidad_promos,
            count(distinct SALES_DSCT.geopromotion_id ) as cantidad_descuentos
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
                PRODUCT_H.EAN as upc
                ,LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code
                ,PRODUCT_H.NM as product_description
                ,CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight
                ,CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit
                ,PRODUCT_H.GRUPO_DSC as sub_category_description
                ,PRODUCT_H.CAT_ID as category_code
                ,PRODUCT_H.CAT_DSC as category_description
                ,PRODUCT_H.CAT_H_DSC as category_description_h
                ,PRODUCT_H.LIN_DESC as department_description
                ,PRODUCT_H.LIN_H_DSC as department_description_h
        from `${gcp_project_2}.${schema_2}.VW_DIM_PRODUCT` as PRODUCT_H
        ) AS PRODUCT_H
        ON SALES_ITEM.ean = PRODUCT_H.upc

        LEFT JOIN (
        SELECT
            id_workflow,
            cast(material as STRING) as material,
            nombre_promocion,
            descripcion_evento_promocional,
            descripcion_mecanica
        FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW`
        WHERE registro_valido = 'X'
        GROUP BY 1, 2, 3, 4, 5
        ) AS WORKFLOW
        ON (
            PROMO_LOOKUP.WKF_PROMOTION_ID = WORKFLOW.id_workflow
            AND product_code =WORKFLOW.material
        )

        left join `${gcp_project_2}.${schema_2}.VW_FACT_MARKET_BASKET_E_COMMERCE` e
        on e.market_basket_key=SALES_ITEM.market_basket_key

        left join `${gcp_project_2}.${schema_2}.VW_DIM_STORE_HIERARCHY` st
        on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

        left join (
            select --es necesario sacar outlier?
                Customer_id
                ,organization_id
                ,mes
        from `${gcp_project_2}.${schema_2}.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER` o

        inner join (
            select
                DATE_TRUNC(cast(date_value as date),MONTH) as mes,
                MAX(FORMAT_DATE('%G%V', DATE_VALUE)) AS Semana
            from `${gcp_project_2}.${schema_2}.VW_DIM_DATE`
        group by 1
        ) s
        on cast(o.week_iso_id as STRING)=s.Semana
        where organization_id= ${org_bdf}
        ) o
        on o.customer_id=SALES_ITEM.customer_key
        and DATE_TRUNC(cast(transaction_date as date),MONTH)=o.mes

        where
            --CAMBIAR FECHAS
            cast(SALES_ITEM.transaction_date as DATE) >= cast('${result_date}' as DATE)
            and cast(SALES_ITEM.transaction_date as DATE) < cast('${min_date}' as DATE)
            and e.market_basket_key is null
            and SALES_ITEM.value>0
            and cast(coalesce(st.org_ip_id,'0') as int)= ${banner_nro} --S10
            and st.org_ip_id<>'None'
            and o.Customer_id is null

        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    --limit 20
    ) a
    group by 1,2,3,4,5,6,7,8
    ) a2
    ) b
    ) C
    where fila=1
    """, # noqa: E501

    'step_1':
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
            ) D ON A.PRODUCT_KEY_1 = D.PRODUCT_KEY

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_FACT_MKT_BSKT E
            ON
                A.MARKET_BASKET_KEY = E.MARKET_BASKET_KEY
                AND SUBSTRING(A.TXN_KEY, INSTR(A.TXN_KEY, '-', -1) + 1)  = E.POS_HEX

            JOIN ${gcp_project_1}.${schema_1}.DW_VW_DIM_STORE F
            ON
                A.STORE_KEY = F.STORE_KEY
                AND F.ORG_IP_ID IN ('01', '04', '09', '02', '08', '06')

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
                where cast(SALES_ITEM.transaction_date as date) >= cast('${min_date}' as date)
                and cast(SALES_ITEM.transaction_date as date) <= cast('${max_date}' as date) --Periodo de promociones
                group by 1,2,3
        ) a
        join (
            SELECT distinct
                cast(material as STRING) as material,
                nombre_promocion as nombre_apo,
                cast(fecha_inicio_de_promocion as date) as inicio_promo,
                cast(fecha_fin_de_promocion as date) as fin_promo
            FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW`
            WHERE
                registro_valido = 'X'
                and organizacion_ventas = '3500'
                and cast(fecha_fin_de_promocion as date) >= cast('${month_date}' as date)
        ) apo_table
        on
            apo_table.material = a.material
            and a.transaction_date >= apo_table.inicio_promo
            and a.transaction_date <= apo_table.fin_promo
        group by 1
    )

    Select
        customer_key
        ,transaction_date
        ,material
        ,basket_id
        ,sub_category_description
        ,category_description
        ,department_description
        ,department_description_h
        ,org_id
        ,Mean_U
        ,Stddev_U
        ,Mean_U_sin_Promo
        ,Stddev_U_sin_Promo
        ,Mean_U_con_Promo
        ,Stddev_U_con_Promo
        ,days_difference_subcat
        ,dias_entre_trx_subcat
        ,dias_entre_UMxtrx_subcat
        ,Ult_compra_subcat
        ,Ult_compra_subcat_mes
        ,Ult_compra_cat
        ,Ult_compra_cat_mes
        ,Ult_compra_cliente
        ,Ult_compra_cliente_mes

        ,TIENE_PROMO

        ,APO
        ,S10_CICLO
        ,APOTEOSICO_S10
        ,PERECIBLES_S10
        ,case when material_apo is not null then 1 else 0 end as Material_APO
        ,nombre_promocion_apo
        ,nombre_promocion_ppal_apo
        ,descripcion_mecanica_apo
        ,nombre_promocion_S10_CICLO
        ,nombre_promocion_ppal_S10_CICLO
        ,nombre_promocion_APOTEOSICO_S10
        ,nombre_promocion_ppal_APOTEOSICO_S10
        ,nombre_promocion_PERECIBLES_S10
        ,nombre_promocion_ppal_PERECIBLES_S10
        ,nombre_promocion_ppal_apo_mat
        ,nombre_promocion_ppal_cat_mat
        ,nombre_promocion_ppal_EA_mat
        ,nombre_promocion_ppal_per_mat
        ,Tiene_descuento
        ,producto_pesable
        ,case
            when discount_value=0
                and material_sin_geo is not null
                and producto_pesable='Si' then 1
                else 0
        end as Promo_sin_geo --no tiene descuento en caja pero producto ya viene con descuento
        ,case
            when discount_value=0
                and material_apo is not null
                and producto_pesable='Si'
                and nombre_promocion_ppal_apo_mat is not null then 1
                else 0
        end as APO_sin_geo
        ,case
            when discount_value=0
                and material_apo is not null
                and producto_pesable='Si'
                and nombre_promocion_ppal_cat_mat is not null then 1
                else 0
        end as S10_CICLO_sin_geo
        ,case
            when discount_value=0
                and material_apo is not null
                and producto_pesable='Si'
                and nombre_promocion_ppal_EA_mat is not null then 1
                else 0
        end as APOTEOSICO_S10_sin_geo
        ,case
            when /*discount_value=0 and*/ material_apo is not null /*and producto_pesable='Si'*/
                and nombre_promocion_ppal_per_mat is not null then 1
                else 0
        end as PERECIBLES_S10_sin_geo
        ,ecommerce
        ,outlier

        ,sum(case when sales_weight>0 then CAST(sales_weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity
            when weight>0 then CAST(weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity else quantity end) as Unidades_Medida
        ,sum(value) as Venta_Bruta
        ,sum(discount_value) as Descuento
        ,sum(quantity) as Cantidad

    from (--A1
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
            Mean_U,
            Stddev_U,
            Mean_U_sin_Promo,
            Stddev_U_sin_Promo,
            Mean_U_con_Promo,
            Stddev_U_con_Promo,
            days_difference_subcat,
            dias_entre_trx_subcat,
            dias_entre_UMxtrx_subcat,
            Ult_compra_subcat,
            Ult_compra_subcat_mes,
            Ult_compra_cat,
            Ult_compra_cat_mes,
            Ult_compra_cliente,
            Ult_compra_cliente_mes,
            weight,
            sales_weight,
            SALES_UNIT,
            quantity,
            value,
            discount_value,
            producto_pesable,
            Tiene_descuento,
            ecommerce,
            outlier,
            Descuento_pronto_consumo,
            a.nombre_promocion_apo,
            a.nombre_promocion_ppal_apo,
            descripcion_mecanica_apo,
            nombre_promocion_S10_CICLO,
            nombre_promocion_ppal_S10_CICLO,
            nombre_promocion_APOTEOSICO_S10,
            nombre_promocion_ppal_APOTEOSICO_S10,
            nombre_promocion_PERECIBLES_S10,
            nombre_promocion_ppal_PERECIBLES_S10,
            TIENE_PROMO,
            APO,
            S10_CICLO,
            APOTEOSICO_S10,
            PERECIBLES_S10,
            sin_geo.material as material_sin_geo,
            sin_geo.nombre_promocion as nombre_promocion_singeo,
            apo_table.material as material_apo,
            max(apo_table.nombre_promocion_ppal_apo_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_apo_mat,
            max(apo_table.nombre_promocion_ppal_cat_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_cat_mat,
            max(apo_table.nombre_promocion_ppal_EA_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_EA_mat,
            max(apo_table.nombre_promocion_ppal_per_mat) over (partition by customer_key,basket_id,transaction_date,a.material) as nombre_promocion_ppal_per_mat,

            rank() over (partition by customer_key,	basket_id,transaction_date,a.material order by sin_geo.nombre_promocion,apo_table.nombre_promocion_ppal_apo_mat,
                apo_table.nombre_promocion_ppal_cat_mat) as rnk

    from ( --A
        Select
            SALES_ITEM.customer_key
            ,SALES_ITEM.transaction_date
            ,SALES_ITEM.txn_key as basket_id
            ,product_code as material
            ,PRODUCT_H.sub_category_description
            ,PRODUCT_H.category_description_h as category_description
            ,PRODUCT_H.department_description
            ,PRODUCT_H.department_description_h
            ,cast(coalesce(st.org_ip_id,'0') as int) as org_id
            ,Mean_U
            ,Stddev_U
            ,Mean_U_sin_Promo
            ,Stddev_U_sin_Promo
            ,Mean_U_con_Promo
            ,Stddev_U_con_Promo
            ,days_difference_subcat
            ,dias_entre_trx_subcat
            ,dias_entre_UMxtrx_subcat
            ,Ult_compra_subcat
            ,Ult_compra_subcat_mes
            ,Ult_compra_cat
            ,Ult_compra_cat_mes
            ,Ult_compra_cliente
            ,Ult_compra_cliente_mes
            ,PRODUCT_H.weight
            ,SALES_ITEM.weight as sales_weight
            ,PRODUCT_H.SALES_UNIT
            ,SALES_ITEM.quantity
            ,SALES_ITEM.value
            ,SALES_ITEM.discount_value
            ,case when SALES_ITEM.weight>0 then 'Si' else 'No' end as producto_pesable
            ,case when SALES_ITEM.discount_value>0 then 1 else 0 end as Tiene_descuento
            ,case when e.market_basket_key is null then 0 else 1 end as ecommerce
            ,case when o.Customer_id is null then 0 else 1 end as outlier

            ,sum(case when SUBSTR(geopromotion_id,1,3)='888' then SALES_DSCT.discount_value else 0 end) as Descuento_pronto_consumo

            ,max(CASE WHEN TIPO_PROMOCION='10 DE S10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_apo
            ,max(CASE WHEN TIPO_PROMOCION='10 DE S10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_apo
            ,max(CASE WHEN TIPO_PROMOCION='10 DE S10' then WORKFLOW.descripcion_mecanica else null end) as descripcion_mecanica_apo
            ,max(CASE WHEN TIPO_PROMOCION='S10 CICLO' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_S10_CICLO
            ,max(CASE WHEN TIPO_PROMOCION='S10 CICLO' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_S10_CICLO
            ,max(CASE WHEN TIPO_PROMOCION='APOTEOSICO S10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_APOTEOSICO_S10
            ,max(CASE WHEN TIPO_PROMOCION='APOTEOSICO S10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_APOTEOSICO_S10
            ,max(CASE WHEN TIPO_PROMOCION='PERECIBLES S10' then WORKFLOW.nombre_promocion else null end) as nombre_promocion_PERECIBLES_S10
            ,max(CASE WHEN TIPO_PROMOCION='PERECIBLES S10' then WORKFLOW.nombre_promocion_ppal else null end) as nombre_promocion_ppal_PERECIBLES_S10

            ,max(CASE WHEN WORKFLOW.descripcion_evento_promocional is null then 0 else 1 end) as TIENE_PROMO
            ,max(CASE WHEN TIPO_PROMOCION='10 DE S10' then 1 else 0 end) as APO
            ,max(CASE WHEN TIPO_PROMOCION='S10 CICLO' then 1 else 0 end) as S10_CICLO
            ,max(CASE WHEN TIPO_PROMOCION='APOTEOSICO S10' then 1 else 0 end) as APOTEOSICO_S10
            ,max(CASE WHEN TIPO_PROMOCION='PERECIBLES S10' then 1 else 0 end) as PERECIBLES_S10

            ,max(CASE WHEN nombre_promocion like '%INSERTO%' and Promo_analizar='Si' then 1 else 0 end) as INSERTO
        FROM `${gcp_project_2}.${schema_2}.VW_SALES_ITEM` AS SALES_ITEM

        LEFT JOIN SALES_DISCOUNT AS SALES_DSCT
        ON (
            SALES_ITEM.txn_key = SALES_DSCT.basket_id
            AND SALES_ITEM.ean = SALES_DSCT.upc
        )

        LEFT JOIN PROMO_LOOKUP
        ON SALES_DSCT.geopromotion_id = PROMO_LOOKUP.GEO_PROMOTION_ID

        INNER JOIN (
        select
            -- PRODUCT_H.product_id
            PRODUCT_H.EAN as upc
            ,LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code
            ,PRODUCT_H.NM as product_description
            ,CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight
            ,CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit
            ,PRODUCT_H.GRUPO_DSC as sub_category_description
            ,PRODUCT_H.CAT_DSC as category_description
            ,PRODUCT_H.CAT_H_DSC as category_description_h
            ,PRODUCT_H.LIN_DESC as department_description
            ,PRODUCT_H.LIN_H_DSC as department_description_h
        from `${gcp_project_2}.${schema_2}.VW_DIM_PRODUCT` as PRODUCT_H
        group by 1,2,3,4,5,6,7,8,9,10
        ) AS PRODUCT_H
        ON SALES_ITEM.ean = PRODUCT_H.upc

        LEFT JOIN (
        SELECT
            id_workflow,
            cast(material as STRING) as material,
            w.nombre_promocion,
            coalesce(p.TIPO_PROMOCION,'OTRA') as TIPO_PROMOCION,
            coalesce(nombre_promocion_ppal,w.nombre_promocion) as nombre_promocion_ppal,
            coalesce(p.Mes_promocion,date_trunc(w.fecha_fin_de_promocion,MONTH)) as Mes_promocion,
            w.descripcion_evento_promocional,
            w.descripcion_mecanica,
            case
                when nombre_promocion_ppal is null then 'No' else 'Si'
            end as Promo_analizar
        FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w

        left join `${gcp_project_2}.${schema_3}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}` p
        on p.n_promocion=w.n_promocion
        WHERE
            registro_valido = 'X'
            and organizacion_ventas='3500'
            --CAMBIAR FECHAS MES ANALISIS DE PROMOCIONES
            and (cast(w.fecha_fin_de_promocion as date)>=cast('${month_date}' as date) or p.Mes_promocion=cast('${month_date}' as date))
        group by 1,2,3,4,5,6,7,8,9
        ) AS WORKFLOW
        ON (
            PROMO_LOOKUP.WKF_PROMOTION_ID = WORKFLOW.id_workflow
            AND product_code = WORKFLOW.material
        )

        left join `${gcp_project_2}.${schema_2}.VW_FACT_MARKET_BASKET_E_COMMERCE` e
        on e.market_basket_key=SALES_ITEM.market_basket_key

        left join `${gcp_project_2}.${schema_2}.VW_DIM_STORE_HIERARCHY` st
        on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

        left join (
            select --es necesario sacar outlier?
                Customer_id
                ,organization_id
                ,mes
            from `${gcp_project_2}.${schema_2}.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER` o

            inner join (
            select
                DATE_TRUNC(DATE_VALUE, MONTH) AS MES,
                MAX(FORMAT_DATE('%G%V', DATE_VALUE)) AS SEMANA
            from `${gcp_project_2}.${schema_2}.VW_DIM_DATE`
            group by 1
            ) s
            on cast(o.week_iso_id as STRING)=s.Semana
            where organization_id=${org_bdf}
        ) o
        on
            o.customer_id=SALES_ITEM.customer_key
            and DATE_TRUNC(TRANSACTION_DATE, MONTH)=o.MES

        --CAMBIAR FECHAS
        left join (
            select
                customer_key
                ,sub_category_description
                ,category_description
                ,Mean_U
                ,Stddev_U
                ,Mean_U_sin_Promo
                ,Stddev_U_sin_Promo
                ,Mean_U_con_Promo
                ,Stddev_U_con_Promo
                ,days_difference_subcat
                ,dias_entre_trx_subcat
                ,dias_entre_UMxtrx_subcat
                ,Ult_compra_subcat
                ,Ult_compra_subcat_mes
                --  ,Ult_compra_cliente
                --  ,Ult_compra_cliente_mes
            from `${gcp_project_2}.${schema_3}.TMP_PROMO_EVAL_BEHAVIOR_LAST_12_MONTHS_${upper_store_banner}`
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14--,15,16
        ) sin_promo
        on
            sin_promo.customer_key=SALES_ITEM.customer_key
            and PRODUCT_H.sub_category_description=sin_promo.sub_category_description
            and PRODUCT_H.category_description_h=sin_promo.category_description

        left join (
            select
                customer_key
                ,category_description -- es categoria mundo h
                ,Ult_compra_cat
                ,Ult_compra_cat_mes
        from `${gcp_project_2}.${schema_3}.TMP_PROMO_EVAL_BEHAVIOR_LAST_12_MONTHS_${upper_store_banner}`
        group by 1,2,3,4
        ) sin_promo_cat
        on
            sin_promo_cat.customer_key=SALES_ITEM.customer_key
            and PRODUCT_H.category_description_h=sin_promo_cat.category_description

        left join (
            select
                customer_key
                ,max(Ult_compra_cliente) as Ult_compra_cliente
                ,max(Ult_compra_cliente_mes) as Ult_compra_cliente_mes
            from `${gcp_project_2}.${schema_3}.TMP_PROMO_EVAL_BEHAVIOR_LAST_12_MONTHS_${upper_store_banner}`
            group by 1
        ) sin_promo_formato
        on sin_promo_formato.customer_key=SALES_ITEM.customer_key

        --inner join (
        --  select Customer_key
        --  from clientes_promo
        --) apo
        --on apo.customer_key=SALES_ITEM.customer_key

        where
            cast(SALES_ITEM.transaction_date as DATE) >= cast('${min_date}' as date)
            AND cast(SALES_ITEM.transaction_date as DATE) <= cast('${max_date}' as date)
            and SALES_ITEM.value>0
            and cast(coalesce(st.org_ip_id,'0') as int)= ${banner_nro} --S10
            and st.org_ip_id<>'None'

        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
    ) a --limit 100

    left join (
        SELECT
            cast(w.material as STRING) as material
            ,nombre_promocion
            ,cast(fecha_inicio_de_promocion as date) as inicio_promo ,cast(fecha_fin_de_promocion as date) as fin_promo
        FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w --limit 100

        LEFT JOIN PROMO_LOOKUP
        on PROMO_LOOKUP.WKF_PROMOTION_ID = w.id_workflow

        WHERE
            registro_valido = 'X'
            and organizacion_ventas='3500'
            and w.descripcion_mecanica not in ('APP UNI MASIVAS','APP UNI PERSONALIZADA','APP UNI PROFUNDIZACION')
            and w.descripcion_evento_promocional not in ('SOLPROM/LIQUIDACION','SOLPROM/LIQUIDACION_FUERA_DE_SURTIDO','SOLPROM/SOBRESTOCK')
            and w.desc_promocion='PRECIO FIJO'
            and cast(w.fecha_fin_de_promocion as date)>=cast('${month_date}' as date)
        group by 1,2,3,4
    ) sin_geo
    on
        sin_geo.material=a.material
        and a.transaction_date>=sin_geo.inicio_promo
        and a.transaction_date<=sin_geo.fin_promo

    left join (
        SELECT
            cast(material as STRING) as material
            ,cast(w.fecha_inicio_de_promocion as date) as inicio_promo
            ,cast(w.fecha_fin_de_promocion as date) as fin_promo
            , max(CASE WHEN p.TIPO_PROMOCION='10 DE S10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_apo_mat
            , max(CASE WHEN p.TIPO_PROMOCION='S10 CICLO' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_cat_mat
            , max(CASE WHEN p.TIPO_PROMOCION='APOTEOSICO S10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_EA_mat
            , max(CASE WHEN p.TIPO_PROMOCION='PERECIBLES S10' then p.nombre_promocion_ppal else null end) as nombre_promocion_ppal_per_mat
        FROM `${gcp_project_2}.${schema_2}.VW_FACT_WORKFLOW` w

        join `${gcp_project_2}.${schema_3}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}` p
        on p.n_promocion=w.n_promocion

        WHERE
            registro_valido = 'X'
            and organizacion_ventas='3500'
            --and Mes_promocion=cast('${month_date}' as date)

        group by 1,2,3--,4,5,6
        ) apo_table
        on
            apo_table.material=a.material
            and a.transaction_date>=apo_table.inicio_promo
            and a.transaction_date<=apo_table.fin_promo
    ) a1
    where rnk=1
    -- where sin_geo.material is not null
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52
    """, # noqa: E501

    'step_2':
    """
    select
        customer_key
        ,transaction_date
        ,basket_id
        ,material
        ,sub_category_description
        ,category_description
        ,department_description
        ,department_description_h
        ,org_id
        --,Mean_U_sin_Promo
        --,Stddev_U_sin_Promo
        ,nombre_promocion_ppal
        ,TIENE_PROMO
        ,APO
        ,S10_CICLO
        ,APOTEOSICO_S10
        ,PERECIBLES_S10
        ,Material_APO
        ,Promo_sin_geo
        ,APO_sin_geo
        ,S10_CICLO_sin_geo
        ,APOTEOSICO_S10_sin_geo
        ,PERECIBLES_S10_sin_geo
        ,Cliente_nuevo_formato
        ,Cliente_nuevo_cat
        ,Cliente_nuevo_subcat
        ,Aumenta_UMxdia as Aumenta_UMxdia_subcat
        ,Unidades_Medida
        ,Venta_Bruta
        ,Descuento
        ,Cantidad
        ,Tiene_descuento
        ,ecommerce
        ,outlier
        ,producto_pesable
        ,case
            when APO=1 then Venta_Bruta else 0
        end as Venta_Bruta_APO
        ,case
            when S10_CICLO=1 then Venta_Bruta else 0
        end as Venta_Bruta_S10_CICLO
        ,case
            when APOTEOSICO_S10=1 then Venta_Bruta else 0
        end as Venta_Bruta_APOTEOSICO_S10
        ,case
            when PERECIBLES_S10=1 then Venta_Bruta else 0
        end as Venta_Bruta_PERECIBLES_S10
        ,case
            when APO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and APO_sin_geo=1 then Venta_Bruta
            else 0
        end as Venta_Bruta_APO_sin_geo
        ,case
            when APO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and S10_CICLO_sin_geo=1 then Venta_Bruta
            else 0
        end as Venta_Bruta_S10_CICLO_sin_geo
        ,case
            when APO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and APOTEOSICO_S10_sin_geo=1 then Venta_Bruta
            else 0
        end as Venta_Bruta_APOTEOSICO_S10_sin_geo
        ,case
            when APO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and PERECIBLES_S10_sin_geo=1 then Venta_Bruta
            else 0
        end as Venta_Bruta_PERECIBLES_S10_sin_geo
        ,case
            when APO=0
                and TIENE_PROMO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and Promo_sin_geo=1
                and APO_sin_geo=0
                and S10_CICLO_sin_geo=0
                and APOTEOSICO_S10_sin_geo=0 then Venta_Bruta
            else 0
        end as Venta_Bruta_Promo_SinGeo
        ,case
            when APO=0
                and TIENE_PROMO=0
                and S10_CICLO=0
                and APO_sin_geo=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and S10_CICLO_sin_geo=0
                and Promo_sin_geo=0
                and Descuento=0 then Venta_Bruta
            else 0
        end as Venta_Bruta_SinPromo
        ,case
            when APO=0
                and TIENE_PROMO=1
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and Promo_sin_geo=0 then Venta_Bruta
            else 0
        end as Venta_Bruta_Promo_NoAPO
        ,case
            when APO=0
                and TIENE_PROMO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and Promo_sin_geo=0
                and Descuento>0 then Venta_Bruta
            else 0
        end as Venta_Bruta_OtrosDescuentos
        ,case
            when APO=0
                and TIENE_PROMO=0
                and S10_CICLO=0
                and APOTEOSICO_S10=0
                and PERECIBLES_S10=0
                and Promo_sin_geo=0
                and Descuento=0
                and Material_APO=0 then Venta_Bruta
            else 0
        end as Venta_Bruta_SinPromo_Matapo0

    from (--C
        select
            customer_key
            ,transaction_date
            ,material
            ,basket_id
            ,sub_category_description
            ,category_description
            ,department_description
            ,department_description_h
            ,org_id
            ,Mean_U_sin_Promo
            ,Stddev_U_sin_Promo
            ,TIENE_PROMO

            --,sum(case when APO=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)
            ,case
                when sum(case when PERECIBLES_S10=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_PERECIBLES_S10) over (partition by customer_key,basket_id,transaction_date)
                when sum(case when PERECIBLES_S10_sin_geo=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_per_mat) over (partition by customer_key,basket_id,transaction_date)

                when sum(case when APO=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_apo) over (partition by customer_key,basket_id,transaction_date)
                when sum(case when APO_sin_geo=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_apo_mat) over (partition by customer_key,basket_id,transaction_date)
                when sum(case when APOTEOSICO_S10=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_APOTEOSICO_S10) over (partition by customer_key,basket_id,transaction_date)
                when sum(case when APOTEOSICO_S10_sin_geo=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_EA_mat) over (partition by customer_key,basket_id,transaction_date)

                when sum(case when S10_CICLO=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_S10_CICLO) over (partition by customer_key,basket_id,transaction_date)
                when sum(case when S10_CICLO_sin_geo=1 then Cantidad else 0 end) over (partition by customer_key,basket_id,transaction_date)>0 then max(nombre_promocion_ppal_cat_mat) over (partition by customer_key,basket_id,transaction_date)
            end as nombre_promocion_ppal

            ,APO
            ,S10_CICLO
            ,APOTEOSICO_S10
            ,PERECIBLES_S10
            ,Material_APO
            ,Tiene_descuento
            ,ecommerce, outlier
            ,producto_pesable
            ,Promo_sin_geo
            ,APO_sin_geo
            ,S10_CICLO_sin_geo
            ,APOTEOSICO_S10_sin_geo
            ,PERECIBLES_S10_sin_geo
            ,Unidades_Medida
            ,Venta_Bruta
            ,Descuento
            ,Cantidad

            ,case
                when max(Ult_compra_cliente) over (partition by customer_key) is null then 'SI'
                else 'NO'
            end as Cliente_nuevo_formato

            ,case
                when Ult_compra_cat is null then 'SI'
                else 'NO'
            end as Cliente_nuevo_cat

            ,case
                when Ult_compra_subcat is null then 'SI'
                else 'NO'
            end as Cliente_nuevo_subcat

            ,case
                when Ult_compra_subcat is null then 'NO'
                when date_diff(transaction_date,Ult_compra_subcat,DAY)=0 then 'NO'
                when date_diff(transaction_date,Ult_compra_subcat,DAY)/Unidades_Medida>dias_entre_UMxtrx_subcat then 'NO'
                else 'SI'
            end as Aumenta_UMxdia

    from ( --B
        Select *
        from `${gcp_project}.${schema}.TMP_PROMO_EVAL_HALO_STEP_1_${upper_store_banner}`
        --where basket_id='5201515-11838487-569-6938564'
    ) B
    ) C
    """, # noqa: E501

    'step_3':
    """
    select
        customer_key
        ,cast(transaction_date as date) transaction_date
        ,sub_category_description
        ,category_description
        ,department_description
        ,department_description_h
        ,org_id
        ,ecommerce
        ,outlier
        ,producto_pesable
        ,case
            when nombre_promocion_ppal like '%LAS 10%' then '10 DE S10'
            when nombre_promocion_ppal like '%CICLO%' then 'S10 CICLO'
            when nombre_promocion_ppal like '%ELEGIDOS%' then 'APOTEOSICO S10'
            when nombre_promocion_ppal like '%ELE%' then 'APOTEOSICO S10'
            when nombre_promocion_ppal like '%PERECIBLES%' then 'PERECIBLES S10'
            when nombre_promocion_ppal like '%PUNTA DE PRECIO%' then 'PUNTA DE PRECIO S10'
            when nombre_promocion_ppal like '%APO%' then 'APOTEOSICO S10'
        end as Tipo_Promo
        ,nombre_promocion_ppal
        ,TIENE_PROMO
        ,APO
        ,S10_CICLO
        ,APOTEOSICO_S10
        ,PERECIBLES_S10
        ,Material_APO
        ,Promo_sin_geo
        ,APO_sin_geo
        ,S10_CICLO_sin_geo
        ,APOTEOSICO_S10_sin_geo
        ,PERECIBLES_S10_sin_geo
        ,Cliente_nuevo_formato
        ,Cliente_nuevo_cat
        ,Cliente_nuevo_subcat
        ,Aumenta_UMxdia_subcat
        ,Tiene_descuento

        ,Venta_Ticket_Apo
        ,Venta_Ticket_S10_CICLO
        ,Venta_Ticket_APOTEOSICO_S10
        ,Venta_Ticket_PERECIBLES_S10
        ,Venta_Ticket_APO_SinGeo
        ,Venta_Ticket_S10_CICLO_SinGeo
        ,Venta_Ticket_APOTEOSICO_S10_SinGeo
        ,Venta_Ticket_PERECIBLES_S10_SinGeo
        ,Venta_Ticket_SinPromo
        ,Venta_Ticket_Promo_NoAPO
        ,Venta_Ticket_otrosdescuentos
        ,Venta_Ticket_Promo_SinGeo
        ,Venta_Ticket_SinPromo_Matapo0
        ,Venta_Ticket_Total

        ,sum(Venta_Bruta) as  Venta_Bruta
        ,sum(Venta_Bruta_APO) as  Venta_Bruta_APO
        ,sum(Venta_Bruta_APO_sin_geo) as Venta_Bruta_APO_sin_geo
        ,sum(Venta_Bruta_S10_CICLO) as  Venta_Bruta_S10_CICLO
        ,sum(Venta_Bruta_S10_CICLO_sin_geo) as  Venta_Bruta_S10_CICLO_sin_geo
        ,sum(Venta_Bruta_APOTEOSICO_S10) as  Venta_Bruta_APOTEOSICO_S10
        ,sum(Venta_Bruta_APOTEOSICO_S10_sin_geo) as  Venta_Bruta_APOTEOSICO_S10_sin_geo
        ,sum(Venta_Bruta_PERECIBLES_S10) as  Venta_Bruta_PERECIBLES_S10
        ,sum(Venta_Bruta_PERECIBLES_S10_sin_geo) as  Venta_Bruta_PERECIBLES_S10_sin_geo
        ,sum(Venta_Bruta_SinPromo) as  Venta_Bruta_SinPromo
        ,sum(Venta_Bruta_Promo_NoAPO) as  Venta_Bruta_Promo_NoAPO
        ,sum(Venta_Bruta_otrosdescuentos) as  Venta_Bruta_otrosdescuentos
        ,sum(Venta_Bruta_Promo_SinGeo) as  Venta_Bruta_Promo_SinGeo
        ,sum(case when Tiene_descuento=1 then Venta_Bruta else 0 end) as  Venta_Bruta_con_descuento
        ,sum(case when Tiene_descuento=0 then Venta_Bruta else 0 end) as  Venta_Bruta_sin_descuento
        ,sum(Venta_Bruta_SinPromo_Matapo0) as  Venta_Bruta_SinPromo_Matapo0
        ,count(distinct basket_id) as transacciones
        ,count(distinct customer_key) as Clientes

    from (
        select s.*

        ,sum(Venta_Bruta_APO) over (partition by customer_key, transaction_date) as Venta_Ticket_Apo
        ,sum(Venta_Bruta_S10_CICLO) over (partition by customer_key, transaction_date) as Venta_Ticket_S10_CICLO
        ,sum(Venta_Bruta_APOTEOSICO_S10) over (partition by customer_key, transaction_date) as Venta_Ticket_APOTEOSICO_S10
        ,sum(Venta_Bruta_PERECIBLES_S10) over (partition by customer_key, transaction_date) as Venta_Ticket_PERECIBLES_S10

        ,sum(Venta_Bruta_APO_sin_geo) over (partition by customer_key, transaction_date) as Venta_Ticket_APO_SinGeo
        ,sum(Venta_Bruta_S10_CICLO_sin_geo) over (partition by customer_key, transaction_date) as Venta_Ticket_S10_CICLO_SinGeo
        ,sum(Venta_Bruta_APOTEOSICO_S10_sin_geo) over (partition by customer_key, transaction_date) as Venta_Ticket_APOTEOSICO_S10_SinGeo
        ,sum(Venta_Bruta_PERECIBLES_S10_sin_geo) over (partition by customer_key, transaction_date) as Venta_Ticket_PERECIBLES_S10_SinGeo

        ,sum(Venta_Bruta_SinPromo) over (partition by customer_key, transaction_date) as Venta_Ticket_SinPromo
        ,sum(Venta_Bruta_Promo_NoAPO) over (partition by customer_key, transaction_date) as Venta_Ticket_Promo_NoAPO
        ,sum(Venta_Bruta_otrosdescuentos) over (partition by customer_key, transaction_date) as Venta_Ticket_otrosdescuentos
        ,sum(Venta_Bruta_Promo_SinGeo) over (partition by customer_key, transaction_date) as Venta_Ticket_Promo_SinGeo
        ,sum(Venta_Bruta_SinPromo_Matapo0) over (partition by customer_key, transaction_date) as Venta_Ticket_SinPromo_Matapo0

        ,sum(venta_bruta) over (partition by customer_key, transaction_date) as Venta_Ticket_Total

        from `${gcp_project}.${schema}.TMP_PROMO_EVAL_HALO_STEP_2_${upper_store_banner}` s

    )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
    """, # noqa: E501

    'get_step_3_data':
    """
    SELECT
        customer_key,
        transaction_date,
        sub_category_description,
        category_description,
        department_description,
        department_description_h,
        org_id,
        ecommerce,
        outlier,
        producto_pesable,
        tipo_promo,
        p.nombre_promocion_ppal,
        tiene_promo,
        apo,
        S10_CICLO,
        APOTEOSICO_S10,
        PERECIBLES_S10,
        material_apo,
        promo_sin_geo,
        apo_sin_geo,
        S10_CICLO_sin_geo,
        APOTEOSICO_S10_sin_geo,
        PERECIBLES_S10_sin_geo,
        cliente_nuevo_formato,
        cliente_nuevo_cat,
        cliente_nuevo_subcat,
        aumenta_umxdia_subcat,
        tiene_descuento,
        venta_ticket_apo,
        venta_ticket_S10_CICLO,
        venta_ticket_APOTEOSICO_S10,
        venta_ticket_PERECIBLES_S10,
        venta_ticket_apo_singeo,
        venta_ticket_S10_CICLO_singeo,
        venta_ticket_APOTEOSICO_S10_singeo,
        venta_ticket_PERECIBLES_S10_singeo,
        venta_ticket_sinpromo,
        venta_ticket_promo_noapo,
        venta_ticket_otrosdescuentos,
        venta_ticket_promo_singeo,
        venta_ticket_sinpromo_matapo0,
        venta_ticket_total,
        venta_bruta,
        venta_bruta_apo,
        venta_bruta_S10_CICLO,
        venta_bruta_S10_CICLO_sin_geo,
        venta_bruta_APOTEOSICO_S10,
        venta_bruta_APOTEOSICO_S10_sin_geo,
        venta_bruta_PERECIBLES_S10,
        venta_bruta_PERECIBLES_S10_sin_geo,
        venta_bruta_sinpromo,
        venta_bruta_promo_noapo,
        venta_bruta_otrosdescuentos,
        venta_bruta_promo_singeo,
        venta_bruta_con_descuento,
        venta_bruta_sin_descuento,
        venta_bruta_sinpromo_matapo0,
        transacciones,
        clientes,
        CASE
            WHEN tipo_promo = 'APOTEOSICO S10' THEN
                CASE
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.1 THEN 0
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.2 THEN 10
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.3 THEN 20
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.4 THEN 30
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.5 THEN 40
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.6 THEN 50
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.7 THEN 60
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.8 THEN 70
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 0.9 THEN 80
                    WHEN (venta_ticket_APOTEOSICO_S10 + venta_ticket_APOTEOSICO_S10_singeo) / Venta_Ticket_Total < 1.0 THEN 90
                    ELSE 100 -- Default value for percentages greater than 100%
                END
            WHEN tipo_promo = 'PERECIBLES S10' THEN
                CASE
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.1 THEN 0
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.2 THEN 10
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.3 THEN 20
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.4 THEN 30
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.5 THEN 40
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.6 THEN 50
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.7 THEN 60
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.8 THEN 70
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 0.9 THEN 80
                    WHEN (venta_ticket_PERECIBLES_S10 + venta_ticket_PERECIBLES_S10_singeo) / Venta_Ticket_Total < 1.0 THEN 90
                    ELSE 100 -- Default value for percentages greater than 100%
                END
            WHEN tipo_promo = 'S10 CICLO' THEN
                CASE
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.1 THEN 0
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.2 THEN 10
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.3 THEN 20
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.4 THEN 30
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.5 THEN 40
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.6 THEN 50
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.7 THEN 60
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.8 THEN 70
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.9 THEN 80
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 1.0 THEN 90
                    ELSE 100 -- Default value for percentages greater than 100%
            END
            WHEN tipo_promo = 'PUNTA DE PRECIO S10' THEN
                CASE
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.1 THEN 0
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.2 THEN 10
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.3 THEN 20
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.4 THEN 30
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.5 THEN 40
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.6 THEN 50
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.7 THEN 60
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.8 THEN 70
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 0.9 THEN 80
                    WHEN (venta_ticket_S10_CICLO + venta_ticket_S10_CICLO_singeo) / Venta_Ticket_Total < 1.0 THEN 90
                    ELSE 100 -- Default value for percentages greater than 100%
            END
            WHEN tipo_promo = '10 DE S10' THEN
                CASE
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.1 THEN 0
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.2 THEN 10
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.3 THEN 20
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.4 THEN 30
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.5 THEN 40
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.6 THEN 50
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.7 THEN 60
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.8 THEN 70
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 0.9 THEN 80
                    WHEN (venta_ticket_apo + venta_ticket_apo_singeo) / Venta_Ticket_Total < 1.0 THEN 90
                    ELSE 100 -- Default value for percentages greater than 100%
                END
            ELSE 0
        END AS Banda_Venta
        FROM `${gcp_project}.${schema}.TMP_PROMO_EVAL_HALO_STEP_3_${upper_store_banner}` p

        join (
            SELECT nombre_promocion_ppal
            from `${gcp_project}.${schema}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}`
            --where   Mes_promocion=cast('${month_date}' as date)
            group by 1
        ) promo
        on promo.nombre_promocion_ppal=p.nombre_promocion_ppal

        WHERE
            CUSTOMER_KEY <> MD5('CST^CL^-1') --homologo a customer_key <> 1938419
            AND tipo_promo IS NOT NULL
    """, # noqa: E501

    'promotional_sales':
    """
    Select
        transaction_date
        ,category_description
        ,department_description
        ,department_description_h
        ,nombre_promocion_ppal
        ,sum(case when sales_weight>0 then CAST(sales_weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity
            when weight>0 then CAST(weight AS NUMERIC)*CAST(SALES_UNIT AS NUMERIC)*quantity else quantity end) as Unidades_Medida
        ,sum(value) as Venta_Bruta
        ,sum(discount_value) as Descuento
        ,sum(quantity) as Cantidad

    from ( --A
        Select
            SALES_ITEM.customer_key,
            SALES_ITEM.transaction_date,
            SALES_ITEM.market_basket_key,
            product_code as material,
            PRODUCT_H.sub_category_description,
            PRODUCT_H.category_description_h as category_description,
            PRODUCT_H.department_description,
            PRODUCT_H.department_description_h,
            cast(coalesce(st.org_ip_id,'0') as int) as org_id,
            PRODUCT_H.weight,
            SALES_ITEM.weight as sales_weight,
            PRODUCT_H.SALES_UNIT,
            SALES_ITEM.quantity ,
            SALES_ITEM.value,
            SALES_ITEM.discount_value,
            case
                when SALES_ITEM.weight>0 then 'Si' else 'No'
            end as producto_pesable,
            case
                when SALES_ITEM.discount_value>0 then 1 else 0
            end as Tiene_descuento,
            case
                when e.market_basket_key is null then 0 else 1
            end as ecommerce,
            nombre_promocion,
            nombre_promocion_ppal
        FROM `${gcp_project}.${schema_1}.VW_SALES_ITEM` AS SALES_ITEM

        INNER JOIN (
            select
                -- PRODUCT_H.product_id
                PRODUCT_H.EAN as upc
                ,LTRIM(PRODUCT_H.SKU_PRODUCT, '0') as product_code
                ,PRODUCT_H.NM as product_description
                ,CAST(PRODUCT_H.CONTENIDO_BRUTO AS NUMERIC) as weight
                ,CAST(PRODUCT_H.CONT_CONV_UMB AS NUMERIC) as sales_unit
                ,PRODUCT_H.GRUPO_DSC as sub_category_description
                ,PRODUCT_H.CAT_DSC as category_description
                ,PRODUCT_H.CAT_H_DSC as category_description_h
                ,PRODUCT_H.LIN_DESC as department_description
                ,PRODUCT_H.LIN_H_DSC as department_description_h
            from `${gcp_project}.${schema_1}.VW_DIM_PRODUCT` as PRODUCT_H
            group by 1,2,3,4,5,6,7,8,9,10
        ) AS PRODUCT_H
        ON SALES_ITEM.ean = PRODUCT_H.upc

        join (
            SELECT
                cast(material as STRING) as material,
                w.nombre_promocion,
                cast(w.fecha_inicio_de_promocion as date) as inicio_promo,
                cast(w.fecha_fin_de_promocion as date) as fin_promo,
                p.nombre_promocion_ppal
            FROM `${gcp_project}.${schema_1}.VW_FACT_WORKFLOW` w

            join `${gcp_project}.${schema_2}.TMP_DIM_PROMOTIONS_TO_EVALUATE_MAIN_PROMOTION_${upper_store_banner}` p
            on p.n_promocion=w.n_promocion

            WHERE
                registro_valido = 'X'
                and organizacion_ventas='3500'
                --and canal_distribucion='10'
                --and Mes_promocion=cast('${month_date}' as date)
                --and cast(material as STRING)='663023'
            group by 1,2,3,4,5--,6
        ) apo
        on
            apo.material=product_code
            and cast(SALES_ITEM.transaction_date as DATE) >= apo.inicio_promo
            and cast(SALES_ITEM.transaction_date as DATE) <= apo.fin_promo

        left join `${gcp_project}.${schema_1}.VW_FACT_MARKET_BASKET_E_COMMERCE` e
        on e.market_basket_key=SALES_ITEM.market_basket_key

        left join `${gcp_project}.${schema_1}.VW_DIM_STORE_HIERARCHY` st
        on st.STORE_ID = LPAD(SALES_ITEM.STORE_ID, 4, '0000')

        where
            cast(SALES_ITEM.transaction_date as DATE) >= cast('${min_date}' as date)
            and cast(SALES_ITEM.transaction_date as DATE) <= cast('${max_date}' as date)
            and SALES_ITEM.value>0
            and cast(coalesce(st.org_ip_id,'0') as int)=${banner_nro}
            and st.org_ip_id<>'None'

        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20--,21
    ) a --limit 100
    group by 1,2,3,4,5--,6,7
    """ # noqa: E501
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

    banner_nro = 9
    org_bdf = 15
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

    logging.info('promo_eval_behavior_last_12_months')

    _ = createTableAsSelect(
    query=SQL_QUERIES['promo_eval_behavior_last_12_months'].substitute(
        gcp_project_1 = 'cl-cda-prod',
        schema_1 = 'DS_CDA_VW_SMU',
        gcp_project_2 = 'cl-bigdata-analytics-preprod',
        schema_2 = 'CDA_VISTAS',
        fecha_carga = fecha_carga,
        banner_nro = banner_nro,
        org_bdf = org_bdf,
        min_date = min_date,
        result_date = result_date
    ),
    table_ref=f'{proyecto}.TMP.TMP_PROMO_EVAL_BEHAVIOR_LAST_12_MONTHS_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{proyecto}.TMP.TMP_PROMO_EVAL_BEHAVIOR_LAST_12_MONTHS_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('step_1')

    _ = createTableAsSelect(
    query=SQL_QUERIES['step_1'].substitute(
        gcp_project_1 = 'cl-cda-prod',
        schema_1 = 'DS_CDA_VW_SMU',
        gcp_project_2 = 'cl-bigdata-analytics-preprod',
        schema_2 = 'CDA_VISTAS',
        schema_3 = 'TMP',
        fecha_carga = fecha_carga,
        banner_nro = banner_nro,
        org_bdf = org_bdf,
        min_date = min_date,
        max_date = max_date,
        month_date = month_date,
        upper_store_banner = upper_store_banner
    ),
    table_ref=f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_1_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_1_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('step_2')

    _ = createTableAsSelect(
    query=SQL_QUERIES['step_2'].substitute(
        gcp_project = proyecto,
        schema = 'TMP',
        upper_store_banner = upper_store_banner
    ),
    table_ref=f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_2_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_2_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('step_3')

    _ = createTableAsSelect(
    query=SQL_QUERIES['step_3'].substitute(
        gcp_project = proyecto,
        schema = 'TMP',
        upper_store_banner = upper_store_banner
    ),
    table_ref=f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_3_{upper_store_banner}',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{proyecto}.TMP.TMP_PROMO_EVAL_HALO_STEP_3_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('promotional_sales')

    df_get_step_3_data = readBigQuery(SQL_QUERIES['get_step_3_data'].substitute(
        gcp_project = 'cl-bigdata-analytics-preprod',
        schema = 'TMP',
        upper_store_banner = upper_store_banner,
        month_date = month_date
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    df_get_step_3_data = df_get_step_3_data[
        (df_get_step_3_data['ecommerce'] == 0) &
        (df_get_step_3_data['outlier'] == 0)
    ]

    totales_nombre_promocion_ppal = df_get_step_3_data.groupby(['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique' # Count distinct customer_key values
    }).reset_index()

    totales_nombre_promocion_ppal = totales_nombre_promocion_ppal.rename(
        columns={'customer_key': 'clientes'}
    )

    filtered_df = df_get_step_3_data[(df_get_step_3_data['cliente_nuevo_formato'] == 'SI')]
    totales_nombre_promocion_ppal_nuevos = filtered_df.groupby(['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    totales_nombre_promocion_ppal_nuevos = totales_nombre_promocion_ppal_nuevos.rename(
        columns={'customer_key': 'clientes_nuevo_formato'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    totales_nombre_promocion_ppal_df = totales_nombre_promocion_ppal.merge(
        totales_nombre_promocion_ppal_nuevos,
        on='nombre_promocion_ppal',
        how='outer',
        suffixes=('_total', '_nuevos'))

    # If there are missing values, fill them with 0
    totales_nombre_promocion_ppal_df = totales_nombre_promocion_ppal_df.fillna(0)

    filtered_df = df_get_step_3_data[(df_get_step_3_data['cliente_nuevo_formato'] == 'SI')]
    nuevos_formato_subcategoria = filtered_df.groupby(
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    nuevos_formato_subcategoria = nuevos_formato_subcategoria.rename(
        columns={'customer_key': 'clientes_nuevo_formato',
                 'venta_bruta_sinpromo_matapo0': 'Efecto_halo_nuevo_formato'
                 })


    # If there are missing values, fill them with 0
    nuevos_formato_subcategoria = nuevos_formato_subcategoria.fillna(0)

    total_nuevos_formato_subcat = nuevos_formato_subcategoria.groupby(  # noqa: F841
        ['nombre_promocion_ppal']).agg({
            'clientes_nuevo_formato': 'sum',  # Count distinct customer_key values
            'Efecto_halo_nuevo_formato': 'sum'
    }).reset_index()

    #Cliente recurrente nuevos en la categoria
    filtered_df = df_get_step_3_data[(df_get_step_3_data['cliente_nuevo_formato'] == 'NO')]
    totales_nombre_promocion_ppal = filtered_df.groupby(['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique'  # Count distinct customer_key values
    }).reset_index()

    totales_nombre_promocion_ppal = totales_nombre_promocion_ppal.rename(
        columns={'customer_key': 'clientes'})

    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_cat'] == 'SI')
    ]

    totales_nombre_promocion_ppal_nuevos_cat_2 = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    totales_nombre_promocion_ppal_nuevos_cat_2 = totales_nombre_promocion_ppal_nuevos_cat_2.rename(
        columns={'customer_key': 'clientes_nuevos_categoria'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    totales_nombre_promocion_ppal_nuevos_cat_2_df = totales_nombre_promocion_ppal.merge(
        totales_nombre_promocion_ppal_nuevos_cat_2,
        on=['nombre_promocion_ppal'],
        how='outer',
        suffixes=('_total', '_nuevos')
    )

    # If there are missing values, fill them with 0
    totales_nombre_promocion_ppal_nuevos_cat_2_df = totales_nombre_promocion_ppal_nuevos_cat_2_df.fillna(0)  # noqa: E501

    #Cliente recurrente nuevos en la categoria
    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['Banda_Venta'] >= 20)
    ]

    totales_nombre_promocion_ppal = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique'  # Count distinct customer_key values
    }).reset_index()

    totales_nombre_promocion_ppal = totales_nombre_promocion_ppal.rename(
        columns={'customer_key': 'clientes'}
    )

    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_cat'] == 'SI') &
        (df_get_step_3_data['Banda_Venta'] >= 20)
    ]

    totales_nombre_promocion_ppal_nuevos_cat_2 = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    totales_nombre_promocion_ppal_nuevos_cat_2 = totales_nombre_promocion_ppal_nuevos_cat_2.rename(
        columns={'customer_key': 'clientes_nuevo_subcategoria'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    totales_nombre_promocion_ppal_nuevos_cat_20_df = totales_nombre_promocion_ppal.merge(
        totales_nombre_promocion_ppal_nuevos_cat_2,
        on=['nombre_promocion_ppal'],
        how='outer',
        suffixes=('_total', '_nuevos')
    )

    # If there are missing values, fill them with 0
    totales_nombre_promocion_ppal_nuevos_cat_20_df = totales_nombre_promocion_ppal_nuevos_cat_20_df.fillna(0)  # noqa: E501

    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_cat'] == 'SI') &
        (df_get_step_3_data['Banda_Venta'] >= 20)
    ]

    nuevos_categoria = filtered_df.groupby(
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description']).agg({
            'customer_key': 'nunique',  # Count distinct customer_key values
            'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    nuevos_categoria = nuevos_categoria.rename(columns={
        'customer_key': 'clientes_nuevos_categoria',
        'venta_bruta_sinpromo_matapo0': 'Efecto_halo_nuevos_categoria'})

    # If there are missing values, fill them with 0
    nuevos_categoria = nuevos_categoria.fillna(0)

    total_nuevos_categoria_subcat = nuevos_categoria.groupby(['nombre_promocion_ppal']).agg({  # noqa: F841
    'clientes_nuevos_categoria': 'sum',  # Count distinct customer_key values
    'Efecto_halo_nuevos_categoria': 'sum'
    }).reset_index()

    #Cliente recurrente al fromato y a la  categoria
    filtered_df = df_get_step_3_data[(df_get_step_3_data['cliente_nuevo_formato'] == 'NO')]
    totales_nombre_promocion_ppal = filtered_df.groupby(['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique'  # Count distinct customer_key values
    }).reset_index()

    totales_nombre_promocion_ppal = totales_nombre_promocion_ppal.rename(
        columns={'customer_key': 'clientes'})


    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_subcat'] == 'NO') &
        (df_get_step_3_data['aumenta_umxdia_subcat'] == 'SI')
    ]

    totales_nombre_promocion_ppal_rec2_subcat_2 = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    totales_nombre_promocion_ppal_rec2_subcat_2 = totales_nombre_promocion_ppal_rec2_subcat_2.rename(  # noqa: E501
        columns={'customer_key': 'clientes_recurrentes_subcategoria'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    totales_nombre_promocion_ppal_rec2_subcat_2_df = totales_nombre_promocion_ppal.merge(
        totales_nombre_promocion_ppal_rec2_subcat_2,
        on=['nombre_promocion_ppal'],
        how='outer',
        suffixes=('_total', '_nuevos')
    )

    # If there are missing values, fill them with l0
    totales_nombre_promocion_ppal_rec2_subcat_2_df = totales_nombre_promocion_ppal_rec2_subcat_2_df.fillna(0)  # noqa: E501


    #Cliente recurrente al fromato y a la  categoria
    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['Banda_Venta'] >= 20)
    ]

    totales_nombre_promocion_ppal = filtered_df.groupby(['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique'  # Count distinct customer_key values

    }).reset_index()

    totales_nombre_promocion_ppal = totales_nombre_promocion_ppal.rename(
        columns={'customer_key': 'clientes'}
    )


    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_subcat'] == 'NO') &
        (df_get_step_3_data['Banda_Venta'] >= 20) &
        (df_get_step_3_data['aumenta_umxdia_subcat'] == 'SI')
    ]

    totales_nombre_promocion_ppal_rec2_subcat_2 = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    totales_nombre_promocion_ppal_rec2_subcat_2 = totales_nombre_promocion_ppal_rec2_subcat_2.rename(  # noqa: E501
        columns={'customer_key': 'clientes_recurrentes_subcategoria'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    totales_nombre_promocion_ppal_rec2_subcat_20_df = totales_nombre_promocion_ppal.merge(
        totales_nombre_promocion_ppal_rec2_subcat_2,
        on=['nombre_promocion_ppal'],
        how='outer',
        suffixes=('_total', '_nuevos')
    )

    # If there are missing values, fill them with 0
    totales_nombre_promocion_ppal_rec2_subcat_20_df = totales_nombre_promocion_ppal_rec2_subcat_20_df.fillna(0)  # noqa: E501


    filtered_df = df_get_step_3_data[
        (df_get_step_3_data['cliente_nuevo_formato'] == 'NO') &
        (df_get_step_3_data['cliente_nuevo_subcat'] == 'NO') &
        (df_get_step_3_data['Banda_Venta'] >= 20) &
        (df_get_step_3_data['aumenta_umxdia_subcat'] == 'SI')
    ]

    recurrentes_subcategoria = filtered_df.groupby(
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    recurrentes_subcategoria = recurrentes_subcategoria.rename(
        columns={'customer_key': 'clientes_recurrentes_subcategoria',
                'venta_bruta_sinpromo_matapo0': 'Efecto_halo_recurrentes_subcategoria'}
    )

    # If there are missing values, fill them with 0
    recurrentes_subcategoria = recurrentes_subcategoria.fillna(0)

    total_recurrentes_categoria_subcat = recurrentes_subcategoria.groupby(  # noqa: F841
        ['nombre_promocion_ppal']).agg({
            'clientes_recurrentes_subcategoria': 'sum',  # Count distinct customer_key values
            'Efecto_halo_recurrentes_subcategoria': 'sum'
    }).reset_index()


    #efecto halo por promocion

    # Assuming df1, df2, and df3 are your DataFrames
    # Replace these with your actual DataFrames

    df1 = totales_nombre_promocion_ppal_df[
        ['nombre_promocion_ppal',
        'venta_bruta_sinpromo_matapo0']].copy()

    df1 = df1.rename(
        columns={'venta_bruta_sinpromo_matapo0': 'Efecto_halo_nuevos_formato'}
    )

    df2 = totales_nombre_promocion_ppal_nuevos_cat_2_df[
        ['nombre_promocion_ppal',
        'venta_bruta_sinpromo_matapo0']].copy()

    df2 = df2.rename(
        columns={'venta_bruta_sinpromo_matapo0': 'Efecto_halo_nuevos_categoria'}
    )

    df3 = totales_nombre_promocion_ppal_rec2_subcat_2_df[
        ['nombre_promocion_ppal',
        'venta_bruta_sinpromo_matapo0']].copy()

    df3 = df3.rename(
        columns={'venta_bruta_sinpromo_matapo0': 'Efecto_halo_recurrentes_subcategoria'}
    )

    # Merge the DataFrames on the specified column
    merged_df = pd.merge(df1, df2, on='nombre_promocion_ppal', how='inner')  # noqa: PD015
    merged_df = pd.merge(merged_df, df3, on='nombre_promocion_ppal', how='inner')  # noqa: PD015
    merged_df['Efecto_Halo'] = merged_df[
        ['Efecto_halo_nuevos_formato',
        'Efecto_halo_nuevos_categoria',
        'Efecto_halo_recurrentes_subcategoria']].sum(axis=1)

    # The resulting DataFrame (merged_df) now contains the merged data
    efecto_halo=merged_df

    #efecto halo por categoria

    # Assuming df1, df2, and df3 are your DataFrames
    # Replace these with your actual DataFrames
    df1 = nuevos_formato_subcategoria[
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description',
        'Efecto_halo_nuevo_formato']
    ]

    df2 = nuevos_categoria[
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description',
        'Efecto_halo_nuevos_categoria']
    ]

    df3 = recurrentes_subcategoria[
        ['nombre_promocion_ppal',
        'department_description_h',
        'department_description',
        'category_description',
        'sub_category_description',
        'Efecto_halo_recurrentes_subcategoria']
    ]

    # Merge the DataFrames on the specified column
    merged_df = pd.merge(df1,df2,  # noqa: PD015
                        on=['nombre_promocion_ppal',
                            'department_description_h',
                            'department_description',
                            'category_description',
                            'sub_category_description'],
                            how='outer'
    )

    merged_df = pd.merge(merged_df, df3,  # noqa: PD015
                        on=['nombre_promocion_ppal',
                            'department_description_h',
                            'department_description',
                            'category_description',
                            'sub_category_description'],
                            how='outer'
    )

    merged_df['Efecto_Halo_subcategoria_venta_bruta'] = merged_df[
        ['Efecto_halo_nuevo_formato',
        'Efecto_halo_nuevos_categoria',
        'Efecto_halo_recurrentes_subcategoria']].sum(axis=1)

    # The resulting DataFrame (merged_df) now contains the merged data
    efecto_halo_subcategoria=merged_df

    total_efectos = efecto_halo_subcategoria.groupby(['nombre_promocion_ppal']).agg({  # noqa: F841
        'Efecto_halo_nuevo_formato': 'sum',
        'Efecto_halo_nuevos_categoria': 'sum',
        'Efecto_halo_recurrentes_subcategoria': 'sum'
    }).reset_index()


    df_promotional_sales = readBigQuery(SQL_QUERIES['promotional_sales'].substitute(
    gcp_project = 'cl-bigdata-analytics-preprod',
    schema_1 = 'CDA_VISTAS',
    schema_2 = 'TMP',
    upper_store_banner = upper_store_banner,
    banner_nro = banner_nro,
    min_date = min_date,
    max_date = max_date,
    month_date = month_date
    ),
    user = usuario,
    gbq_client = gbq_client
    ).groupby(['nombre_promocion_ppal']).agg({
        'Venta_Bruta': 'sum'
    }).reset_index()

    clientes_totales = df_get_step_3_data.groupby(
    ['nombre_promocion_ppal']).agg({
    'customer_key': 'nunique' # Count distinct customer_key values
    }).reset_index()

    clientes_totales = clientes_totales.rename(
        columns={'customer_key': 'clientes_totales'}
    )

    filtered_df = df_get_step_3_data[(df_get_step_3_data['Banda_Venta'] >= 70)]

    cherry_pickers_df = filtered_df.groupby(
        ['nombre_promocion_ppal']).agg({
        'customer_key': 'nunique',  # Count distinct customer_key values
        'venta_bruta': 'sum',
        'venta_bruta_sinpromo_matapo0': 'sum'
    }).reset_index()

    cherry_pickers_df = cherry_pickers_df.rename(
        columns={'customer_key': 'clientes_cherry_pickers'}
    )

    # Merge the two DataFrames based on 'nombre_promocion_apo'
    cherry_pickers_df = clientes_totales.merge(
        cherry_pickers_df,
        on='nombre_promocion_ppal',
        how='outer'
    )

    cherry_pickers_df['%_Cherry_Pickers'] = cherry_pickers_df['clientes_cherry_pickers'] / cherry_pickers_df['clientes_totales']  # noqa: E501
    # If there are missing values, fill them with 0
    cherry_pickers_df = cherry_pickers_df.fillna(0)


    excel_content = BytesIO()
    excel_writer = pd.ExcelWriter(
        excel_content,
    )

    efecto_halo.to_excel(
        excel_writer,
        sheet_name='Efecto Halo Promocion',
        index=False,
        header=True
    )

    efecto_halo_subcategoria.to_excel(
        excel_writer,
        sheet_name='Efecto Halo Categoria',
        index=False,
        header=True
    )

    totales_nombre_promocion_ppal_df.to_excel(
        excel_writer,
        sheet_name='Clientes Nuevo Formato',
        index=False,
        header=True
    )

    totales_nombre_promocion_ppal_nuevos_cat_2_df.to_excel(
        excel_writer,
        sheet_name='Clientes Nuevos Categoria',
        index=False,
        header=True
    )

    totales_nombre_promocion_ppal_nuevos_cat_20_df.to_excel(
        excel_writer,
        sheet_name='Clientes Nuevos Categoria 20',
        index=False,
        header=True
    )

    totales_nombre_promocion_ppal_rec2_subcat_2_df.to_excel(
        excel_writer,
        sheet_name='Clientes Recurrentes Categoria',
        index=False,
        header=True
    )

    totales_nombre_promocion_ppal_rec2_subcat_20_df.to_excel(
        excel_writer,
        sheet_name='Clientes Recurrentes Cat 20 ',
        index=False,
        header=True
    )

    df_promotional_sales.to_excel(
        excel_writer,
        sheet_name='Venta Bruta Total',
        index=False,
        header=True
    )

    cherry_pickers_df.to_excel(
        excel_writer,
        sheet_name='Cherry Pickers',
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
            'halo_efect_s10.xlsx'
        )
    ).upload(excel_content)
    logging.info('Tabla subida en Sharepoint')

if __name__ == '__main__':
    main()
