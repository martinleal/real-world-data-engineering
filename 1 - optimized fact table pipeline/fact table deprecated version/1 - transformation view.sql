CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_SCHEMA.PR_CREATE_V_TRANSFORM_FACT_PROCESS_RULE_STATUS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
BEGIN	
    CREATE OR REPLACE VIEW DEMO_DB.DEMO_SCHEMA.V_TRANSFORM_FACT_PROCESS_RULE_STATUS AS
        WITH T_SOURCE AS (
            SELECT
                p.EVENT_PAYLOAD,
                p.EVENT_PAYLOAD:RULE.PROCESS_RULE_id::INTEGER AS ID_PROCESS_RULE,
                f.value:PROCESS_document_id::INTEGER AS ID_DOCUMENT_PROCESS,
                c.value AS COMP,
                p.EVENT_DATE::TIMESTAMP_TZ(3) AS START_DATE,
                (
                    CASE 
                        WHEN p.METADATA_ACTION='RULEDelete' AND p.NEXT_EVENT_DATE IS NULL 
                        THEN START_DATE 
                        ELSE p.NEXT_EVENT_DATE 
                    END
                )::TIMESTAMP_TZ(3) AS END_DATE,
                p.EVENT_METADATA,
                NVL(c.value:sustainable_MATERIAL_code,-1)::INTEGER AS ID_SUSTAINABLE_MATERIAL_FROM_MATERIALS,
                NVL(p.EVENT_PAYLOAD:RULE.sustainable_MATERIAL_code::INTEGER, -1) AS ID_SUSTAINABLE_MATERIAL,
                CASE WHEN ID_SUSTAINABLE_MATERIAL_FROM_MATERIALS = ID_SUSTAINABLE_MATERIAL THEN 1 ELSE 0 END AS IS_MATERIAL_ANALYTICS_CANDIDATE,
                p.EVENT_PAYLOAD:RULE.self_declaration AS SELF_DECLARATION
                FROM DEMO_DB.DEMO_SCHEMA.TMP_EXTRACT_FACT_PROCESS_RULE_STATUS p,
                lateral flatten(input => p.EVENT_PAYLOAD:RULE:documentList, OUTER => TRUE) f,
                lateral flatten(input => NVL(CASE WHEN p.EVENT_PAYLOAD:RULE.MATERIALs = '[]' THEN NULL ELSE p.EVENT_PAYLOAD:RULE.MATERIALs END, ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('composition_code',p.EVENT_PAYLOAD:RULE.composition_code,'composition_type_code',p.EVENT_PAYLOAD:RULE.composition_type_code, 'garment_zone_code',p.EVENT_PAYLOAD:RULE.garment_zone_code,'sustainable_MATERIAL_code',p.EVENT_PAYLOAD:RULE.sustainable_MATERIAL_code))), OUTER => TRUE) c
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_PROCESS_RULE, ID_DOCUMENT_PROCESS, COMP, START_DATE ORDER BY END_DATE ASC) = 1
            ), RAW AS(
                SELECT 
                    ID_ORDER,
                    ID_RAW_MATERIAL_ORDER
                    FROM CENTRAL_SHARING_DB.DEMO_SCHEMA.V_FACT_RAW_ORDER
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_ORDER ORDER BY MODIFICATION_DATE DESC) = 1
            )
            SELECT DISTINCT
                NVL(m.COD_ARTICLE, -1) AS COD_ARTICLE, 
                NVL(CAST(p.EVENT_PAYLOAD:RULE.article_id AS int),-1) AS ID_ARTICLE,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.raw_material_code AS int),-1) AS ID_RAW,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.color_id AS int),-1) AS ID_COLOR,
                NVL(p.ID_DOCUMENT_PROCESS, -1) AS ID_DOCUMENT_PROCESS,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.document_type_code AS int),-1) AS ID_DOCUMENT_TYPE,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.supplier_code AS int),-1) AS ID_SUPPLIER,
                CAST(p.EVENT_PAYLOAD:RULE.PROCESS_RULE_id AS int) AS ID_PROCESS_RULE,
                CAST(p.EVENT_PAYLOAD:RULE.PROCESS_status_id AS int) AS ID_PROCESS_STATUS,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.PROCESS_status_reason_id AS int),-1) AS ID_PROCESS_STATUS_REASON,
                NVL(p.comp:composition_code,-1)::INTEGER AS ID_COMPOSITION,
                NVL(p.comp:composition_type_code,-1)::INTEGER AS ID_COMPOSITION_TYPE,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.draft_order_garment_code AS int),-1) AS ID_NEGOTIATION_ORDER,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.order_raw_material_code AS int),-1) AS ID_ORDER,
                NVL(p.comp:garment_zone_code,-1)::INTEGER AS ID_PRODUCT_ZONE,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.BUYING_CENTER_code AS int),-1) AS ID_BUYING_CENTER,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.RULE_level AS int),-1) AS ID_RULE_LEVEL,
                p.ID_SUSTAINABLE_MATERIAL_FROM_MATERIALS AS ID_SUSTAINABLE_MATERIAL,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.sustainable_MATERIAL_group_id AS int),-1) AS ID_SUSTAINABLE_MATERIAL_GROUP,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.delivery_number AS int),-1) AS DELIVERY_NUMBER,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.purchase_order_number AS int),-1) AS ORDER_NUMBER,
                CASE WHEN CAST(AR.ID_BRAND AS int) = -1 OR CAST(AR.ID_BRAND AS int) IS NULL THEN NVL(r.ID_BRAND,-1) ELSE NVL(CAST(AR.ID_BRAND AS int),-1) END AS ID_BRAND,
                CASE WHEN CAST(AR.ID_SECTION AS int) = -1 OR CAST(AR.ID_SECTION AS int) IS NULL THEN NVL(r.ID_SECTION,-1) ELSE NVL(CAST(AR.ID_SECTION AS int),-1) END AS ID_SECTION,
                CASE WHEN CAST(AR.ID_SEASON AS int) = -1111 OR CAST(AR.ID_SEASON AS int) IS NULL THEN NVL(r.ID_SEASON,-1111) ELSE NVL(CAST(AR.ID_SEASON AS int),-1111) END AS ID_SEASON,
                NVL(CAST(RAW.ID_RAW_MATERIAL_ORDER AS INT),-1) AS ID_RAW_MATERIAL_ORDER,
                REPLACE(REPLACE(NVL(p.EVENT_PAYLOAD:RULE.deadline_date,'9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS DEADLINE_DATE,
                REPLACE(REPLACE(NVL(p.EVENT_PAYLOAD:RULE.date_created,'9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS DATE_CREATED,
                REPLACE(REPLACE(NVL(p.EVENT_PAYLOAD:RULE.urgent_date,'9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS URGENT_DATE,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.is_urgent AS int),-1) AS IS_URGENT,
                NVL(CAST(p.EVENT_PAYLOAD:RULE.is_first_purchase_order_color AS int),-1) AS IS_FIRST_ORDER,
                CASE WHEN ID_ORDER IS NULL OR ID_ORDER = -1 THEN 0 ELSE 1 END AS IS_MMPP,
                NVL(p.START_DATE, '0000-01-01') AS START_DATE,
                NVL(p.END_DATE,'9999-12-31')::TIMESTAMP_TZ(3) END_DATE,
                CASE WHEN p.END_DATE IS NULL THEN 1 ELSE 0 END IS_ACTUAL,
                p.EVENT_METADATA,
                CASE WHEN NVL(p.comp:element_code,-1) = NVL(CAST(p.EVENT_PAYLOAD:RULE.article_id AS int),-1) THEN -1 ELSE NVL(p.comp:element_code,-1) END AS ID_SUBARTICLE,
                CASE WHEN ID_SUBARTICLE = -1 THEN 0 ELSE 1 END IS_SUBARTICLE,
                p.IS_MATERIAL_ANALYTICS_CANDIDATE,
                CASE WHEN lower(p.SELF_DECLARATION) = 'true' THEN 1 ELSE 0 END AS IS_SELF_DECLARATION
            FROM T_SOURCE AS p
            LEFT JOIN RAW ON RAW.ID_ORDER = NVL(CAST(p.EVENT_PAYLOAD:RULE.order_raw_material_code AS int),0)
            LEFT JOIN CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_ARTICLE_COLOR m
                ON m.ID_ARTICLE = NVL(p.EVENT_PAYLOAD:RULE.article_id,-1)::INTEGER 
                AND m.ID_COLOR = NVL(p.EVENT_PAYLOAD:RULE.color_id,-1)::INTEGER 
                AND IS_LAST_VERSION = 1
            LEFT JOIN CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_ARTICLE AR
                ON AR.ID_ARTICLE = NVL(p.EVENT_PAYLOAD:RULE.article_id,-1)::INTEGER
                AND AR.IS_LAST_VERSION = 1
            LEFT JOIN  DEMO_DB.DEMO_SCHEMA.TMP_DIM_RAW_LAST_VERSION r ON r.ID_RAW = NVL(p.EVENT_PAYLOAD:RULE.raw_material_code,-1)
            GROUP BY ALL
            ;
END;
$$;
