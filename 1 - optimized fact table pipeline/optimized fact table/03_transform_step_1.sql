CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_SCHEMA.PR_CREATE_V_TRANSFORM_1_FACT_PROCESS_RULE_STATUS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
BEGIN
CREATE OR REPLACE VIEW DEMO_DB.DEMO_SCHEMA.V_TRANSFORM_1_FACT_PROCESS_RULE_STATUS AS
SELECT
    EVENT_EXTRACT.ID_PROCESS_RULE,
    EVENT_EXTRACT.ID_PROCESS_STATUS,
    EVENT_EXTRACT.ID_ARTICLE,
    EVENT_EXTRACT.ID_RAW,
    EVENT_EXTRACT.ID_COLOR,
    EVENT_EXTRACT.ID_DOCUMENT_TYPE,
    EVENT_EXTRACT.ID_SUPPLIER,
    EVENT_EXTRACT.ID_PROCESS_STATUS_REASON,
    EVENT_EXTRACT.ID_NEGOTIATION_ORDER,
    EVENT_EXTRACT.ID_ORDER,
    EVENT_EXTRACT.ID_BUYING_CENTER,
    EVENT_EXTRACT.ID_RULE_LEVEL,
    EVENT_EXTRACT.ID_SUSTAINABLE_MATERIAL_GROUP,
    EVENT_EXTRACT.DELIVERY_NUMBER,
    EVENT_EXTRACT.ORDER_NUMBER,
    EVENT_EXTRACT.DEADLINE_DATE,
    EVENT_EXTRACT.DATE_CREATED,
    EVENT_EXTRACT.URGENT_DATE,
    EVENT_EXTRACT.IS_URGENT,
    EVENT_EXTRACT.IS_FIRST_ORDER,
    EVENT_EXTRACT.IS_SELF_DECLARATION,
    NVL(documentList.value:PROCESS_document_id::INTEGER, -1) AS ID_DOCUMENT_PROCESS,
    NVL(MaterialsList.value:composition_code::INTEGER, -1) AS ID_COMPOSITION,
    NVL(MaterialsList.value:composition_type_code::INTEGER, -1) AS ID_COMPOSITION_TYPE,
    NVL(MaterialsList.value:garment_zone_code::INTEGER, -1) AS ID_PRODUCT_ZONE,
    NVL(MaterialsList.value:sustainable_MATERIAL_code::INTEGER, -1) AS ID_SUSTAINABLE_MATERIAL,
    CASE 
        WHEN MaterialsList.value:element_code::INTEGER = ID_ARTICLE
        THEN -1
        ELSE NVL(MaterialsList.value:element_code::INTEGER, -1)
    END AS ID_SUBARTICLE,
    CASE 
        WHEN ID_SUBARTICLE = -1 THEN 0
        ELSE 1
    END AS IS_SUBARTICLE,
    CASE
        WHEN EVENT_EXTRACT.ID_ORDER = -1 THEN 0
        ELSE 1
    END AS IS_MMPP,
    CASE 
        WHEN ID_SUSTAINABLE_MATERIAL = SUSTAINABLE_MATERIAL_CODE THEN 1 
        ELSE 0 
    END AS IS_MATERIAL_ANALYTICS_CANDIDATE,
    NVL(EVENT_EXTRACT.EVENT_DATE::TIMESTAMP_TZ, '0000-01-01') AS START_DATE,
    NVL(EVENT_EXTRACT.NEXT_EVENT_DATE::TIMESTAMP_TZ, '9999-12-31') AS END_DATE,
    CASE 
        WHEN EVENT_EXTRACT.NEXT_EVENT_DATE IS NULL THEN 1 
        ELSE 0 
    END AS IS_ACTUAL,
    EVENT_EXTRACT.INGEST_DATE,
    OBJECT_CONSTRUCT(
        'EVENT_NAME', EVENT_EXTRACT.EVENT_NAME,
        'EVENT_ID', EVENT_EXTRACT.EVENT_ID,
        'EVENT_PK', EVENT_EXTRACT.EVENT_PK ,
        'INGEST_DATE', EVENT_EXTRACT.INGEST_DATE,
        'EVENT_DATE', EVENT_EXTRACT.EVENT_DATE,
        'NEXT_EVENT_DATE', EVENT_EXTRACT.NEXT_EVENT_DATE,
        'EVENT_ENTITY', 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.SOURCE_DEMO_TABLE'
    ) AS EVENT_METADATA,
    CASE
        WHEN EVENT_EXTRACT.EVENT_NAME = 'RULEDelete' THEN 'DELETE'
        ELSE 'INSERT/UPDATE'
    END AS ACTION,
    EVENT_EXTRACT.RANK_ORDER
FROM DEMO_DB.DEMO_SCHEMA.TMP_EXTRACT_FACT_PROCESS_RULE_STATUS EVENT_EXTRACT,
LATERAL FLATTEN(INPUT => EVENT_EXTRACT.documentList, OUTER => TRUE) documentList,
LATERAL FLATTEN(
    INPUT => COALESCE(
        NULLIF(EVENT_EXTRACT.MATERIALs, '[]'),
        ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
            'composition_code', EVENT_EXTRACT.composition_code,
            'composition_type_code', EVENT_EXTRACT.composition_type_code,
            'garment_zone_code', EVENT_EXTRACT.garment_zone_code,
            'sustainable_MATERIAL_code', EVENT_EXTRACT.sustainable_MATERIAL_code
        ))
    ),
    OUTER => TRUE
) MaterialsList
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
        ID_PROCESS_RULE, 
        ID_DOCUMENT_PROCESS, 
        ID_COMPOSITION,
        ID_COMPOSITION_TYPE,
        ID_PRODUCT_ZONE,
        ID_SUSTAINABLE_MATERIAL,
        ID_SUBARTICLE,
        START_DATE 
    ORDER BY END_DATE ASC
) = 1
;
END;
$$;
