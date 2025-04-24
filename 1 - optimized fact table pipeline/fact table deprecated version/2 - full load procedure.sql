CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_SCHEMA.PR_FULL_LOAD_FACT_PROCESS_RULE_STATUS()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    STATE VARCHAR DEFAULT 'SUCCESS';
    EVENT_ID VARCHAR DEFAULT '0';
    LAST_QUERY_ID VARCHAR DEFAULT '0';
    LOG_RESULT VARCHAR DEFAULT '[]';
    EXCEPTION_MSG VARCHAR DEFAULT '-';
BEGIN
    CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_START(
        'DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS',
        'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.LOGISTICA_GLOBAL_PRO_CERTIFICATEMANAGE_RULE_PUBLIC'
    );
    
    EVENT_ID := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    let tableName := (
        SELECT CASE (SELECT current_account()) WHEN 'ORGANIZATION_PRE' 
            THEN 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.SOURCE_DEMO_TABLE_PRE_ENV'
            ELSE 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.SOURCE_DEMO_TABLE'
        END
    );

    ALTER TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TSK_LOAD_FACT_PROCESS_RULE_STATUS SUSPEND;
    SELECT SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS('DEMO_DB.DEMO_SCHEMA.TSK_LOAD_FACT_PROCESS_RULE_STATUS');

    CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.DEMO_SCHEMA.TMP_FACT_PROCESS_RULE_STATUS
    LIKE DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS;

    BEGIN TRANSACTION;
    
        TRUNCATE TABLE DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS;

        CALL CENTRAL_DATA.MANAGEMENT.P_GET_ETL_JSON(LAST_QUERY_ID(), 'TRUNCATE', 'DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS', :LOG_RESULT) INTO LOG_RESULT;

        INSERT INTO DEMO_DB.DEMO_SCHEMA.TMP_FACT_PROCESS_RULE_STATUS (
            ID_ARTICLE,
            ID_RAW,
            ID_COLOR,
            ID_DOCUMENT_TYPE,
            ID_SUPPLIER,
            ID_PROCESS_RULE,
            ID_PROCESS_STATUS,
            ID_PROCESS_STATUS_REASON,
            ID_NEGOTIATION_ORDER,
            ID_ORDER,
            ID_BUYING_CENTER,
            ID_RULE_LEVEL,
            ID_SUSTAINABLE_MATERIAL_GROUP,
            DELIVERY_NUMBER,
            ORDER_NUMBER,
            DEADLINE_DATE,
            DATE_CREATED,
            URGENT_DATE,
            IS_URGENT,
            IS_FIRST_ORDER,
            START_DATE,
            END_DATE,
            IS_ACTUAL,
            EVENT_METADATA,
            ID_DOCUMENT_PROCESS,
            ID_COMPOSITION,
            ID_COMPOSITION_TYPE,
            ID_PRODUCT_ZONE,
            ID_SUSTAINABLE_MATERIAL,
            ID_SUBARTICLE,
            IS_SUBARTICLE,
            IS_MATERIAL_ANALYTICS_CANDIDATE,
            COD_ARTICLE,
            ID_BRAND,
            ID_SECTION,
            ID_SEASON,
            ID_RAW_MATERIAL_ORDER,
            IS_MMPP,
            IS_SELF_DECLARATION,
            LOAD_DATE,
            LOAD_USER,
            MODIFICATION_DATE,
            MODIFICATION_USER
        )
        WITH DIM_RAW_LAST_VERSION AS (
            SELECT 
                ID_RAW,
                ID_BRAND,
                ID_SEASON,
                ID_SECTION,
                MODIFICATION_DATE  
            FROM CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_RAW
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_RAW ORDER BY FROM_TIMESTAMP DESC) = 1
        ), FACT_RAW_ORDER_LAST_VERSION AS (
            SELECT 
                ID_ORDER,
                ID_RAW_MATERIAL_ORDER
            FROM CENTRAL_SHARING_DB.DEMO_SCHEMA.V_FACT_RAW_ORDER
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_ORDER ORDER BY MODIFICATION_DATE DESC) = 1
        ), EXTRACT AS (
            SELECT
                e.PAYLOAD,
                dl.value AS DOCUMENT_LIST,
                fl.value AS MATERIAL_LIST,
                e.EVENT_DATE,
                e.NEXT_EVENT_DATE,
                e.METADATA:action AS METADATA_ACTION,
                OBJECT_CONSTRUCT(
                    'EVENT_METADATA',
                    ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
                        'EVENT_NAME', e.EVENT_NAME,
                        'EVENT_PK', e.EVENT_PK ,
                        'EVENT_DATE', e.EVENT_DATE,
                        'EVENT_ID', e.EVENT_ID,
                        'EVENT_ENTITY', :tableName
                    ))
                ) AS EVENT_METADATA
            FROM TABLE(:tableName) e,
            LATERAL FLATTEN(input => e.PAYLOAD:RULE:documentList, OUTER => TRUE) dl,
            LATERAL FLATTEN(input => NVL(
                CASE WHEN e.PAYLOAD:RULE.MATERIALs = '[]' THEN NULL ELSE e.PAYLOAD:RULE.MATERIALs END,
                ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
                    'composition_code', e.PAYLOAD:RULE.composition_code,
                    'composition_type_code', e.PAYLOAD:RULE.composition_type_code,
                    'garment_zone_code', e.PAYLOAD:RULE.garment_zone_code,
                    'sustainable_MATERIAL_code', e.PAYLOAD:RULE.sustainable_MATERIAL_code
                ))
            ), OUTER => TRUE) fl
            WHERE NOT(EQUAL_NULL(e.EVENT_DATE, e.NEXT_EVENT_DATE))
        )
        SELECT DISTINCT
            NVL(e.PAYLOAD:RULE.article_id::INT, -1) AS ID_ARTICLE,
            NVL(e.PAYLOAD:RULE.raw_material_code::INT, -1) AS ID_RAW,
            NVL(e.PAYLOAD:RULE.color_id::INT, -1) AS ID_COLOR,
            NVL(e.PAYLOAD:RULE.document_type_code::INT, -1) AS ID_DOCUMENT_TYPE,
            NVL(e.PAYLOAD:RULE.supplier_code::INT, -1) AS ID_SUPPLIER,
            NVL(e.PAYLOAD:RULE.PROCESS_RULE_id::INT, -1) AS ID_PROCESS_RULE,
            NVL(e.PAYLOAD:RULE.PROCESS_status_id::INT, -1) AS ID_PROCESS_STATUS,
            NVL(e.PAYLOAD:RULE.PROCESS_status_reason_id::INT, -1) AS ID_PROCESS_STATUS_REASON,
            NVL(e.PAYLOAD:RULE.draft_order_garment_code::INT, -1) AS ID_NEGOTIATION_ORDER,
            NVL(e.PAYLOAD:RULE.order_raw_material_code::INT, -1) AS ID_ORDER,
            NVL(e.PAYLOAD:RULE.BUYING_CENTER_code::INT, -1) AS ID_BUYING_CENTER,
            NVL(e.PAYLOAD:RULE.RULE_level::INT, -1) AS ID_RULE_LEVEL,
            NVL(e.PAYLOAD:RULE.sustainable_MATERIAL_group_id::INT, -1) AS ID_SUSTAINABLE_MATERIAL_GROUP,
            NVL(e.PAYLOAD:RULE.delivery_number::INT, -1) AS DELIVERY_NUMBER,
            NVL(e.PAYLOAD:RULE.purchase_order_number::INT, -1) AS ORDER_NUMBER,
            REPLACE(REPLACE(NVL(e.PAYLOAD:RULE.deadline_date, '9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS DEADLINE_DATE,
            REPLACE(REPLACE(NVL(e.PAYLOAD:RULE.date_created, '9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS DATE_CREATED,
            REPLACE(REPLACE(NVL(e.PAYLOAD:RULE.urgent_date, '9999-12-31'), 'T', ' '), 'Z', '')::TIMESTAMP_TZ(3) AS URGENT_DATE,
            NVL(CAST(e.PAYLOAD:RULE.is_urgent AS int),-1) AS IS_URGENT,
            NVL(CAST(e.PAYLOAD:RULE.is_first_purchase_order_color AS int),-1) AS IS_FIRST_ORDER,
            NVL(e.EVENT_DATE::TIMESTAMP_TZ(3), '0000-01-01') AS START_DATE,
            NVL(CASE WHEN e.METADATA_ACTION = 'RULEDelete' AND e.NEXT_EVENT_DATE IS NULL THEN START_DATE ELSE e.NEXT_EVENT_DATE END, '9999-12-31')::TIMESTAMP_TZ(3) END_DATE,
            CASE WHEN END_DATE = '9999-12-31'::TIMESTAMP_TZ(3) THEN 1 ELSE 0 END IS_ACTUAL,
            e.EVENT_METADATA,
            NVL(e.DOCUMENT_LIST:PROCESS_document_id::INT, -1) AS ID_DOCUMENT_PROCESS,
            NVL(e.MATERIAL_LIST:composition_code::INT, -1) AS ID_COMPOSITION,
            NVL(e.MATERIAL_LIST:composition_type_code::INT, -1) AS ID_COMPOSITION_TYPE,
            NVL(e.MATERIAL_LIST:garment_zone_code::INT, -1) AS ID_PRODUCT_ZONE,
            NVL(e.MATERIAL_LIST:sustainable_MATERIAL_code::INT, -1) AS ID_SUSTAINABLE_MATERIAL,
            CASE WHEN NVL(e.MATERIAL_LIST:element_code,-1) = NVL(e.PAYLOAD:RULE.article_id::INT, -1) THEN -1 ELSE NVL(e.MATERIAL_LIST:element_code,-1) END AS ID_SUBARTICLE,
            CASE WHEN ID_SUBARTICLE = -1 THEN 0 ELSE 1 END IS_SUBARTICLE,
            CASE WHEN ID_SUSTAINABLE_MATERIAL = NVL(e.PAYLOAD:RULE.sustainable_MATERIAL_code::INT, -1) THEN 1 ELSE 0 END AS IS_MATERIAL_ANALYTICS_CANDIDATE,
            NVL(m.COD_ARTICLE, -1) AS COD_ARTICLE,
            CASE WHEN a.ID_BRAND::INT = -1 OR a.ID_BRAND IS NULL THEN NVL(r.ID_BRAND, -1) ELSE NVL(a.ID_BRAND::INT, -1) END AS ID_BRAND,
            CASE WHEN a.ID_SECTION::INT = -1 OR a.ID_SECTION IS NULL THEN NVL(r.ID_SECTION, -1) ELSE NVL(a.ID_SECTION::INT, -1) END AS ID_SECTION,
            CASE WHEN a.ID_SEASON::INT = -1111 OR a.ID_SEASON IS NULL THEN NVL(r.ID_SEASON, -1111) ELSE NVL(a.ID_SEASON::INT, -1111) END AS ID_SEASON,
            NVL(ro.ID_RAW_MATERIAL_ORDER::INT, -1) AS ID_RAW_MATERIAL_ORDER,
            CASE WHEN ID_ORDER IS NULL OR ID_ORDER = -1 THEN 0 ELSE 1 END AS IS_MMPP,
            CASE WHEN lower(e.PAYLOAD:RULE.self_declaration) = 'true' THEN 1 ELSE 0 END AS IS_SELF_DECLARATION,
            CURRENT_TIMESTAMP(),
            CURRENT_ROLE(),
            NULL,
            NULL
        FROM EXTRACT e
        LEFT JOIN CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_ARTICLE_COLOR m ON m.ID_ARTICLE = NVL(e.PAYLOAD:RULE.article_id::INT, -1) AND m.ID_COLOR = NVL(e.PAYLOAD:RULE.color_id::INT, -1) AND m.IS_LAST_VERSION = 1
        LEFT JOIN CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_ARTICLE a ON a.ID_ARTICLE = NVL(e.PAYLOAD:RULE.article_id::INT, -1) AND a.IS_LAST_VERSION = 1
        LEFT JOIN DIM_RAW_LAST_VERSION r ON r.ID_RAW = NVL(e.PAYLOAD:RULE.raw_material_code::INT, -1)
        LEFT JOIN FACT_RAW_ORDER_LAST_VERSION ro ON ro.ID_ORDER = NVL(e.PAYLOAD:RULE.order_raw_material_code::INT, 0);

        INSERT INTO DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS (
            ID_ARTICLE,
            ID_RAW,
            ID_COLOR,
            ID_DOCUMENT_TYPE,
            ID_SUPPLIER,
            ID_PROCESS_RULE,
            ID_PROCESS_STATUS,
            ID_PROCESS_STATUS_REASON,
            ID_NEGOTIATION_ORDER,
            ID_ORDER,
            ID_BUYING_CENTER,
            ID_RULE_LEVEL,
            ID_SUSTAINABLE_MATERIAL_GROUP,
            DELIVERY_NUMBER,
            ORDER_NUMBER,
            DEADLINE_DATE,
            DATE_CREATED,
            URGENT_DATE,
            IS_URGENT,
            IS_FIRST_ORDER,
            START_DATE,
            END_DATE,
            IS_ACTUAL,
            EVENT_METADATA,
            ID_DOCUMENT_PROCESS,
            ID_COMPOSITION,
            ID_COMPOSITION_TYPE,
            ID_PRODUCT_ZONE,
            ID_SUSTAINABLE_MATERIAL,
            ID_SUBARTICLE,
            IS_SUBARTICLE,
            IS_MATERIAL_ANALYTICS_CANDIDATE,
            COD_ARTICLE,
            ID_BRAND,
            ID_SECTION,
            ID_SEASON,
            ID_RAW_MATERIAL_ORDER,
            IS_MMPP,
            IS_SELF_DECLARATION,
            LOAD_DATE,
            LOAD_USER,
            MODIFICATION_DATE,
            MODIFICATION_USER
        )
        SELECT
            ID_ARTICLE,
            ID_RAW,
            ID_COLOR,
            ID_DOCUMENT_TYPE,
            ID_SUPPLIER,
            ID_PROCESS_RULE,
            ID_PROCESS_STATUS,
            ID_PROCESS_STATUS_REASON,
            ID_NEGOTIATION_ORDER,
            ID_ORDER,
            ID_BUYING_CENTER,
            ID_RULE_LEVEL,
            ID_SUSTAINABLE_MATERIAL_GROUP,
            DELIVERY_NUMBER,
            ORDER_NUMBER,
            DEADLINE_DATE,
            DATE_CREATED,
            URGENT_DATE,
            IS_URGENT,
            IS_FIRST_ORDER,
            START_DATE,
            END_DATE,
            IS_ACTUAL,
            EVENT_METADATA,
            ID_DOCUMENT_PROCESS,
            ID_COMPOSITION,
            ID_COMPOSITION_TYPE,
            ID_PRODUCT_ZONE,
            ID_SUSTAINABLE_MATERIAL,
            ID_SUBARTICLE,
            IS_SUBARTICLE,
            IS_MATERIAL_ANALYTICS_CANDIDATE,
            COD_ARTICLE,
            ID_BRAND,
            ID_SECTION,
            ID_SEASON,
            ID_RAW_MATERIAL_ORDER,
            IS_MMPP,
            IS_SELF_DECLARATION,
            LOAD_DATE,
            LOAD_USER,
            MODIFICATION_DATE,
            MODIFICATION_USER
        FROM DEMO_DB.DEMO_SCHEMA.TMP_FACT_PROCESS_RULE_STATUS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY
            ID_PROCESS_RULE,
            ID_DOCUMENT_PROCESS,
            ID_COMPOSITION,
            ID_COMPOSITION_TYPE,
            ID_PRODUCT_ZONE,
            ID_SUSTAINABLE_MATERIAL,
            ID_SUBARTICLE,
            START_DATE
        ORDER BY END_DATE ASC) = 1;
        
        CALL CENTRAL_DATA.MANAGEMENT.P_GET_ETL_JSON(LAST_QUERY_ID(), 'INSERT', 'DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS', :LOG_RESULT) INTO LOG_RESULT;

    COMMIT;

    ALTER TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TSK_LOAD_FACT_PROCESS_RULE_STATUS RESUME;

    CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_END(:EVENT_ID, :STATE, :LOG_RESULT);

    RETURN LOG_RESULT;

    EXCEPTION
        WHEN STATEMENT_ERROR THEN BEGIN
            ROLLBACK;
            STATE := 'ERROR';
            LOG_RESULT := (SELECT REPLACE(:sqlerrm, '\''));
            CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_END(:EVENT_ID, :STATE, :LOG_RESULT);
            ALTER TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TSK_LOAD_FACT_PROCESS_RULE_STATUS RESUME;
            RAISE;
    END;
END;
$$;