CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_SCHEMA.PR_LOAD_FACT_PROCESS_RULE_STATUS()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    STATE VARCHAR DEFAULT 'SUCCESS';
    EVENT_ID VARCHAR DEFAULT '0';
    LOG_RESULT VARCHAR DEFAULT '-';
BEGIN
    CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_START('DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS', 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.LOGISTICA_GLOBAL_PRO_CERTIFICATEMANAGE_RULE_PUBLIC');
    EVENT_ID := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    let tableName := (
        SELECT CASE (SELECT current_account()) WHEN 'ORGANIZATION_PRE' 
            THEN 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.SOURCE_DEMO_TABLE_PRE_ENV'
            ELSE 'SOURCE_DEMO_DB.SOURCE_DEMO_SCHEMA.SOURCE_DEMO_TABLE'
        END
    );

    CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.DEMO_SCHEMA.TMP_CHANGES_FACT_PROCESS_RULE_STATUS (EVENT_PK STRING);
	CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.DEMO_SCHEMA.TMP_EXTRACT_FACT_PROCESS_RULE_STATUS (EVENT_PAYLOAD VARIANT, EVENT_DATE TIMESTAMP_LTZ(3), NEXT_EVENT_DATE TIMESTAMP_LTZ(3), METADATA_ACTION VARIANT, EVENT_METADATA VARIANT);
   	
    /*Tabla temporal con la ultima versión de cada ID_RAW de DIM_RAW*/
    CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.DEMO_SCHEMA.TMP_DIM_RAW_LAST_VERSION as
    SELECT ID_RAW, ID_BRAND, ID_SEASON, ID_SECTION, MODIFICATION_DATE  
    FROM CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_RAW
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_RAW ORDER BY FROM_TIMESTAMP DESC) = 1
    ;

    CALL DEMO_DB.DEMO_SCHEMA.PR_CREATE_V_TRANSFORM_FACT_PROCESS_RULE_STATUS();

    BEGIN TRANSACTION;
        INSERT INTO DEMO_DB.DEMO_SCHEMA.TMP_CHANGES_FACT_PROCESS_RULE_STATUS(EVENT_PK)
        (
            SELECT DISTINCT EVENT_PK
            FROM TABLE(:tableName) ev
            WHERE TIMESTAMPDIFF(HOUR, INGEST_DATE, CURRENT_TIMESTAMP())<= 2
        );

        INSERT INTO DEMO_DB.DEMO_SCHEMA.TMP_EXTRACT_FACT_PROCESS_RULE_STATUS (EVENT_PAYLOAD, EVENT_DATE, NEXT_EVENT_DATE, METADATA_ACTION, EVENT_METADATA) 
        (
            SELECT         
                ev.PAYLOAD AS EVENT_PAYLOAD
                ,ev.EVENT_DATE
                ,ev.NEXT_EVENT_DATE
                ,ev.METADATA:action AS METADATA_ACTION
                ,OBJECT_CONSTRUCT('EVENT_METADATA', ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('EVENT_NAME', ev.EVENT_NAME,'EVENT_PK', ev.EVENT_PK ,'EVENT_DATE', ev.EVENT_DATE,'EVENT_ID', ev.EVENT_ID,'EVENT_ENTITY', :tableName))) AS EVENT_METADATA
            FROM TABLE(:tableName) ev
            INNER JOIN CENTRAL_SHARING_DB.DEMO_SCHEMA.V_DIM_ARTICLE dim ON dim.ID_ARTICLE = PAYLOAD:RULE.article_id AND IS_LAST_VERSION = 1 
            WHERE TIMESTAMPDIFF(HOUR, MODIFICATION_DATE, CURRENT_TIMESTAMP())<= 2 AND NOT(EQUAL_NULL(EVENT_DATE,NEXT_EVENT_DATE))		
            UNION
            SELECT         
                ev.PAYLOAD AS EVENT_PAYLOAD
                ,ev.EVENT_DATE
                ,ev.NEXT_EVENT_DATE
                ,ev.METADATA:action AS METADATA_ACTION
                ,OBJECT_CONSTRUCT('EVENT_METADATA', ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('EVENT_NAME', ev.EVENT_NAME,'EVENT_PK', ev.EVENT_PK ,'EVENT_DATE', ev.EVENT_DATE,'EVENT_ID', ev.EVENT_ID,'EVENT_ENTITY', :tableName))) AS EVENT_METADATA
            FROM TABLE(:tableName) ev
            INNER JOIN DEMO_DB.DEMO_SCHEMA.TMP_DIM_RAW_LAST_VERSION dim ON dim.ID_RAW = PAYLOAD:RULE.raw_material_code
            WHERE TIMESTAMPDIFF(HOUR, MODIFICATION_DATE, CURRENT_TIMESTAMP())<= 2 AND NOT(EQUAL_NULL(EVENT_DATE,NEXT_EVENT_DATE))
            UNION
            SELECT         
                ev.PAYLOAD AS EVENT_PAYLOAD
                ,ev.EVENT_DATE
                ,ev.NEXT_EVENT_DATE
                ,ev.METADATA:action AS METADATA_ACTION
                ,OBJECT_CONSTRUCT('EVENT_METADATA', ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('EVENT_NAME', ev.EVENT_NAME,'EVENT_PK', ev.EVENT_PK ,'EVENT_DATE', ev.EVENT_DATE,'EVENT_ID', ev.EVENT_ID,'EVENT_ENTITY', :tableName))) AS EVENT_METADATA
            FROM TABLE(:tableName) ev
            INNER JOIN DEMO_DB.DEMO_SCHEMA.TMP_CHANGES_FACT_PROCESS_RULE_STATUS ch ON ev.EVENT_PK = ch.EVENT_PK
            WHERE NOT(EQUAL_NULL(EVENT_DATE,NEXT_EVENT_DATE))
        );

        MERGE INTO DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS AS sink
        USING DEMO_DB.DEMO_SCHEMA.V_TRANSFORM_FACT_PROCESS_RULE_STATUS AS src 
        ON
            src.ID_PROCESS_RULE  = sink.ID_PROCESS_RULE AND
            src.ID_DOCUMENT_PROCESS = sink.ID_DOCUMENT_PROCESS AND
            src.ID_COMPOSITION = sink.ID_COMPOSITION AND
            src.ID_COMPOSITION_TYPE = sink.ID_COMPOSITION_TYPE AND
            src.ID_PRODUCT_ZONE = sink.ID_PRODUCT_ZONE AND
            src.ID_SUSTAINABLE_MATERIAL = sink.ID_SUSTAINABLE_MATERIAL AND
            src.START_DATE = sink.START_DATE AND
            src.ID_SUBARTICLE = sink.ID_SUBARTICLE
        WHEN MATCHED AND 
        (
            src.COD_ARTICLE <> sink.COD_ARTICLE OR
            src.ID_ARTICLE <> sink.ID_ARTICLE OR 
            src.ID_RAW <> sink.ID_RAW OR 
            src.ID_COLOR <> sink.ID_COLOR OR 
            src.ID_DOCUMENT_PROCESS <> sink.ID_DOCUMENT_PROCESS OR 
            src.ID_DOCUMENT_TYPE <> sink.ID_DOCUMENT_TYPE OR 
            src.ID_SUPPLIER <> sink.ID_SUPPLIER OR 
            src.ID_PROCESS_RULE <> sink.ID_PROCESS_RULE OR 
            src.ID_PROCESS_STATUS <> sink.ID_PROCESS_STATUS OR 
            src.ID_PROCESS_STATUS_REASON <> sink.ID_PROCESS_STATUS_REASON OR
            src.ID_NEGOTIATION_ORDER <> sink.ID_NEGOTIATION_ORDER OR 
            src.ID_ORDER <> sink.ID_ORDER OR 
            src.ID_BUYING_CENTER <> sink.ID_BUYING_CENTER OR 
            src.ID_RULE_LEVEL <> sink.ID_RULE_LEVEL OR 
            src.ID_SUSTAINABLE_MATERIAL_GROUP <> sink.ID_SUSTAINABLE_MATERIAL_GROUP OR 
            src.DELIVERY_NUMBER <> sink.DELIVERY_NUMBER OR 
            src.ORDER_NUMBER <> sink.ORDER_NUMBER OR 
            src.ID_BRAND <> sink.ID_BRAND OR
            src.ID_SECTION <> sink.ID_SECTION OR
            src.ID_SEASON <> sink.ID_SEASON OR
            src.ID_RAW_MATERIAL_ORDER <> sink.ID_RAW_MATERIAL_ORDER OR
            src.DEADLINE_DATE <> sink.DEADLINE_DATE OR 
            src.DATE_CREATED <> sink.DATE_CREATED OR 
            src.URGENT_DATE <> sink.URGENT_DATE OR 
            src.IS_URGENT <> sink.IS_URGENT OR 
            src.IS_FIRST_ORDER <> sink.IS_FIRST_ORDER OR 
            src.IS_MMPP <> sink.IS_MMPP OR
            src.END_DATE <> sink.END_DATE  OR 
            src.IS_ACTUAL <> sink.IS_ACTUAL OR
            src.IS_SUBARTICLE <> sink.IS_SUBARTICLE OR
            src.IS_MATERIAL_ANALYTICS_CANDIDATE <> sink.IS_MATERIAL_ANALYTICS_CANDIDATE OR
            src.IS_SELF_DECLARATION <> sink.IS_SELF_DECLARATION
        ) THEN UPDATE SET
            sink.COD_ARTICLE = src.COD_ARTICLE,
            sink.ID_ARTICLE = src.ID_ARTICLE, 
            sink.ID_RAW = src.ID_RAW, 
            sink.ID_COLOR = src.ID_COLOR, 
            sink.ID_DOCUMENT_PROCESS = src.ID_DOCUMENT_PROCESS, 
            sink.ID_DOCUMENT_TYPE = src.ID_DOCUMENT_TYPE, 
            sink.ID_SUPPLIER = src.ID_SUPPLIER, 
            sink.ID_PROCESS_RULE = src.ID_PROCESS_RULE, 
            sink.ID_PROCESS_STATUS = src.ID_PROCESS_STATUS, 
            sink.ID_PROCESS_STATUS_REASON = src.ID_PROCESS_STATUS_REASON,
            sink.ID_NEGOTIATION_ORDER = src.ID_NEGOTIATION_ORDER, 
            sink.ID_ORDER = src.ID_ORDER, 
            sink.ID_BUYING_CENTER = src.ID_BUYING_CENTER, 
            sink.ID_RULE_LEVEL = src.ID_RULE_LEVEL, 
            sink.ID_SUSTAINABLE_MATERIAL_GROUP = src.ID_SUSTAINABLE_MATERIAL_GROUP, 
            sink.DELIVERY_NUMBER = src.DELIVERY_NUMBER, 
            sink.ORDER_NUMBER = src.ORDER_NUMBER, 
            sink.ID_BRAND = src.ID_BRAND,
            sink.ID_SECTION = src.ID_SECTION,
            sink.ID_SEASON = src.ID_SEASON,
            sink.ID_RAW_MATERIAL_ORDER = src.ID_RAW_MATERIAL_ORDER,
            sink.DEADLINE_DATE = src.DEADLINE_DATE, 
            sink.DATE_CREATED = src.DATE_CREATED, 
            sink.URGENT_DATE = src.URGENT_DATE, 
            sink.IS_URGENT = src.IS_URGENT, 
            sink.IS_FIRST_ORDER = src.IS_FIRST_ORDER, 
            sink.IS_MMPP = src.IS_MMPP,
            sink.END_DATE = src.END_DATE , 
            sink.IS_ACTUAL = src.IS_ACTUAL,
            sink.IS_SUBARTICLE = src.IS_SUBARTICLE,
            sink.MODIFICATION_DATE = CURRENT_TIMESTAMP(),
            sink.MODIFICATION_USER = CURRENT_ROLE(),
            sink.EVENT_METADATA =  src.EVENT_METADATA,
            sink.IS_MATERIAL_ANALYTICS_CANDIDATE =  src.IS_MATERIAL_ANALYTICS_CANDIDATE,
            sink.IS_SELF_DECLARATION =  src.IS_SELF_DECLARATION
        WHEN NOT MATCHED THEN INSERT 
        (
            COD_ARTICLE,
            ID_ARTICLE,
            ID_RAW,
            ID_COLOR,
            ID_DOCUMENT_PROCESS,
            ID_DOCUMENT_TYPE,
            ID_SUPPLIER,
            ID_PROCESS_RULE,
            ID_PROCESS_STATUS,
            ID_PROCESS_STATUS_REASON,
            ID_COMPOSITION,
            ID_COMPOSITION_TYPE,
            ID_NEGOTIATION_ORDER,
            ID_ORDER,
            ID_PRODUCT_ZONE,
            ID_BUYING_CENTER,
            ID_RULE_LEVEL,
            ID_SUSTAINABLE_MATERIAL,
            ID_SUSTAINABLE_MATERIAL_GROUP,
            DELIVERY_NUMBER,
            ORDER_NUMBER,
            ID_BRAND,
            ID_SECTION,
            ID_SEASON,
            ID_RAW_MATERIAL_ORDER,
            DEADLINE_DATE,
            DATE_CREATED,
            URGENT_DATE,
            IS_URGENT,
            IS_FIRST_ORDER,
            IS_MMPP,
            START_DATE,
            END_DATE, 
            IS_ACTUAL,
            ID_SUBARTICLE,
            IS_SUBARTICLE,
            LOAD_DATE,
            LOAD_USER,
            MODIFICATION_DATE,
            MODIFICATION_USER,
            EVENT_METADATA,
            IS_MATERIAL_ANALYTICS_CANDIDATE,
            IS_SELF_DECLARATION
        ) VALUES (
            src.COD_ARTICLE,
            src.ID_ARTICLE,
            src.ID_RAW,
            src.ID_COLOR,
            src.ID_DOCUMENT_PROCESS,
            src.ID_DOCUMENT_TYPE,
            src.ID_SUPPLIER,
            src.ID_PROCESS_RULE,
            src.ID_PROCESS_STATUS,
            src.ID_PROCESS_STATUS_REASON,
            src.ID_COMPOSITION,
            src.ID_COMPOSITION_TYPE,
            src.ID_NEGOTIATION_ORDER,
            src.ID_ORDER,
            src.ID_PRODUCT_ZONE,
            src.ID_BUYING_CENTER,
            src.ID_RULE_LEVEL,
            src.ID_SUSTAINABLE_MATERIAL,
            src.ID_SUSTAINABLE_MATERIAL_GROUP,
            src.DELIVERY_NUMBER,
            src.ORDER_NUMBER,
            src.ID_BRAND,
            src.ID_SECTION,
            src.ID_SEASON,
            src.ID_RAW_MATERIAL_ORDER,
            src.DEADLINE_DATE,
            src.DATE_CREATED,
            src.URGENT_DATE,
            src.IS_URGENT,
            src.IS_FIRST_ORDER,
            src.IS_MMPP,
            src.START_DATE,
            src.END_DATE ,
            src.IS_ACTUAL,
            src.ID_SUBARTICLE,
            src.IS_SUBARTICLE,
            CURRENT_TIMESTAMP(),
            CURRENT_ROLE(),
            NULL,
            NULL,
            src.EVENT_METADATA ,
            src.IS_MATERIAL_ANALYTICS_CANDIDATE,
            src.IS_SELF_DECLARATION
        );

        LOG_RESULT := (SELECT $1||' rows inserted - '||$2||' rows update' FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    COMMIT;
    
    CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_END(:EVENT_ID, :STATE, :LOG_RESULT);
    RETURN LOG_RESULT;

    EXCEPTION
        WHEN STATEMENT_ERROR THEN BEGIN
            ROLLBACK;
            STATE := 'ERROR';
            LOG_RESULT := (SELECT REPLACE(:sqlerrm, '\''));
            CALL CENTRAL_DATA.MANAGEMENT.P_ETL_EVENT_END(:EVENT_ID, :STATE, :LOG_RESULT);
            RAISE;
    END;
END
$$;