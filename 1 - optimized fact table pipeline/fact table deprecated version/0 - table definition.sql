CREATE TRANSIENT TABLE IF NOT EXISTS DEMO_DB.DEMO_SCHEMA.FACT_PROCESS_RULE_STATUS(
    COD_ARTICLE INTEGER,
    ID_ARTICLE INTEGER,
    ID_RAW INTEGER,
    ID_COLOR INTEGER,
    ID_DOCUMENT_PROCESS INTEGER NOT NULL,
    ID_DOCUMENT_TYPE INTEGER,
    ID_SUPPLIER INTEGER,
    ID_PROCESS_RULE INTEGER NOT NULL,
    ID_PROCESS_STATUS INTEGER NOT NULL,
    ID_PROCESS_STATUS_REASON INTEGER,
    ID_COMPOSITION INTEGER,
    ID_COMPOSITION_TYPE INTEGER,
    ID_NEGOTIATION_ORDER INTEGER,
    ID_ORDER INTEGER,
    ID_PRODUCT_ZONE INTEGER,
    ID_BUYING_CENTER INTEGER,
    ID_RULE_LEVEL INTEGER,
    ID_SUSTAINABLE_MATERIAL INTEGER,
    ID_SUSTAINABLE_MATERIAL_GROUP INTEGER,
    DELIVERY_NUMBER INTEGER,
    ORDER_NUMBER INTEGER,
    ID_BRAND INTEGER,
    ID_SECTION INTEGER,
    ID_SEASON INTEGER,
    ID_RAW_MATERIAL_ORDER INTEGER,
    DEADLINE_DATE TIMESTAMP_TZ(3),
    DATE_CREATED TIMESTAMP_TZ(3),
    URGENT_DATE TIMESTAMP_TZ(3),
    IS_URGENT BOOLEAN,
    IS_FIRST_ORDER BOOLEAN,
    IS_MMPP BOOLEAN,
    START_DATE TIMESTAMP_TZ(3) NOT NULL,
    END_DATE TIMESTAMP_TZ(3) NOT NULL,
    IS_ACTUAL INTEGER NOT NULL,
    ID_SUBARTICLE INTEGER NOT NULL,
    IS_SUBARTICLE INTEGER NOT NULL,
    LOAD_DATE TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LOAD_USER STRING NOT NULL DEFAULT CURRENT_ROLE(),
    MODIFICATION_DATE TIMESTAMP_TZ NULL,
    MODIFICATION_USER STRING NULL,
    EVENT_METADATA VARIANT,
    IS_MATERIAL_ANALYTICS_CANDIDATE INTEGER,
    IS_SELF_DECLARATION BOOLEAN
);