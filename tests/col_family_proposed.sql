-- Column family (table) used to aggregate results for query 1.
CREATE TABLE fake_group_by_1 (
    CITY_CODE           int,
    MSC_CODE            int,
    -- Here are some things which don't (read: shouldn't) exist in the DB.
    -- They are just for fun/proof of concept. (Whatever that concept is...)
    FAKE_THINGY01       text,
    FAKE_THINGY02       text,
    FAKE_THINGY03       text,
    FAKE_THINGY04       text,
    FAKE_THINGY05       text,
    FAKE_THINGY06       text,
    FAKE_THINGY07       text,
    PRIMARY KEY (CITY_CODE, CITY_MSC)   -- TODO: Is this the right idea?
    TABLE_UUID          uuid,       -- TODO: Is this a good attr name?
);

-- Here's a test table.
CREATE TABLE main_thingy (
    CITY_CODE           int,
    MSC_CODE            int,
    STARTTIME           timestamp,
    CONNEC_REQUEST_TIME timestamp,
    REPORT_TIME         timestamp,
    SEIZ_CELL_NUM_L     text,
    SEIZ_SEC_NUM_L      text,
    LAST_DRC_CELL_L     text,
    LAST_DRC_SEC_L      text,
    ASS_CELL_ID_FOR_CONN_L text,
    -- Here are some things which don't (read: shouldn't) exist in the DB.
    -- They are just for fun/proof of concept. (Whatever that concept is...)
    RANDOM_THINGY01     text,
    RANDOM_THINGY02     text,
    RANDOM_THINGY03     text,
    RANDOM_THINGY04     text,
    RANDOM_THINGY05     text,
    RANDOM_THINGY06     text,
    RANDOM_THINGY07     text,
    RANDOM_THINGY08     text,
    RANDOM_THINGY09     text,
    RANDOM_THINGY10     text,
    RANDOM_THINGY11     text,
    RANDOM_THINGY12     text,
    RANDOM_THINGY13     text,
    RANDOM_THINGY14     text,
    RANDOM_THINGY15     text,
    PRIMARY KEY (CITY_CODE, MSC_CODE),  -- TODO: Can we have PK and UUID...
    TABLE_UUID          uuid    -- TODO: Is this a good attr name?
);

/*
   And here's a query which "aggregates" results based on whether the
   This is (sort of) akin to using a foreign key.
*/
SELECT  m.CITY_CODE, m.MSC_CODE
FROM    main_thingy m, fake_group_by_1 f
WHERE   (m.CITY_CODE = f.CITY_CODE)
        AND (
