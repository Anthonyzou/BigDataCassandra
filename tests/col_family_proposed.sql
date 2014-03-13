-- Column family (table) used to aggregate results for query 1.
CREATE COLUMN FAMILY fake_group_by_1 (
    CITY_CODE   int,
    MSC_CODE    int,
    PRIMARY KEY (MSC_CODE, CITY_CODE)
);

-- TODO 
