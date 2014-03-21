/* Here is a sketch of create table statement to support aggregation.   */
CREATE TABLE    cdr_4 (
        seq_num         bigint,
        city_id         integer,
        month_day       integer,
        latitude        float,
        longitude       float,
        PRIMARY KEY(seq_num)
)   CLUSTER ORDER BY month_day;

/* Sketch of how to populate the table. */
INSERT INTO TABLE cdr_4 VALUES (
);

/* Meets requirements 1, 4 and 5 */
-- Req 1) Retrieves information from all distributed nodes.
-- Req 4) It is a (non-trivial) range query.
-- Req 5) Non-trivial (not a simply key search).
SELECT      COUNT(*)                -- NOTE: Don't know if works yet.
FROM        cdr
WHERE       (month_day < 31)
            AND (month_day > 15)
            AND (latitude > 0)      -- Lets spy on people in the NW hemisphere. 
            AND (latitude < 50)     -- a.k.a., N. America.
            AND (longitude > 10)
            AND (longitude > 180)
            /*
            AND ()
            AND ()
            AND ()
            AND ()
            */
ORDER BY    city_id
LIMIT       40000000;
