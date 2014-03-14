/* Here is a list of proposed queries to meet the specifications for
   the BigData project.

Project:    "BigData"
Group No:   Group 3
Class:      CMPUT 391
Term:       Winter 2014
Authors:    Shenwei Liao, Anthony Ou, Jacqueline Terlaan

*/

/* Specification of query requirements. 
   (See also: Section 2.3 of project specification document.)
   
    1)  4/5 queries will retrieve information from ALL distributed nodes.
    2)  1 query must contain >= 10 atomic conditions in the WHERE clause.
    3)  2 queries must contain both of: GROUP BY and ORDER BY clauses.
    4)  >= 3 queries must be "range" queries: i.e., retrieving all rows
        where a column value is between an upper and lower boundary.
    5)  0 queries are trivial in nature. (No simple key searches.)
*/

/* Proposed strategies/options:

    A)  Attack requirement no. 1 first. Then it is possible to reserve
        one of the five queries to meet some other requirement without
        necessarily incurring extra costs. (i.e., it might not be 
        optimal if it were to have to be executed over all nodes in
        the cluser, but it can meet its requirement relatively easily.)

        Potential difficulties:

            Prob 1) We may not yet know the nature of how the data will
                    be distributed.
            Soln 1) Use this query to plan that out. When writing queries
                    to satisfy requirement no. 1, decide upon whether to
                    partition vertically or horizontally, and make all of
                    the queries which will meet this requirement "optimal"
                    for that type of partition.


    B)  Attack requirements no. 2, 3 and 4 first.

        Potential difficulties:

            Prob 1) We may not be able to find a way to make the queries
                    meet requirement no. 1 easily and/or efficiently.
            Soln 1) None so far.

    C)  Save requirement 2 for the query which will not meet requirement 1.
        This could save a lot of time. (Does not necessarily contradict
        strategy A).

        Potential difficulties:

            Prob 1) Queries which meet requirement 3 could take a long
                    time, because they will be distributed over all of
                    the nodes in the cluser.
            Soln 1) Make queries which are to meet requirement 3 very,
                    very simple. (Probably not possible, though.).

    D)  Save requirement 2 for one of the two queries which are to meet
        requirement 3 (i.e., they will have both GROUP BY and ORDER BY
        clauses.)

        Potential difficulties:
            Prob 1) Bloody expensive.
            Soln 1) I dunno m8. 'Ave a giggle.

    E)  Make queries which are to meet requirement 2 relatively simple,
        removing any unnecesary nesting.

    F)  Make one query which meets requirement 2 also meet requirement 4.
        i.e., kill two birds with one stone. Making a range query with
        TONS of other atomic conditions in the WHERE clause should be
        easy, right?

        Potential difficulties:
        
            Prob 1) That query could become very expensive?
            Soln 1) That query will only have to be executed once, right?
                    We could reserve that query as the one which will not
                    be distributed over all nodes in the cluster if it is
                    absolutely necessary.

*/

/*              Queries which satisfy strategies A and F.                   */
/* New proposed query no. 1. */
/* The first 10 cols in the table are: 
CITY_ID int,
MSC_CODE int,
STARTTIME timestamp,
CONNEC_REQUEST_TIME timestamp,
REPORT_TIME timestamp,
SEIZ_CELL_NUM_L text,
SEIZ_SEC_NUM_L  text,
LAST_DRC_CELL_L text,
LAST_DRC_SEC_L  text,
ASS_CELL_ID_FOR_CONN_L text
*/

/* Why not find the city_id and msc_mode of all calls placed in the past year? 
 Say that we will start counting from one year ago to this exact timestamp. 
TODO: In the future, maybe make a slight margin of error where we can instead 
 measure from one year ago to 00:00 today so that it is less "arbitrary"? 
 Then again, this whole idea is pretty arbitrary, so... */
SELECT  CITY_ID,
        MSC_CODE,
        STARTTIME,
        CONNEC_REQUEST_TIME,
        REPORT_TIME,
        SEIZ_CELL_NUM_L,
        SEIZ_SEC_NUM_L,
        LAST_DRC_CELL_L,
        LAST_DRC_SEC_L,
        ASS_CELL_ID_FOR_CONN_L
FROM    call_details_record
WHERE   (
            (((STARTTIME >=  to_date('2010-01-01'))
                AND (STARTTIME <= SYSTIMESTAMP))
            OR ((CONNEC_REQUEST_TIME >= to_date('2010-01-01'))
                AND (CONNEC_REQUEST_TIME < SYSTIMESTAMP))
            OR ((REPORT_TIME >= to_date('2010-01-01'))
                AND (REPORT_TIME < SYSTIMESTAMP)))
        AND 
            (OR (SEIZ_CELL_NUM_L <> '')
            OR (SEIZ_SEC_NUM_L <> '')
            OR (LAST_DRC_CELL_L <> '')
            OR (LAST_DRC_SEC_L <> '')
            OR (ASS_CELL_ID_FOR_CONN_L <> ''))
        )
-- NOTE: GROUP BY aggregration is NOT _directly_ supported in CQL 3.1.5!
-- GROUP BY 
ORDER BY CITY_ID;

/* NOTE: We can simplify the above query in the following way? */


/*
-- Inner query:
SELECT      attr_0-1, attr_0-2, ..., attr_0-m
FROM        big_table AS b0
WHERE       [EXPRESSION]
ORDER BY    [fake_group-by_attr] -- replaces group by attribute

-- Alternate inner query:
SELECT      fake_group-by_attr -- only need one(?)
FROM        big_table AS b0
WHERE       [EXPRESSION]
ORDER BY    [fake_group-by_attr] -- replaces group by attribute

-- Outer query:
SELECT      attr_1-1, arrt_1-2, ..., attr_0-n
FROM        b0  -- Results from inner query
WHERE       [EXPRESSION]
ORDER BY    [order-by_attr]

-- Alternate inner query 1: Returns all fake_group-by_attrs
SELECT      DISTINCT(fake_group-by_attr) AS fgr -- only need one(?)
FROM        big_table AS b0
WHERE       [EXPRESSION]
ORDER BY    [fgr] -- replaces group by attribute

-- Alternative outer query 1:
SELECT      attr_1-1, attr_1-2, ..., attr_0-n
FROM        b0  -- Results from inner query
WHERE       [EXPRESSION]
ORDER BY    fgr

-- Alternate inner query 2: Uses a new column to "aggregate".
SELECT file:///home/azou/git/CMPUT391/tests/queries1.sql

FROM
WHERE
ORDER BY

-- Alternative outer query 2:
SELECT 
FROM
WHERE
ORDER BY

-- Why not only search b0 by one "aggregated" section at a time?

-- How to calculate the time and space complexity...?

*/

/* NOTE: Trying to write what is essentially an ad-hoc query involving
   a makeshift "group-by" or "join" of any kind is misinformed at best!

   Cassandra is better suited to utilizing supercolumns as a way to
   make pre-determined queries (i.e., things you KNOW will be done
   many times on large amounts of data), as opposed to "ad-hoc" queries,
   (i.e., some day after the DB is set up you decide that you want
   to do some new kind of query that you did not design the schema to
   be able to be searched for quickly/efficiently with supercolumns),
   (see Grinev), also (see Stonebraker, Madden, Abadi and Harizopolous).

   Therefore, decide upon the query's results before creating the DB
   schema + supercolumn families, and then you will have a much nicer
   time executing "aggregate" queries.
*/


/* Old proposed query no. 1. To make it simpler, we can use only the first
   10 or so columns of the table.
*/
/* Query 1: Sections taken from queries, 1.5 */
/* Should meet requirements: 1, 3. (All nodes + GROUP BY and ORDER for col-wise. */
/* Meets requirements: 1, maybe 3. */
/* Explanation of what this query does: Selects call data records based on 
 pilot strengths such that at least one pilot strength on the band RD1 or
 RD2 is at least below -12 dB (when scaled appropriately by units of -0.5).
 Groups the results by the city that the calls were made from, and orders
 them by their timestamps.
*/
-- 
/* Note: pilot strengths are measured in units of -0.5 db. (See Agilent
   Technologies Inc. citation).
   TODO: Make this query more interesting and/or valueable. For instance, why
   do we want all of these pilot strengths...?
   One answer - maybe the data is more meaningful if we group it by city and
   order the data by what time the calls were made?
*/
/*
SELECT CITY_ID,
       REPORT_TIME,
       RD1_NOREF_PLT1_PN_STR,
       RD1_NOREF_PLT2_PN_STR,
       RD1_NOREF_PLT3_PN_STR,
       RD1_NOREF_PLT4_PN_STR,
       RD1_NOREF_PLT5_PN_STR,
       RD2_NOREF_PLT1_PN_STR,
       RD2_NOREF_PLT2_PN_STR,
       RD2_NOREF_PLT3_PN_STR,
       RD2_NOREF_PLT4_PN_STR,
       RD2_NOREF_PLT5_PN_STR
FROM   CALL_DETAILS_RECORD
WHERE  (((RD1_NOREF_PLT1_PN_STR * -0.5) < -12)
       OR ((RD1_NOREF_PLT2_PN_STR * -0.5) < -12)
       OR ((RD1_NOREF_PLT3_PN_STR * -0.5) < -12)
       OR ((RD1_NOREF_PLT4_PN_STR * -0.5) < -12)
       OR ((RD1_NOREF_PLT5_PN_STR * -0.5) < -12)
       OR ((RD2_NOREF_PLT1_PN_STR * -0.5) < -12)
       OR ((RD2_NOREF_PLT2_PN_STR * -0.5) < -12)
       OR ((RD2_NOREF_PLT3_PN_STR * -0.5) < -12)
       OR ((RD2_NOREF_PLT4_PN_STR * -0.5) < -12)
       OR (RD2_NOREF_PLT5_PN_STR * -0.5) < -12)
ORDER BY REPORT_TIME;
*/

/* Query 2: Sections taken from queries  */
/* Should meet requirements: 1, 3. (All nodes + GROUP BY and ORDER for col-wise. */
/* Meets requirements: None so far. */
SELECT
FROM   CALL_DETAILS_RECORD 
WHERE
GROUP BY
ORDER BY 

/* Query 3: Sections taken from queries 1.1,  */
/* Should meet requirements: 1, 4. (All nodes + range for col-wise partition). */
/* Meets requirements: 4, and possibly 1 (depending on partition).  */
/* Problems: VERY slow, lots of information selected!
    TODO: When deciding how to partition the table vertically, 
    must determine specific columns from each node to select, as
    opposed to select *.
    It might also be too trivial.
*/

-- TODO: Fix the column names to match the big_data_setup.sql file!!!
SELECT *
FROM   call_details_record 
WHERE  starttime >= to_date('2013-01-15')   -- TODO: Find to_date analogue!!!
       and starttime < to_date('2013-03-16')
       and ini_cell_user_label = '10-1-0-63-2'; -- TODO: ini_cell_user_label!


/* Query 4: Sections taken from queries 2.2,*/
/* Should meet requirements: 1, 4. (All nodes + range for column-wise partition). */
/* Meets requirements: 4, and possibly 1 (depending on partition).  */
/* Problems: There is no way to know if this will access all of the nodes in
   the cluster. (It all depends on how we partition the data.)
   Also, it is still a bit trivial. It is basically an exact copy of query 2.2!
*/
SELECT callingpartynumber, originalcalledpartynumber, finalcalledpartynumber, 
       datetimeconnect, datetimedisconnect, duration, origdevicename, 
       destdevicename
FROM   cdr
WHERE  datetimeconnect>1293930000 AND 
       datetimeconnect<1293930100 AND 
       duration>300 AND 
       duration<900 AND 
       cdrrecordtype=2;

/* Query 5: Sections taken from queries  */
/* Should meet requirements: 2, 4. (>=10 atomic conditions in WHERE + range. */
/* Meets requirements: None so far. */
SELECT  callingpartynumber
FROM    cdr
WHERE   



/*          Queries which satisfy other strategies.                         */
