
CREATE TABLE big_table (
        some_num        INTEGER,
        PRIMARY KEY(some_num)
);

CREATE TABLE small_table (
        num1            INTEGER,
        num2            INTEGER,
        PRIMARY KEY(num1)
);

CREATE TABLE all_small_tables (
        a_num           INTEGER,
        PRIMARY KEY(a_num)
);

ALTER TABLE big_table (ADD col_fam small_table);

/* So now we have a small_table nested in big_table */

/* If we want to count elements of big_table: */
SELECT  COUNT (*)
FROM    big_table;

/* If we want to count all of the elements of small_table   */
SELECT  COUNT (*)
FROM    small_table;

/* If we want to count all of the elements of all of the small_tables
   in the big_table... We must create a new table! */

