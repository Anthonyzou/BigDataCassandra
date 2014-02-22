/*
 *  The list of queries for 391 BigSQL project
 *
 */

--  Query 2.1
SELECT callingpartynumber, originalcalledpartynumber, finalcalledpartynumber, 
       datetimeconnect, datetimedisconnect, duration, origdevicename, destdevicename
FROM   cdr
WHERE  duration = 60;

--  Query 2.2
SELECT callingpartynumber, originalcalledpartynumber, finalcalledpartynumber, 
       datetimeconnect, datetimedisconnect, duration, origdevicename, destdevicename
FROM   cdr
WHERE  datetimeconnect>1293930000 AND 
       datetimeconnect<1293930100 AND 
       duration>300 AND 
       duration<900 AND 
       cdrrecordtype=2;

-- Query 2.3
SELECT callingpartynumber, finalcalledpartynumber, SUM(duration)
FROM   cdr
WHERE  datetimeconnect>1293930000 AND 
       datetimeconnect<1293930100 AND 
       duration>300  AND 
       duration<900  AND 
       cdrrecordtype=2
GROUP BY callingpartynumber, finalcalledpartynumber
ORDER BY callingpartynumber, finalcalledpartynumber ASC;


-- Query 2.4

SELECT  callingpartynumber, finalcalledpartynumber, DurAvg, ranking
FROM    ( SELECT callingpartynumber, finalcalledpartynumber, RANK(finalcalledpartynumber) as ranking, DurAvg
          FROM  ( SELECT callingpartynumber, finalcalledpartynumber, SUM(duration)/count(*) as DurAvg
                  FROM cdr
		  WHERE duration>300  AND 
		        duration<900  AND 
			cdrrecordtype=2
		  GROUP BY callingpartynumber, finalcalledpartynumber
                 ) a
		 DISTRIBUTE BY callingpartynumber, finalcalledpartynumber
  		 SORT BY callingpartynumber, finalcalledpartynumber, DurAvg DESC
        ) b
WHERE ranking < 101
ORDER BY callingpartynumber, ranking;


--  Query 2.5
SELECT callingpartynumber, finalcalledpartynumber, SUM(duration)/count(*) as DurAvg
FROM   cdr
WHERE  duration > 300 AND 
       duration<900   AND 
       cdrrecordtype=2
GROUP BY callingpartynumber, finalcalledpartynumber
ORDER BY callingpartynumber DESC
LIMIT 100;