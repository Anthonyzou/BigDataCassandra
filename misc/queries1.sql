/*
 *  The list of queries for 391 BigSQL project
 *
 */

-- 1.1
select count(*)
from   call_details_record 
where  starttime >= to_date('2013-01-15')
       and starttime < to_date('2013-03-16')
       and ini_cell_user_label = '10-1-0-63-2';

-- 1.2
select count(*)
from   call_details_record d
where starttime >= to_date('2013-01-15')
      and starttime < to_date('2013-01-16')
      and  LAST_CELL_USER_LABEL = '10-1-0-11-3';

-- 1.3
select count(*)
from   call_details_record d
where  starttime >= to_date('2013-01-01')
       and starttime < to_date('2013-02-01');

-- 1.4
select count(*)
from   call_details_record d
where starttime >= to_date('2013-01-15')
      and starttime < to_date('2014-09-20');

-- 1.5
SELECT count(*)
FROM   call_details_record
WHERE  ((FIRST_PSMM_NONREF_PILOTSTR1 * -0.5) < -12 OR
       (FIRST_PSMM_NONREF_PILOTSTR2 * -0.5) < -12 OR
       (FIRST_PSMM_NONREF_PILOTSTR3 * -0.5) < -12 OR
       (FIRST_PSMM_NONREF_PILOTSTR4 * -0.5) < -12 OR
       (FIRST_PSMM_NONREF_PILOTSTR5 * -0.5) < -12 OR
       (LAST_PSMM_PILOTSSTRENGTH * -0.5) < -12 OR
       (LAST_PSMM_NONREF_PILOTSTR1 * -0.5) < -12 OR
       (LAST_PSMM_NONREF_PILOTSTR2 * -0.5) < -12 OR
       (LAST_PSMM_NONREF_PILOTSTR3 * -0.5) < -12 OR
       (LAST_PSMM_NONREF_PILOTSTR4 * -0.5) < -12 OR
       (LAST_PSMM_NONREF_PILOTSTR5 * -0.5) < -12 OR
       (FIRST_PSMM_PILOTSTR * -0.5) < -12)
       AND (FIRST_PSMM_NONREF_RTD1 - 32) * 0.244 / 2 < 0.5
       AND (FIRST_PSMM_NONREF_RTD2 - 32) * 0.244 / 2 < 0.5
       AND (FIRST_PSMM_NONREF_RTD3 - 32) * 0.244 / 2 < 0.5
       AND (FIRST_PSMM_NONREF_RTD4 - 32) * 0.244 / 2 < 0.5
       AND (FIRST_PSMM_NONREF_RTD5 - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_RTD - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_NONREF_RTD1 - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_NONREF_RTD2 - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_NONREF_RTD3 - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_NONREF_RTD4 - 32) * 0.244 / 2 < 0.5
       AND (LAST_PSMM_NONREF_RTD5 - 32) * 0.244 / 2 < 0.5
       AND (FIRST_PSMM_RTD - 32) * 0.244 / 2 < 0.5;

-- 1.6
select *
from   call_details_record D
WHERE ((RD2_NOREF_PLT4_PN_STR * - 0.5) < -12 OR
       (RD2_NOREF_PLT5_PN_STR * - 0.5) < -12 OR
       (RD1_REF_PILOT_STR * - 0.5) < -12 OR
       (RD1_NOREF_PLT1_PN_STR * - 0.5) < -12 OR
       (RD1_NOREF_PLT2_PN_STR * - 0.5) < -12 OR
       (RD1_NOREF_PLT3_PN_STR * - 0.5) < -12 OR
       (RD1_NOREF_PLT4_PN_STR * - 0.5) < -12 OR
       (RD1_NOREF_PLT5_PN_STR * - 0.5) < -12 OR
       (RD2_REF_PILOT_STR * - 0.5) < -12 OR
       (RD2_NOREF_PLT1_PN_STR * - 0.5) < -12 OR
       (RD2_NOREF_PLT2_PN_STR * - 0.5) < -12 OR
       (RD2_NOREF_PLT3_PN_STR * - 0.5) < -12)
   AND (RD2_NOREF_PLT4_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD2_NOREF_PLT5_RTD - 32) * 0.244 / 2 < 0.5
   AND (ACC_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_REF_PLT_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_NOREF_PLT1_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_NOREF_PLT2_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_NOREF_PLT3_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_NOREF_PLT4_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD1_NOREF_PLT5_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD2_REF_PLT_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD2_NOREF_PLT1_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD2_NOREF_PLT2_RTD - 32) * 0.244 / 2 < 0.5
   AND (RD2_NOREF_PLT3_RTD - 32) * 0.244 / 2 < 0.5
   and starttime >= '2012-08-01 00:00:00'
   and starttime <= '2012-08-07 00:00:00';

-- 1.7  The  IN-SET function

SELECT 'Query 1.7 IN-SET function' FROM dual;

/*
 *  The OP_IN_SET does not work in the where clause with the group by
 */
select INI_BTS_USER_LABEL, INI_CELL_USER_LABEL , count(*)
from   call_details_record  d
where  starttime >= to_date('2012-09-19')
       and starttime < to_date('2014-09-25')
       and ecp_id=4
       and cfc=2
       and Cfc_Qualifier in (104,106)
group by INI_BTS_USER_LABEL, INI_CELL_USER_LABEL ;

select INI_BTS_USER_LABEL, INI_CELL_USER_LABEL 
from   call_details_record  d
where  starttime >= to_date('2012-09-19')
       and starttime < to_date('2014-09-25')
       and ecp_id=4
       and cfc=2
       and Cfc_Qualifier in (104,106);


select INI_BTS_USER_LABEL, INI_CELL_USER_LABEL, count(*)
from call_details_record d
where starttime >= to_date('2012-09-19')
      and starttime < to_date('2014-09-25')
      and msc_code=4
      and cfc=2
       and cfc_qualifier in (104,106)
group by INI_BTS_USER_LABEL, INI_CELL_USER_LABEL ;


-- 1.8

/*
 *   to_char(starttime) does not work as expected
 *   SESS_SFC   
 */
Select d.ini_cell_user_label, CFC, CFC_Qualifier,  count(1)
from   call_details_record d
where  starttime >= to_date('2012-09-10')
       and starttime < to_date('2014-09-16')
       and to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC, CFC_Qualifier;



Select d.ini_cell_user_label, CFC, cfc_qual, count(1)
from   call_details_record d
where  starttime >= to_date('2012-09-10')
   and starttime < to_date('2012-09-16')
   and to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC, cfc_qual;


-- 1.9   NO alloc_service

Select  d.ini_cell_user_label, CFC, CFC_Qualifier, count(1) 
from    call_details_record d
where   starttime >= to_date('2012-09-10')
        and  starttime < to_date('2014-09-16')
--        and  to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC, CFC_Qualifier;


Select  d.ini_cell_user_label, CFC, CFC_Qualifier, count(1)
from    call_details_record d
where   starttime >= to_date('2012-09-10')
   and  starttime < to_date('2014-09-16')
   and  to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC, CFC_Qualifier;


--  1.10   sess_sfc  cfc_qual

Select d.ini_cell_user_label, CFC, count(1)
from   call_details_record d
where  starttime >= to_date('2012-09-10')
   and starttime < to_date('2014-09-16')
   and to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC;


Select d.ini_cell_user_label, CFC,  count(1)
from   call_details_record d
where  starttime >= to_date('2012-09-10')
   and starttime < to_date('2014-09-16')
   and to_char(starttime, '%H:%M:%S') = '10:00:00'
 group by ini_cell_user_label, CFC;


-- 1.11   the decode does not support 
--        call_established 
--        7+to_date
--
select top 100  t.starttime, t.imsi,sum(decode(t.cfc,2,1,0))/count(*) as lostcall_rate 
from   call_details_record t
where  t.starttime >=to_date('2012-07-02') and 
       t.starttime <= to_date('2014-07-02') and
       t.call_established=1
group by t.starttime,t.imsi
order by sum(decode(t.cfc,2,1,0)) desc;


select  t.starttime, t.imsi, sum(t.cfc)/ count(*) 
from   call_details_record t
where  t.starttime >=to_date('2012-07-02 10:00:00') and 
       t.starttime <=to_date('2014-07-09 10:00:00') 
group by t.starttime,t.imsi 
order by sum(t.cfc) desc;

-- 1.12  trunct(date) does not work

select top 100 to_char(t.starttime,'%Y-%m-%d'),t.imsi,  sum(decode(t.cfc,2,1,0))/count(*) lostcall_rate 
from  call_details_record t
where t.starttime >=to_date('2012-07-02') 
      and t.starttime <= to_date('2014-07-02')
      and t.call_established=1
group by  to_char(t.starttime,'%Y:%m-%d'), t.imsi
order by sum(decode(t.cfc,2,1,0)) desc;


select top 10  t.starttime, t.imsi, sum(t.cfc)/count(*) lostcall_rate 
from    call_details_record  t
where   t.starttime >=to_date('2012-07-02 10:00:00') and 
        t.starttime <=to_date('2014-07-09 10:00:00') and
        t.call_established=1
group by t.starttime, t.imsi
order by sum(t.cfc) desc;

-- 1.13   difference between count(1) and count(*)


SELECT 'Q 1.13 ' FROM dual;

Select count(*)
from   call_details_record t
Where  starttime >= '2012-08-01 00:00:00'
   and starttime <= '2014-08-01 23:00:00'
   and alloc_service = 33


Select count(*)
from   call_details_record t
Where  starttime >= '2012-08-01 00:00:00'
       and starttime <= '2014-08-02 23:00:00'
       and imsi in  ( select imsi
                      from call_details_record d
                      where starttime > '2012-08-01 00:00:00'
                       and starttime <= '2014-08-01 23:00:00');


SELECT 'Q 2.9 ' FROM dual;

select count(*)
from   ( select t.starttime,t.imsi, sum(decode(t.cfc,2,1,0))/count(sequence_number) lostcall_rate 
         from   call_details_record t
         where  starttime >= '2012-07-02 10:00:00' and
                t.starttime <= '2014-07-09 10:00:00'
         group by t.starttime,t.imsi
        );


select count(*)
from   ( select t.starttime,t.imsi
         from   call_details_record t
         where  starttime >= '2012-07-02 10:00:00' and
                t.starttime <= '2014-07-09 10:00:00'
         group by t.starttime,t.imsi
        );

select count(*)
from  ( select distinct t.starttime,t.imsi
         from   call_details_record t
         where  starttime >= '2012-07-02 10:00:00' and
                t.starttime <= '2014-07-09 10:00:00'
        );



select top 10  t.starttime,t.imsi, sum(decode(t.cfc,2,1,0)) as sum,  sum(decode(t.cfc,2,1,0))/count(sequence_number) lostcall_rate 
from   call_details_record t
where  starttime >= '2012-07-02 10:00:00' and
       t.starttime <= '2014-07-09 10:00:00'
group by t.starttime,t.imsi
order by sum(decode(t.cfc,2,1,0)) desc;



select count(*) 
from ( select t.starttime,t.imsi,sum(decode(t.cfc,2,1,0))/count(*) lostcall_rate 
       from   call_details_record t
       where  t.starttime >= '2012-07-02 10:00:00' and 
              t.starttime <= '2014-07-09 10:00:00'
       group by t.starttime,t.imsi
     );



-- The fillowings demonstrate the difference between top 10 and first rownum 10

select  top 10 t.starttime,t.imsi,sum(decode(t.cfc,2,1,0))/count(SEQUENCE_NUMBER) lostcall_rate 
from    call_details_record t
where   starttime >= '2012-07-02 10:00:00' and 
        starttime <= '2014-07-09 10:00:00'
group by t.starttime,t.imsi
order by 3;

select  first rownum 10 t.starttime,t.imsi,sum(decode(t.cfc,2,1,0))/count(SEQUENCE_NUMBER) lostcall_rate 
from    call_details_record t
where   starttime >= '2012-07-02 10:00:00' and 
        starttime <= '2014-07-09 10:00:00'
group by t.starttime,t.imsi;


--   This one is extremely slow, and do not try it for a big table. 
--   A new code must be developed for this one, which is not so difficult.

select count(*)
from   call_details_record c
where  c.starttime >= '2012-07-02 10:00:00' and 
       c.starttime <= '2014-07-09 10:00:00' and
       exists ( select *
                from call_details_record d
                where c.imsi = d.imsi and
                      d.starttime >= '2012-07-02 10:00:00' and
                      d.starttime <= '2014-07-09 10:00:00');


