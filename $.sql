!!!;
--alter user prod_upd account unlock;
--select * from dict where table_name like '%UNDO%' ORDER BY TABLE_NAME;
/*select * from dba_sequences s where REGEXP_LIKE (s.sequence_name, 'sql_list', 'i');
select prcore.SQ_emp_duty.nextval from dual;*/
select df.tablespace_name, df.autoextensible, df.file_name, vdf.CREATION_TIME, b.freeMB, round(df.bytes/1024/1024, 2) as file_size, round(df.maxbytes/1024/1024, 2) as max_size, df.status--, vdf.*
--, 'ALTER TABLESPACE "'||df.tablespace_name||'" ADD DATAFILE '''||df.file_name||''' SIZE 1000M AUTOEXTEND ON NEXT 500M MAXSIZE 32000M' as add_df
--, 'ALTER TABLESPACE "'||df.tablespace_name||'" ADD DATAFILE '''||substr(df.file_name, 1, length(df.file_name)-6)||(to_number(REGEXP_replace(substr(df.file_name, length(df.file_name)-7, 6), '\D') /*substr(df.file_name, length(df.file_name)-5, 2)*/)+1)||'.dbf'' SIZE 1000M AUTOEXTEND ON NEXT 500M MAXSIZE 32000M' as add_df
, 'ALTER TABLESPACE "'||df.tablespace_name||'" ADD DATAFILE '''||substr(df.file_name, 1, instr(df.file_name, '/', -1, 1))||regexp_replace(substr(df.file_name, instr(df.file_name, '/', -1, 1), length(df.file_name)-instr(df.file_name, '/', -1, 1)-3), '\W|\d')||(to_number(REGEXP_replace(substr(df.file_name, length(df.file_name)-7, 6), '\D') /*substr(df.file_name, length(df.file_name)-5, 2)*/)+1)||'.dbf'' SIZE 10000M AUTOEXTEND ON NEXT 500M MAXSIZE 32000M;' as add_df
, 'alter database datafile '''||df.file_name||''' resize '||coalesce(to_char(round(round(df.bytes/1024/1024, 2)-b.freeMB, -(length(to_char(b.freemb))-2))), '32000')||'M;' res_df
from DBA_DATA_FILES df, v$datafile vdf,
(SELECT file_id, round(SUM(bytes)/1024/1024, 0) freeMB
    FROM dba_free_space b GROUP BY file_id) b
where 1=1
  and df.file_id = b.file_id(+)  and df.file_id = vdf.FILE#
  and REGEXP_LIKE (df.tablespace_name, 'bigtab', 'i')
--  and REGEXP_LIKE (df.file_name, '18', 'i')
--  and vdf.CREATION_TIME > trunc(sysdate-70)
order by vdf.creation_time desc
;-- посмотреть табличные пространства с файлами



SELECT * FROM foundation_area.payment AS OF TIMESTAMP
   TO_TIMESTAMP('2018-01-31 16:30:00', 'YYYY-MM-DD HH24:MI:SS');

--Использование места
select
    a.tablespace_name "TABLESPACE",
    round(a.bytes_alloc/(1024*1024*1024),0) "TOTAL ALLOC (GB)",
    round(a.physical_bytes/(1024*1024*1024),0) "TOTAL PHYS ALLOC (GB)",
    round(nvl(b.tot_used,0)/(1024*1024*1024),0) "USED (GB)",
    round((nvl(b.tot_used,0)/a.bytes_alloc)*100,2) "% USED",
    round((nvl(a.bytes_alloc,0)-b.tot_used)/(1024*1024*1024),1) "EST (GB)",
    case 
         when round((nvl(b.tot_used,0)/a.bytes_alloc)*100,2) between 98 and 98.99 then 'WARNING'
         when round((nvl(b.tot_used,0)/a.bytes_alloc)*100,2) between 99 and 100 then 'CRITICAL'
         else 'OK'
    end as status
from
   (select tablespace_name, sum(bytes) physical_bytes, sum(decode(autoextensible,'NO',bytes,'YES',maxbytes)) bytes_alloc
    from dba_data_files
    group by tablespace_name ) a,
   (select tablespace_name, sum(bytes) tot_used 
    from dba_segments
    group by tablespace_name ) b
where a.tablespace_name = b.tablespace_name (+)
  and a.tablespace_name not in (select distinct tablespace_name from dba_temp_files)
--  and a.tablespace_name not like 'UNDO%'
  and a.tablespace_name not in (/*'APEX42',*/'APEX_1303706562313273','FMW_BIPLATFORM','FMW_MDS')
order by 5 desc;

select * from dba_temp_files;

select tfs.tablespace_size/1024/1024/1024 as TS_GB, tfs.allocated_space/1024/1024/1024 as AS_GB, round(tfs.free_space/1024/1024/1024, 2) as FS_GB from dba_temp_free_space tfs;

--использование темпа сессиями
select tu.USERNAME, vs.OSUSER, vs.sid, vs.PROGRAM, vs.SQL_EXEC_START, vs.WAIT_CLASS, tu.SQL_ID, tu.blocks*8/1024/1024 as size_GB--, tu.* 
from v$tempseg_usage tu, v$session vs
where 1=1 and tu.SESSION_NUM = vs.SERIAL#(+)
order by tu.BLOCKS desc;

--фрагментация
with frag as 
(
  select owner
         ,table_name
         ,dt.tablespace_name
         ,round((blocks*8)/1024/1024,2) Fragmented_size_MB
         , round((num_rows*avg_row_len/1024/1024),2) Actual_size_MB
         , round((round((blocks*8),2)-round((num_rows*avg_row_len/1024),2))/1024, 1) as free_space_mb
         ,round(((round((blocks*8),2)-round((num_rows*avg_row_len/1024),2))/coalesce(case blocks when 0 then 1 else round((blocks*8),2) end,  1))*100 -10, 1) recl_prcnt
  from dba_tables dt
  where 1=1
    and blocks!= 0
    and REGEXP_LIKE(dt.tablespace_name, 'BIG_DATA$', 'i') 
  --  and REGEXP_LIKE(table_name, 'CLIENT_FCST_REP', 'i') 
--    and REGEXP_LIKE(owner, '^pr', 'i') 
)
select frag.*, sum(free_space_mb) over (partition by 1) as summ_free_MB from frag
where 1=1 
  and Actual_size_mb > 100
--  and 
order by free_space_mb desc, recl_prcnt desc
;

--Посмотреть размеры таблиц
select s.owner||'.'||s.segment_name as segment_name, s.segment_type, coalesce(i.table_name, l.table_name, sp.table_name, p.table_name, s.segment_name) as table_name, l.column_name, i.table_name, l.table_name, ips.index_name, sp.table_name, ip.index_name, p.table_name, s.partition_name, coalesce(lp.lob_partition_name, lp1.partition_name) as lob_part, p.high_value, sp.high_value, s.owner, s.tablespace_name, l.in_row, s.bytes/1024/1024 MB, round(sum(s.bytes/1024/1024) over (partition by 1)) as sum_MB
, 'alter table '||s.owner||'.'||s.segment_name||' drop partition '||s.partition_name||' update indexes;' as stmnt
, case when l.segment_name is not null then 'alter table '||l.owner||'.'||l.table_name||' modify lob('||l.column_name||') (shrink space);' end as stmnt2
--s.tablespace_name, sum(s.bytes/1024/1024) as MB, count(*) as cnt
from dba_segments s
left outer join dba_indexes i on s.segment_name = i.index_name and s.segment_type = 'INDEX' and s.owner = i.owner
left outer join dba_lobs l on s.segment_name = l.segment_name and s.owner = l.owner and s.segment_type = 'LOBSEGMENT'
left outer join dba_tab_partitions p on s.partition_name = p.partition_name and s.segment_name = p.table_name and s.owner = p.table_owner
left outer join dba_tab_subpartitions sp on s.partition_name = sp.subpartition_name
left outer join dba_lob_partitions lp on s.partition_name = lp.partition_name
left outer join dba_lob_partitions lp1 on s.partition_name = lp1.lob_partition_name
left outer join dba_ind_partitions ip on s.segment_name = ip.index_name and s.owner = ip.index_owner and s.partition_name = ip.partition_name
left outer join dba_ind_subpartitions ips on s.segment_name = ips.index_name and s.owner = ips.index_owner and s.partition_name = ips.subpartition_name
where 1=1
--  and REGEXP_LIKE (coalesce(i.table_name, l.table_name, sp.table_name, p.table_name, s.segment_name), '^ARCH_', 'i')-- or s.segment_name = 'SYS_LOB0021966524C00003$$'
--  and s.segment_name = 'SYS_LOB0021966524C00003$$'
--  and REGEXP_LIKE (segment_type, 'tab', 'i')
--  and REGEXP_LIKE (s.owner, 'pristav13800', 'i')
  and REGEXP_LIKE (s.tablespace_name, 'APEX_LOB', 'i')
--  and REGEXP_LIKE (s.segment_name, 'ARCH_SNP_SESS_TASK$', 'i')
--  and s.segment_name = 'WWV_FLOW_DATA'
--  and REGEXP_LIKE (l.owner||'.'||l.table_name, 'APP_ERROR_LOG|^.$', 'i') 
--group by s.tablespace_name order by sum(s.bytes/1024/1024) desc
order by s.bytes desc
;

select * from dba_tables where 1=1 and REGEXP_LIKE(table_name, 'ARCH_SNP_SESS_TASK$', 'i') ;


--select 1105461*1024/ from dual
select r.owner, count(*), 'purge tablespace ' ||r.owner||';' from dba_recyclebin r group by r.owner;
select r.ts_name, count(*), 'purge tablespace ' ||r.ts_name||';' from dba_recyclebin r group by r.ts_name;
select distinct 'purge tablespace '||r.ts_name||' user '||r.owner||';' from dba_recyclebin r where r.ts_name is not null;
select distinct 'purge table "'||r.owner||'"."'||r.object_name||'";' from dba_recyclebin r;
select count(*) from dba_recyclebin;

select s.owner||'.'||s.segment_name as segment_name, s.segment_type, coalesce(i.table_name, l.table_name, s.segment_name) as table_name, l.column_name, s.partition_name, s.tablespace_name, round(s.bytes/1024/1024) as MB, round(sum(s.bytes/1024/1024) over (partition by 1)) as sum_MB
--s.tablespace_name, sum(s.bytes/1024/1024) as MB, count(*) as cnt
from dba_segments s
left outer join dba_indexes i on s.segment_name = i.index_name and s.segment_type = 'INDEX' and s.owner = i.owner
left outer join dba_lobs l on s.segment_name = l.segment_name and s.owner = l.owner and s.segment_type = 'LOBSEGMENT'
where 1=1
--  and REGEXP_LIKE (segment_type, 'TABLE PARTITION', 'i')
--  and REGEXP_LIKE (s.owner, 'MSSQL_MIGR$', 'i')
--  and (s.segment_name in ('JOB_LOG', 'LSESS_LOG', 'LCONTEXT_LOG', 'EVENT_LOG', 'APP_LOG', 'EMAIL_QUEUE', 'APP_ERROR') or i.table_name in ('JOB_LOG', 'LSESS_LOG', 'LCONTEXT_LOG', 'EVENT_LOG', 'APP_LOG', 'EMAIL_QUEUE', 'APP_ERROR'))
-- and REGEXP_LIKE (coalesce(i.table_name, l.table_name, s.segment_name), 'SNP_SESS_TASK_LOG', 'i') 
--  and s.segment_type = 'INDEX'
  and REGEXP_LIKE(s.tablespace_name, '^users', 'i')
--  and REGEXP_LIKE(s.segment_name, '^WWV_FLOW_DATA$', 'i')
--group by s.tablespace_name order by sum(s.bytes/1024/1024) desc
order by s.bytes desc
;

select pt.partitioning_type, p.table_name, p.partition_name, p.interval, p.segment_created, p.*  
from dba_tab_partitions p 
inner join dba_part_tables pt on p.table_owner = pt.owner and p.table_name = pt.table_name
where 1=1
  and REGEXP_LIKE (p.table_name, 'TMP_APEX_FILE_STORAGE', 'i')
  --and REGEXP_LIKE (p.partition_name, '432', 'i') 
;

--Alertlog
select to_char(originating_timestamp, 'DD-MM-YYYY HH24:MI:SS') as timestamp_c, message_text
from
   x$dbgalertext xa
where 1=1
--  and originating_timestamp > trunc(sysdate)
  and originating_timestamp > sysdate-1/24/60
--  and to_char(originating_timestamp, 'DD-MM-YYYY HH24:MI:SS') like '20-10-2018 00%' 
--  and REGEXP_LIKE (to_char(originating_timestamp, 'DD-MM-YYYY HH24:MI:SS'), '^20-10-2018 0(0|1)', 'i')
--  and REGEXP_LIKE (message_text, 'TEMP', 'i')  
--  and not REGEXP_LIKE (message_text, 'ORA-19505|Archived Log entry|Standby redo logfile selected|advanced to log sequence|Current log', 'i')  
order by xa.ORIGINATING_TIMESTAMP desc;-- просмотр алерт лога


--Блокирующие сессии
with w as (
select
 chain_id cid,rownum n,level l
 ,lpad(' |',level,' ')||(select instance_name from gv$instance where inst_id=w.instance)||' '''||w.sid||','||w.sess_serial#||'@'||w.instance||'''' "session"
 ,lpad(' ',level,' ')||w.wait_event_text ||
   case
   when w.wait_event_text like 'enq: TM%' then
    ' mode '||decode(w.p1 ,1414332418,'Row-S' ,1414332419,'Row-X' ,1414332420,'Share' ,1414332421,'Share RX' ,1414332422,'eXclusive')
     ||( select ' on '||object_type||' "'||owner||'"."'||object_name||'" ' from all_objects where object_id=w.p2 )
   when w.wait_event_text like 'enq: TX%' then     
   (
     select ' on '||object_type||' "'||owner||'"."'||object_name||'" on rowid '
     ||dbms_rowid.rowid_create(1,data_object_id,relative_fno,w.row_wait_block#,w.row_wait_row#)
     from all_objects ,dba_data_files where object_id=w.row_wait_obj# and w.row_wait_file#=file_id
   )
   else
   (
     ' mode '||w.p1_text||' '||w.p1||' '||w.p2_text||' '||w.p2||' '||w.p3_text||' '||w.p3
     ||( select ' on '||object_type||' "'||owner||'"."'||object_name||'" ' from all_objects where object_id=w.p2 )
     ||(select ' on '||object_type||' "'||owner||'"."'||object_name||'" on rowid '
     ||dbms_rowid.rowid_create(1,data_object_id,relative_fno,w.row_wait_block#,w.row_wait_row#)
     from all_objects ,dba_data_files where object_id=w.row_wait_obj# and w.row_wait_file#=file_id)
   )     
   end "wait event e"
 , w.in_wait_secs "secs"
 , s.username uname, s.osuser , s.program prg, s.sql_id, (select distinct sq.SQL_TEXT from v$sql sq where sq.SQL_ID = s.sql_id) as sql_text--, w.*
 , 'alter system DISCONNECT SESSION ''' || w.sid || ',' || w.sess_serial# || ''' immediate;' as stmnt
 from v$wait_chains w join gv$session s on (s.sid=w.sid and s.serial#=w.sess_serial# and s.inst_id=w.instance)
 connect by prior w.sid=w.blocker_sid and prior w.sess_serial#=w.blocker_sess_serial# and prior w.instance = w.blocker_instance
 start with w.blocker_sid is null
)
select * from w where cid in (select cid from w group by cid having max("secs") >= 1 and max(l)>1 ) 
--and L = 1
order by n
;

select v.EVENT, count(*)  from v$session v where 1=1 and (v.TYPE = 'USER') group by v.EVENT order by count(*) desc;

--ещё блокировки
SELECT s.SID, USERNAME AS "User", PROGRAM, MODULE,
       ACTION, LOGON_TIME "Logon", l.*
FROM V$SESSION s, V$ENQUEUE_LOCK l
WHERE l.SID = s.SID
  and s.sid = 890
--  and s.sid in (618, 767, 1102, 1504, 2790, 3068, 3203, 3213, 3272, 3547, 3946, 4970, 5525, 7078)
--AND l.TYPE = 'CF'
--AND l.TYPE = 'TO'
--AND l.ID1 = 0
--AND l.ID2 = 2
;

select 'alter system DISCONNECT SESSION ''' || s.sid || ',' || s.serial# || ''' immediate;' as stmn, trunc((sysdate-s.logon_time)*24*60*60, 2) as dur_ses, trunc((sysdate-s.SQL_EXEC_START)*24*60*60, 2) as dur_sql, s.SQL_EXEC_START, s.sql_id, s.EVENT, s.WAIT_CLASS, s.LOGON_TIME, s.* 
from v$session s 
where 1=1
--  and s.STATUS = 'ACTIVE'
--  and s.SQL_EXEC_START between to_date('21-03-2018 5:30:00', 'DD.MM.YYYY HH24:MI:SS') and to_date('21032018 5:40:00', 'DD.MM.YYYY HH24:MI:SS')
--  and s.LOGON_TIme between to_date('21-03-2018 5:30:00', 'DD.MM.YYYY HH24:MI:SS') and to_date('21032018 5:40:00', 'DD.MM.YYYY HH24:MI:SS')
  and s.SCHEMANAME!= 'SYS'
--  and s.SID in (4468)
  and s.sql_id is not null and s.WAIT_CLASS = 'Idle' and trunc((sysdate-s.logon_time)*24*60*60, 2) > 3600
--  and trunc((sysdate-s.SQL_EXEC_START)*24*60*60, 2) > 180
--  and REGEXP_LIKE (s.USERNAME, 'apex', 'i')
--  and s.SQL_ID = '6nwc3tfax70dy'
--group by s.SCHEMANAME
order by coalesce(trunc((sysdate-s.logon_time)*24*60*60, 2), 0) desc
;  

select vue.* 
  from prod_upd.vw_user_edition vue
where 1=1 
--  and REGEXP_LIKE(username, 'pristav', 'i')   
  and vue.OBJECT_NAME = 'RELEASE_03.69.00'
;  


select sid, event, time_waited, time_waited_micro from v$session_event 
where sid=1633 order by 3;


-- поиск в сессиях
select distinct 
count(*) over (partition by 1) as sid_cnt , s.LAST_CALL_ET
,w.BLOCKER_SID as b_sid, s.BLOCKING_SESSION --, w.BLOCKER_SESS_SERIAL#
/*,regexp_replace('kill -9 '||to_char(wm_concat(p.spid) over (partition by 1)), ',', ' ') as kill_stm*/
--, 'alter system DISCONNECT SESSION ''' || s.sid || ',' || s.serial# || ''' immediate;' as stmn
,(select o.object_name from all_objects o where o.object_id = s.session_edition_id) as edition_name
,s.sid, p.SPID, q.SQL_ID, q.SQL_TEXT, s.USERNAME, s.osuser, s.EVENT, s.WAIT_CLASS, s.LOGON_TIME, s.MODULE, s.machine, s.action, s.program, rsi.MAPPED_CONSUMER_GROUP, rsi.MAPPING_ATTRIBUTE, rsi.STATE, rsi.ACTIVE_TIME, s.status,  pvs.degree, pvs.REQ_DEGREE, trunc((sysdate-s.logon_time)*24*60*60, 2) as dur_sec, trunc((sysdate-s.logon_time)*24*60*60, 2)/60/60/24 as dur_dd, s.SQL_EXEC_START, s.PREV_EXEC_START
--, q.SQL_FULLTEXT--
, s.*
from v$session s
left outer join v$process p on s.pADDR = p.ADDR
left outer join v$sql q on s.SQL_HASH_VALUE = q.HASH_VALUE
left outer join V$PX_SESSION pvs on s.SID = pvs.SID
left outer join V$RSRC_SESSION_INFO rsi on s.sid = rsi.sid
left outer join v$wait_chains w on s.sid = w.sid
where 1=1
--  and REGEXP_LIKE (s.program, 'crm', 'i') 
--  and s.SID in (415)
--  and s.LOGON_TIme between to_date('21-03-2018 5:30:00', 'DD.MM.YYYY HH24:MI:SS') and to_date('21032018 5:40:00', 'DD.MM.YYYY HH24:MI:SS')
--  and p.PID = 970
  and REGEXP_LIKE (s.STATUS, '^active', 'i')
  and s.TYPE = 'USER'
--  and trunc((sysdate-s.logon_time)*24*60*60, 2)/60/60 > 1
--  and REGEXP_LIKE(s.event, 'latch|lock', 'i') 
--  and w.BLOCKER_SID is not null
--  and REGEXP_LIKE (s.USERNAME, 'PRNODE_TEST', 'i')
--  and (select o.object_name from all_objects o where o.object_id = s.session_edition_id)!= 'RELEASE_03.73.00'
--  and rsi.MAPPED_CONSUMER_GROUP is null
--  and REGEXP_LIKE (s.OSUSER, 'PRISTAV13988', 'i')
--  and s.sql_id = '310ynvjg2nk2g'
--  and s.MACHINE = 'priserv070.pristav.int'
--  and REGEXP_LIKE (s.WAIT_CLASS, 'Concurrency', 'i') 
--  and REGEXP_LIKE (s.PROCESS, '7277|1530|28438', 'i')
--  and REGEXP_LIKE (s.USERNAME, '^equifax', 'i')
--  and s.EVENT like 'SQL*Net break/reset to client'
--  and REGEXP_LIKE (q.SQL_TEXT, '14', 'i')
--  and upper(q.SQL_TEXT) like upper('%product%')
order by s.LOGON_TIME asc, s.SQL_EXEC_START desc;

--Сессии длительное время в актвином статусе
select s.username,s.sid,s.status,s.last_call_et,s.logon_time, s.SQL_EXEC_START
from v$session s
left outer join v$process p on s.pADDR = p.ADDR
where 1=1
  and s.username is not null 
  and s.type = 'USER'
  and s.last_call_et>(3600*6)
  and s.status='ACTIVE';
  

select v.status, count(*)  from v$session v where 1=1 and (v.TYPE = 'USER' or v.module = 'DBMS_SCHEDULER') group by v.status;

select v.username, count(*)  from v$session v where 1=1 and (v.TYPE = 'USER' or v.module = 'DBMS_SCHEDULER') and v.status = 'ACTIVE' group by v.username;

select v.username, count(*)  from v$session v where 1=1 and v.TYPE = 'USER' and v.status = 'ACTIVE' and REGEXP_LIKE(v.osuser, 'pristav', 'i') group by v.username having count(*) > 1;

--Использование pga сессиями
SELECT DECODE(TRUNC(SYSDATE - LOGON_TIME), 0, NULL, TRUNC(SYSDATE - LOGON_TIME) || ' Days' || ' + ') || 
TO_CHAR(TO_DATE(TRUNC(MOD(SYSDATE-LOGON_TIME,1) * 86400), 'SSSSS'), 'HH24:MI:SS') LOGON, 
v$session.SID, v$session.SERIAL#, v$process.SPID , ROUND(v$process.pga_used_mem/(1024*1024), 2) PGA_MB_USED, round(v$sesstat.value/1024/1024, 2) as PGA_MB_USED_MAX,-- v$statname.name,
ROUND((sum(v$process.pga_used_mem) over (partition by 1))/(1024*1024), 2) as PGA_MB_USED_TOTAL,
v$session.USERNAME, STATUS, OSUSER, MACHINE, sql_id, v$session.PROGRAM, MODULE 
FROM v$session, v$process, v$sesstat, v$statname
WHERE v$session.paddr = v$process.addr and v$sesstat.statistic# = v$statname.STATISTIC# and v$sesstat.sid = v$session.sid and v$statname.name like '%pga%memory%max%'
--and status = 'ACTIVE' 
--and v$session.sid = 623
--and v$session.username = 'SYSTEM' 
--and v$process.spid = 24301
ORDER BY pga_used_mem DESC;

--Сессии выполняющие активные транзакции
SELECT TO_CHAR(s.sid)||','||TO_CHAR(s.serial#) sid_serial,
       NVL(s.username, 'None') orauser,
       NVL(s.osuser, 'None') osuser,
       s.program,
       s.event,
       r.name undoseg,
       (select round(sum(ue.blocks)*8/1024/1024,1) as MB from dba_undo_extents ue where 1=1 and ue.segment_name = r.name) as GB,
       t.used_ublk * TO_NUMBER(x.value)/1024||'K' "Undo",
       round(t.used_ublk * TO_NUMBER(x.value)/1024/1024/1024, 2)||'GB' "UndoGB"
  FROM sys.v_$rollname    r,
       sys.v_$session     s,
       sys.v_$transaction t,
       sys.v_$parameter   x
 WHERE s.taddr = t.addr
   AND r.usn   = t.xidusn(+)
   AND x.name  = 'db_block_size'
order by t.USED_UBLK desc   
;

select 792194392*0.8*0.2/1024/1024 from dual;
-- поиск в запросах
select 
vsq.SQL_TEXT, vsq.SQL_FULLTEXT, vsq.PARSING_SCHEMA_NAME, vsq.LAST_ACTIVE_TIME, vsq.FIRST_LOAD_TIME, vsq.LAST_LOAD_TIME, vsq.SQL_ID, vsq.EXECUTIONS, vsq.ACTION, vs.SID, vs.PROGRAM, vs1.SID, vs1.PROGRAM, vs.ACTION, vs.MACHINE, vs.SQL_EXEC_START, vs1.MACHINE, vs1.SQL_EXEC_START, vs.EVENT, vs.WAIT_CLASS, vsq.DISK_READS, trunc((sysdate - vs.SQL_EXEC_START)*24*60, 1) as duration_min, pvs.degree, pvs.REQ_DEGREE
,vsq.SQLTYPE, vs.COMMAND, vs.LAST_CALL_ET, vs.status, vs.LOGON_TIME, vs.RESOURCE_CONSUMER_GROUP, 'ALTER SYSTEM DISCONNECT SESSION ''' || vs.sid || ',' || vs.serial# || ''' IMMEDIATE;'
--, vsq.*
--sum(pvs.DEGREE) 
from v$sql vsq
left outer join v$session vs on vsq.HASH_VALUE = vs.SQL_HASH_VALUE
left outer join v$session vs1 on vsq.sql_id = vs1.SQL_id
left outer join V$PX_SESSION pvs on vs.SID = pvs.SID
where 1=1
--  and vsq.sqltype = 12
  and vsq.SQL_ID = '8ggw94h7mvxd7'
--  and vsq.sql_id like 'cdyzkpxkg9nx4%'
--  and upper(vsq.SQL_TEXT) like upper('%prtran.calc%')
--  and REGEXP_LIKE (vs.action, '58685898', 'i')
--  and vsq.LAST_ACTIVE_TIME > trunc(sysdate)
--  and REGEXP_LIKE (vsq.PARSING_SCHEMA_NAME, 'migr', 'i')
--  and vsq.PARSING_SCHEMA_NAME!= 'SYS'
--  and  REGEXP_LIKE (vsq.action, 'shed', 'i')
--  and REGEXP_LIKE (vs.MACHINE, '13988', 'i')
--  and vs.status = 'ACTIVE'
--  and vsq.LAST_ACTIVE_TIME between to_date('15-05-2017 14:40:00', 'DD-MM-YYYY HH24:MI:SS') and to_date('15-05-2017 15:10:00', 'DD-MM-YYYY HH24:MI:SS')
order by vsq.LAST_ACTIVE_TIME desc
;

select * from v$sqltext q where 1=1 and q.sql_id = 'f9jp24031snh2' order by q.PIECE;


--Просмотр байнд переменных
select * from V$SQL_BIND_CAPTURE vb where vb.sql_id = 'bh6x90jc21zbs';

select * from DBA_HIST_SQLBIND db 
where 1=1
  and db.sql_id = '3x2s6nggg02w2' 
--  and db.value_string = '45739040'
  and db.last_captured is not null
--  and db.
order by db.LAST_CAPTURED desc;

--ASH
select u.username, ve.NAME, ve.WAIT_CLASS, ash.SQL_ID, sq.SQL_TEXT, ash.TIME_WAITED, ash.WAIT_TIME, ash.* from v$active_session_history ash
left outer join dba_users u on ash.USER_ID = u.user_id
left outer join v$event_name ve on ash.EVENT_ID = ve.EVENT_ID
left outer join v$sql sq on ash.SQL_ID = sq.SQL_ID
--left outer join (select distinct SQL_TEXT, SQL_ID from v$sql) vsq on ash.SQL_ID = vsq.sql_id
--left outer join v$session vs on ash.SESSION_ID = vs.sid
where 1=1
--  and REGEXP_LIKE (ash.MACHINE, 'priserv246', 'i') 
--  and REGEXP_LIKE (u.username, 'apex', 'i') 
--  and REGEXP_LIKE (ve.name, 'lock|latch', 'i') 
--  and ash.w
  and ash.SESSION_ID = 4360
  and u.username = 'PRCORE'
--  and ash.MACHINE = '02SPB36'
--  and ash.sql_id like '%ggs8xhn8yr44m%'
-- and REGEXP_LIKE (ash.CLIENT_ID, '15357', 'i') 
--  and ash.sql_opname = 'UPDATE'Lion1986
--  and ash.SQL_EXEC_START between to_date('22062017 0:00','ddmmyyyy hh24:mi') and to_date('22062017 8:00','ddmmyyyy hh24:mi')
--  and upper(sq.SQL_TEXT) like upper('%(select 1 ,t.id_status ,t.id_participant ,%')
order by ash.SAMPLE_TIME desc
;

--Поиск объектов для компиляции
select distinct
case
  when o.object_type in ('FUNCTION', 'PROCEDURE', 'TRIGGER', 'VIEW', 'MATERIALIZED VIEW', 'TYPE') then 'alter '||o.object_type||' '||o.owner||'.'||o.object_name||' compile;-- PLSQL_DEBUG = TRUE;'
  when o.object_type in ('PACKAGE', 'PACKAGE BODY') then 'alter package '||o.owner||'.'||o.object_name||' compile '|| case object_type when 'PACKAGE BODY' then 'body' else 'specification' end||';-- PLSQL_DEBUG = TRUE;'
end as compile_stm
,case
  when REGEXP_LIKE (o.object_type, '^func|^proc|^pack|^type', 'i')  then 'grant execute, debug on '||o.owner||'.'||o.object_name||' to '||'&user;--with grant option;'
  when REGEXP_LIKE (o.object_type, '^tab|view|^seq', 'i') then 'grant select/*, insert, update, delete*/ on '||o.owner||'.'||o.object_name||' to '||'&user;-- with grant option;'
  else 'none'
end as grant_stm 
,o.owner||'.'||o.object_name as obj
, 'drop '||o.object_type||' '||o.owner||'.'||o.object_name||';' as droppp
,o.*--, s.*
from dba_objects o
--left join dba_segments s on o.owner = s.owner and o.object_name = s.segment_name
where 1=1
  and o.status!= 'VALID' and (o.owner like 'PR%' or o.owner like 'MSSQL%') and o.owner not like 'PRISTAV%' and (o.object_type like 'PAC%' or o.object_type like 'TRIG%' or o.object_type like 'TYPE%'/* or o.object_type like 'SYN%'*/)-- and o.object_name not in ('PCK_DATAFLUX','pck_migr_phase1','PCK_AGGREGATE_CALC','PCK_LEGAL_NOTIFICATION','TMP_CREATE')
--  and REGEXP_LIKE (o.owner, '^prreport', 'i')
--  and o.status!= 'VALID'
--  and o.edition_name like '%.7%' and o.edition_name!= 'RELEASE_03.77.00'
--  and not REGEXP_LIKE (o.owner, '^sys', 'i')
--  and REGEXP_LIKE (o.object_type, 'BMW', 'i')
--  and REGEXP_LIKE(o.object_type, '^table$|view|syn', 'i')
--  and not REGEXP_LIKE(o.object_name, 'PCK_DATAFLUX|pck_migr_phase1|PCK_AGGREGATE_CALC|PCK_LEGAL_NOTIFICATION|TMP_CREATE', 'i')
--  and REGEXP_LIKE(o.object_name, 'VW_REPORT_TOYOTA', 'i')
--  and o.last_ddl_time >= trunc(sysdate) - 60
--  and o.object_id = 1774565
--  and object_name in ('VW_PHONES_CALL_ROUT_TO_PKB_DWH')
;

alter session set edition = "RELEASE_03.79.00";
--Список пользователей по которым отрабатывает тригер на перевод в редакцию
select * from prcore.user_edition_prm ue where 1=1 and REGEXP_LIKE (mask, 'migr', 'i') order by ue.id_user_edition_prm desc;

update prcore.user_edition_prm ue set ue.db_edition = 'RELEASE_03.78.00' where 1=1 and ue.id_user_edition_prm = 213057;


select * from prcore.user_edition_prm ue where 1=1 and  ue.db_edition = 'RELEASE_03.74.00' order by ue.id_user_edition_prm desc;

--update prcore.user_edition_prm t set t.db_edition = 'RELEASE_03.71.00' where 1=1 and REGEXP_LIKE (mask, 'geocoder', 'i');

select * from dba_editions ed where 1=1 and ed.edition_name like 'REL%' order by 1 desc;

select * from dba_objects_ae oe where 1=1 and REGEXP_LIKE(oe.object_name, '^pck_act_result$', 'i') and oe.edition_name like '%RELEASE_03.6%';

select * from table(prdba.pck_dba_toolbelt.f_get_ddl);
select prdba.pck_dba_toolbelt.f_concat('select s.text from dba_source_ae  s where 1=1 and s.owner = ''PRLOAD'' and s.name = ''PCK_SOFT_HANDLE_FILE'' and edition_name like ''%75%''', 'text') as txt from dual;

SELECT PROPERTY_VALUE FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = 'DEFAULT_EDITION';
SELECT   SYS_CONTEXT ( 'userenv', 'AUTHENTICATION_TYPE' ) authent, SYS_CONTEXT ( 'userenv', 'CURRENT_SCHEMA' )      curr_schema, SYS_CONTEXT ( 'userenv', 'SID' )                 SID, SYS_CONTEXT ( 'userenv', 'CURRENT_USER' )        curr_user, SYS_CONTEXT ( 'userenv', 'DB_NAME' )             db_name, SYS_CONTEXT ( 'userenv', 'DB_DOMAIN' )           db_domain, SYS_CONTEXT ( 'userenv', 'HOST' )                host, SYS_CONTEXT ( 'userenv', 'IP_ADDRESS' )          ip_address, SYS_CONTEXT ( 'userenv', 'OS_USER' )             os_user, SYS_CONTEXT ( 'userenv', 'CURRENT_EDITION_NAME' ) cur_edition, SYS_CONTEXT ( 'userenv', 'SESSION_EDITION_NAME' ) ses_edition FROM dual;

select * from dba_views_ae v where 1=1 and v.view_name = 'vw_user_edition' and v.edition_name like '%75%';

select * from dba_views v where 1=1 and REGEXP_LIKE(v.view_name, 'edition', 'i') ;

select ds.owner||'.'||ds.name||';' as pck, ds.* from dba_source ds where 1=1 and ds.owner = 'PRCORE' and ds.name = 'API_PAYMENT'; 
select * from dba_triggers t where REGEXP_LIKE (t.trigger_name, 'TG_PAYMENT_H', 'i') ;

select 'select * from '||d.table_name||';' as sel, d.* from dict d where 1=1 and REGEXP_LIKE(d.table_name, 'view', 'i');

select * from prjobs.cleaning_tables ct, prjobs.cleaning_tab_columns ctc where 1=1 and ct.cleaning_table_id = ctc.cleaning_table_id and REGEXP_LIKE(ct.table_name, 'stor', 'i');

--просмотр степеней параллелизма
--select t.owner||'.'||t.table_name as tab, t.degree, t.*
select trunc(t.last_analyzed) as dt, t.owner, count(*) as cnt
from dba_tables t 
where 1=1
  and REGEXP_LIKE(t.owner, '^pr', 'i')
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
  and REGEXP_LIKE(t.table_name, 'payment', 'i') 
group by trunc(t.last_analyzed), t.owner
  ;


select *
from dba_tables t 
where 1=1
--  and REGEXP_LIKE(t.owner, '^mssql', 'i')
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
  and REGEXP_LIKE(t.table_name, 'bal', 'i')   
;  
select t.owner, count(*) as cnt
from dba_tables t 
where 1=1
  and REGEXP_LIKE(t.owner, '^pr', 'i')
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
--  and REGEXP_LIKE(t.table_name, 'RTD_INSIDE', 'i') 
group by t.owner
  ;  



select substr(t.table_name, 1, 4) as tbl, count(*) as cnt, count(*)/9050*100 as prcnt
from dba_tables t 
where 1=1
  and REGEXP_LIKE(t.owner, '^prcoll', 'i')
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
--  and REGEXP_LIKE(t.table_name, 'RTD_INSIDE', 'i') 
group by substr(t.table_name, 1, 4)
  ;  
  
select trunc(o.created), count(*) as cnt
from dba_objects o
where 1=1
  and REGEXP_LIKE(o.owner, '^prcoll', 'i')
  and REGEXP_LIKE(o.object_type, 'tab', 'i') 
  and substr(o.object_name, 1, 3) in ('TMP')
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
--  and REGEXP_LIKE(t.table_name, 'RTD_INSIDE', 'i') 
group by trunc(o.created)
  ;    

select t.table_owner||'.'||t.table_name as tab, t.owner||'.'||t.index_name as indx, t.degree, t.status
,'alter index '||t.owner||'.'||t.index_name||' NOPARALLEL;' as ddl_stm
from dba_indexes t 
where 1=1 
  and t.status not in ('VALID', 'N/A')
--  and REGEXP_LIKE(t.owner, '^pr', 'i') 
--  and (to_number(REGEXP_replace(t.degree, '\D', '')  ) > 1 or t.degree = 'DEFAULT')
--  and REGEXP_LIKE(t.table_name, 'zd_motivation|ent|duty_allow|^debt$|participant|part_info|debt_info|identity_doc|union_part_info|physical|^hist$', 'i') 
--  and REGEXP_LIKE(t.table_name, 'RTD_INSIDE', 'i') 
  ;



--Поиск зависимостей
select 
--отношение - "используется в объектах"
d.referenced_owner||'.'||d.referenced_name as obj, d.referenced_type as obj_type, d.owner||'.'||d.name as used_in, d.type as used_in_type--, d.*
--отношение - "используют объекты"
--d.owner||'.'||d.name as obj, d.type as obj_type, d.referenced_owner||'.'||d.referenced_name as used_in_obj, d.referenced_type as used_obj_type-- d
--отношение - использует объекты
--distinct d.owner||'.'||d.name as obj, d.referenced_owner||'.'||d.referenced_name as used_obj, d.referenced_type as used_obj_type-- d.*
from dba_dependencies d 
where 1=1 
--  and REGEXP_LIKE(d.referenced_name, 'SNP_MTXT', 'i')
--  and REGEXP_LIKE(d.type, 'pack|func|proc|mater|type|trig', 'i') 
--  and not REGEXP_LIKE(d.owner, '^pristav|sys|public', 'i') 
--  and not REGEXP_LIKE(d.referenced_owner, '^pristav|sys|public', 'i') 
  and REGEXP_LIKE(d.name, '^UK_OLD_ALG', 'i') 
  and d.referenced_type != 'NON-EXISTENT'  
--  and d.type = 'MATERIALIZED VIEW'
order by d.referenced_owner||'.'||d.referenced_name
;

select * from dba_directories d where 1=1 and REGEXP_LIKE (d.directory_name, 'EKVIFAX', 'i') 

select 'ALTER INDEX '||i.index_owner||'.'||i.index_name||' REBUILD PARTITION '||i.partition_name||';' as rebuild_stm,
'EXEC DBMS_STATS.gather_index_stats('''||i.index_owner||''','''||i.index_name||''');' as stat_stm,
 i.* from DBA_IND_PARTITIONS i
where 1=1
  and i.status= 'UNUSABLE'
--  and REGEXP_LIKE (i.index_name, 'ENT_ADD_ATTR_VAL', 'i')
;


drop public database link    NRS_STNB;

CREATE public DATABASE LINK nrs_stnb
CONNECT TO prcore IDENTIFIED BY "1"
USING '10.1.3.71:1531/nrs_stnb1_DGMGRL';

drop database link dmdb;

CREATE DATABASE LINK nrs_test_2
CONNECT TO prod_upd IDENTIFIED BY "manager*123"
USING '(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = priserv082.pristav.int)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SID = nrs_test)
    )
  )';

select * from dba_db_links d where 1=1 and REGEXP_LIKE (d.host, 'NRS_', 'i') and REGEXP_LIKE(owner, '19592', 'i') ;

select * from all_tables@altitude where table_name like '%%';

select 'drop '||case d.owner when 'PUBLIC' then ' public database link '||d.db_link else ' database link '||d.db_link end||';' as stm, d.* from dba_db_links d where 1=1 and REGEXP_LIKE (d.host, '82|test', 'i') and REGEXP_LIKE (username, '.*', 'i') ;


--Генерация запросов для констрейнтов
select 'select * from '||o.owner||'.'||o.object_name||' t where t.'||o.object_name||'.'||c.COLUMN_NAME||' in ();'||chr(10) as dml_sel
--,'insert into '||o.owner||'.'||o.object_name||' select t1.* from '||o.owner||'.'||o.object_name||' t1 where t1.'||c.COLUMN_NAME||' in ();'||chr(10) as dml_ins
--,'delete from from '||o.owner||'.'||o.object_name||' t where t.'||o.object_name||'.'||c.COLUMN_NAME||' in ();'||chr(10) as dml_del
--,'truncate table '||o.owner||'.'||substr(o.object_name, 6)||';' as ddl_tr
--,'ALTER TABLE '||o.owner||'.'||substr(o.object_name, 6)||' ADD CONSTRAINT '||cc.constraint_name||' FOREIGN KEY ('||ccl.l_col||') REFERENCES '||o.owner||'.'||cr.table_name||' ('||ccr.r_col||') ON DELETE CASCADE ENABLE;' as ddl_pk
--,'ALTER TABLE '||o.owner||'.'||substr(o.object_name, 6)||' drop constraint '||cc.constraint_name||';' as ddl_drop_cons
,'ALTER TABLE '||o.owner||'.'||o.object_name||' ADD CONSTRAINT '||cc.constraint_name||' FOREIGN KEY ('||ccl.l_col||') REFERENCES '||o.owner||'.'||cr.table_name||' ('||ccr.r_col||') ON DELETE CASCADE ENABLE;' as ddl_fk
,o.*
from dba_objects o
left outer join dba_tab_columns c on o.owner = c.OWNER and o.object_name = c.TABLE_NAME and c.COLUMN_ID = 1
left outer join dba_constraints cc on o.owner = cc.owner and o.object_name = cc.table_name and cc.constraint_type = 'R'
left outer join 
(
  select distinct listagg('"'||cc.column_name||'"', ', ') within group (order by cc.position)  as l_col
         ,cc.constraint_name
         ,cc.owner
  from dba_cons_columns cc
  group by cc.owner, cc.constraint_name  
) ccl on cc.owner = ccl.owner and cc.constraint_name = ccl.constraint_name
left outer join dba_constraints cr on o.owner = cr.owner and cc.r_constraint_name = cr.constraint_name-- and cc.constraint_type = 'P'
left outer join 
(
  select distinct listagg('"'||cc.column_name||'"', ', ') within group (order by cc.position)  as r_col
         ,cc.constraint_name
         ,cc.owner
  from dba_cons_columns cc
  group by cc.owner, cc.constraint_name  
)   ccr on cr.owner = ccr.owner and cr.constraint_name = ccr.constraint_name
where 1=1
--  and REGEXP_LIKE (o.object_name, '^arch_snp_s(c|t)e(n|p)|^arch', 'i') 
--  and REGEXP_LIKE (o.object_name, 'pop', 'i') 
--  and o.object_type = 'TABLE'
--  and o.owner = 'G11_ODI_WORK001'
  and o.object_name = 'MIGR_CACHE'
--  and REGEXP_LIKE(cc.constraint_name, 'UK_OLD_ALG', 'i') 
--  and not REGEXP_LIKE (o.object_name, 'pop', 'i') 
;

select tc.OWNER||'.'||tc.TABLE_NAME as tab, 'create index '||tc.OWNER||'.'||tc.TABLE_NAME||'_idx'||tc.COLUMN_ID||' on '||tc.OWNER||'.'||tc.TABLE_NAME ||'('||tc.COLUMN_NAME||');' as stmnt
,tc.*
from dba_tab_columns tc where 1=1 and tc.COLUMN_NAME in ('ID_USER_SETTINGS');



--Columns list, coma separated
select listagg(tc.COLUMN_NAME, ',') within group(order by tc.COLUMN_ID) as col_name, listagg(case tc.DATA_TYPE when 'VARCHAR2' then 'VARCHAR' when 'NUMBER' then 'NUMERIC' when 'DATE' then 'DATETIME' end, ',') within group(order by tc.COLUMN_ID) as data_t from dba_tab_columns tc where 1=1 and tc.OWNER = 'PRREPORT' and tc.TABLE_NAME = 'VW_REPORT_ROSBANK_AR' group by tc.TABLE_NAME;

select * from dba_constraints cc where 1=1 and REGEXP_LIKE(cc.constraint_name, '^UK_OLD_ALG$', 'i') order by 1;

--select '|'||pr.grantee||'|' as "||pr.grantee||", '|'||pr.owner||'|' as "||pr.owner||", '|'||pr.table_name||'|' as "||pr.table_name||", '|'||pr.privilege||'|' as "||pr.privilege||"
select 'grant ' ||pr.privilege||' on '||pr.owner||'.'||pr.table_name||' to &user;' as gr_stm
       ,'grant ' ||pr.privilege||' on '||pr.owner||'.'||pr.table_name||' to '||pr.grantee||';' as gr_stm2
       ,'revoke ' ||pr.privilege||' on '||pr.owner||'.'||pr.table_name||' from '||pr.grantee||';' as rev_stm
        , pr.*
from dba_tab_privs pr
where 1=1
  and REGEXP_LIKE(pr.grantee, 'supp', 'i')
--  and  REGEXP_LIKE(pr.owner, '^prdba', 'i')
--  and (select distinct status from dba_objects o where 1=1 and pr.owner = o.owner and pr.table_name = o.object_name) = 'VALID'
--  and REGEXP_LIKE (pr.privilege, 'any', 'i')
--  and REGEXP_LIKE (pr.table_name, 'TMP_APEX_FILE_STORAGE', 'i')
--  and table_name not in ('API_ACTION_LIST', 'API_ACTION_LIST_INFO', 'API_ACTION_PRODUCT_PARAM', 'API_ADDRESS_REL', 'API_BALANCE_HIST', 'API_DEBT', 'API_DEBTOR_INFO', 'API_DEBT_INFO', 'API_ENT', 'API_ENT_ACTUAL_DATA', 'API_ENT_ADD_ATTR_VAL', 'API_ENT_PROC_TASK', 'API_ENT_ROW_LOCK', 'API_OWN_ACCOUNT', 'API_OWN_JOB', 'API_OWN_PROPERTY', 'API_OWN_TRANSPORT', 'API_PARTICIPANT', 'API_PART_INFO', 'API_PAYMENT', 'API_PT_DEBT', 'API_PT_DEBT_ACTION', 'API_PT_INFO', 'API_RECOVERY_TASK', 'API_RECOVERY_TASK_HIST', 'API_RECOVERY_TASK_LINK', 'API_TODO', 'A_RES', 'A_TYPES', 'PCK_APP_ERROR', 'PCK_EX_SQL', 'PCK_HC_TASK_OLD', 'PCK_PART_INFO', 'PCK_RTD_UTIL', 'PCK_SESS')
;-- привилегии пользователя на объекты

grant insert on prcore.tmp_ldap_users to gr_devsup 

select * 
from dba_users u 
where 1=1 and REGEXP_LIKE (u.username, 'v7271', 'i');

--select '|'||pr.grantee||'|' as "||pr.grantee||", '|'||pr.privilege||'|' as "||pr.privilege||"
select 'grant ' ||pr.privilege||' to &user;' as gr_stm, 'revoke ' ||pr.privilege||' from "'||grantee||'";' as gr_stm
,'grant ' ||pr.privilege||' to "'||grantee||'";' as gr_stm2
,u.fio, pr.* 
from dba_sys_privs pr
left outer join (select distinct '--'||first_value(emp.descr) over(partition by au.user_name order by emp.sys_dt_ins desc) as fio, au.user_name from prcore.employee emp, prcore.app_user au where emp.id_app_user = au.id_app_user) u on substr(pr.grantee, 1, instr(pr.grantee, '@', 1, 1)-1) = u.user_name
where 1=1
  and REGEXP_LIKE (pr.grantee, '&grantee', 'i')
--  and t REGEXP_LIKE (, '^prgeo$', 'i')
--  and (select distinct status from dba_objects o where 1=1 and pr.owner = o.owner and pr.table_name = o.object_name) = 'VALID'
  and REGEXP_LIKE (pr.privilege, 'any', 'i')
--  and REGEXP_LIKE (pr.privilege, '^sql_list', 'i')
order by grantee
;


--select '|'||r.grantee||'|' as "||r.grantee||", '|'||r.granted_role||'|' as "||r.granted_role||"
select r.*
       ,'grant '||listagg(r.granted_role, ', ') within group (order by r.grantee) over(partition by r.grantee)||' to "&user";' as grant_roles,
       'alter user "'||coalesce('&user', r.grantee)||'" default role '||listagg(r.granted_role, ', ') within group (order by r.grantee) over(partition by r.grantee)||';' as alter_user,
       'revoke '||listagg(r.granted_role, ', ') within group (order by r.grantee) over(partition by r.grantee)||' from "'||r.grantee||'";' as revoke_roles
       ,'grant create procedure to '||r.grantee||';' as grant_sys_priv
from dba_role_privs r
where 1=1
  and  REGEXP_LIKE (r.granted_role, 'gr_dev', 'i')
--  and r.default_role!= 'YES'\
--  and REGEXP_LIKE (r.grantee, '&grantee', 'i')
order by r.grantee
;


select 'grant '||listagg(r.role, ', ') within group (order by 1) over(partition by 1)||' to &user;' as grant_roles,
       'alter user '||'&user default role '||listagg(r.role, ', ') within group (order by r.role) over(partition by 1)||';' as alter_user 
from dba_roles r where 1=1 and REGEXP_LIKE(r.role, 'GR_devs', 'i');

select 'GRANT SELECT ON '||t.owner||'.'||t.table_name||' TO gr_prcore;' as stmnt, t.* from all_tables t where 1=1 and REGEXP_LIKE(t.table_name, 'action_list', 'i');

GRANT SELECT ON PRCORE.ACTION_LIST TO gr_prcore;

--Привилегии вместе с ролями и выданными через роль
select 
c.*
,'grant '||c.direct_granted_priv||' on '||c.DIRECT_GRANTED_OBJ||' to &user;' as grant_priv
from 
(
  select distinct r.grantee, tp.owner||'.'||tp.table_name as direct_granted_obj, tp.privilege as direct_granted_priv, 'direct_priv' as default_role, decode(tp.privilege, 'DELETE', 'tab', 'INSERT', 'tab', 'UPDATE', 'tab', 'SELECT', 'tab', 'EXECUTE', 'proc', 'DEBUG', 'proc', 'none') as obj
  from dba_role_privs r 
  left outer join dba_tab_privs tp on r.grantee = tp.grantee
  where 1=1
  --  and  REGEXP_LIKE (r.granted_role, 'GR_dev', 'i')
  --  and r.default_role!= 'YES'
    and REGEXP_LIKE (r.grantee, '&grantee', 'i')
  --order by r.grantee asc, r.granted_role, tp.table_name  
  union
  select distinct r.granted_role, tp1.owner||'.'||tp1.table_name as leg_granted_obj, tp1.privilege as direct_granted_priv, r.default_role, decode(tp1.privilege, 'DELETE', 'tab', 'INSERT', 'tab', 'UPDATE', 'tab', 'SELECT', 'tab', 'EXECUTE', 'proc', 'DEBUG', 'proc', 'none') as obj
  from dba_role_privs r 
  left outer join dba_tab_privs tp1 on r.granted_role = tp1.grantee
  where 1=1
  --  and  REGEXP_LIKE (r.granted_role, 'GR_dev', 'i')
  --  and r.default_role!= 'YES'
    and REGEXP_LIKE (r.grantee, '&grantee', 'i')
  --order by r.granted_role, tp.table_name  
  union
  select distinct r.grantee, sp.grantee as direct_granted_obj, sp.privilege as direct_granted_priv, 'direct_priv' as default_role, decode(sp.privilege, 'DELETE', 'tab', 'INSERT', 'tab', 'UPDATE', 'tab', 'SELECT', 'tab', 'EXECUTE', 'proc', 'DEBUG', 'proc', 'none') as obj
  from dba_role_privs r 
  left outer join dba_sys_privs sp on r.grantee = sp.grantee
  where 1=1
  --  and  REGEXP_LIKE (r.granted_role, 'GR_dev', 'i')
  --  and r.default_role!= 'YES'
    and REGEXP_LIKE (r.grantee, '&grantee', 'i')
  --order by r.grantee asc, r.granted_role, tp.table_name  
  union
  select distinct r.granted_role, sp1.grantee as direct_granted_obj, sp1.privilege as direct_granted_priv, r.default_role, decode(sp1.privilege, 'DELETE', 'tab', 'INSERT', 'tab', 'UPDATE', 'tab', 'SELECT', 'tab', 'EXECUTE', 'proc', 'DEBUG', 'proc', 'none') as obj
  from dba_role_privs r 
  left outer join dba_sys_privs sp1 on r.granted_role = sp1.grantee
  where 1=1
  --  and  REGEXP_LIKE (r.granted_role, 'GR_dev', 'i')
  --  and r.default_role!= 'YES'
    and REGEXP_LIKE (r.grantee, '&grantee', 'i')
  order by 1, 2
) c
where 1=1 
  and REGEXP_LIKE(direct_granted_obj, 'any_dict', 'i') 
  --and not (REGEXP_LIKE (DIRECT_GRANTED_OBJ, 'TMP_LDAP|BIN\$|TMP_RTD|identity_doc|comms|address|address_rel|participant_link|message|legal|legal_requisite|own_transport|own_job|own_property|own_account|own_quest|own_children|part_info|physical|ent_add_attr_val|participant|fssp_data|rtd_inside|superdebt_person|part_info_to_link|idq_person_details_view|ent_attach_rel|ent_attach', 'i') and obj = 'tab')
--  and DIRECT_GRANTED_PRIV not in ('DELETE', 'INSERT', 'UPDATE', 'DEBUG')
  and DIRECT_GRANTED_OBJ not like ('SYS.%')
;


--Скопировать привилегии и роли с другой схемы
with c as
(
  select 1 as pp
         , 'grant '||pr.privilege||' on '||pr.owner||'.'||pr.table_name||' to &user'||case pr.grantable when 'YES' then ' with grant option;' else ';' end as ddl_stmnt
         , pr.grantee
         , pr.owner||'.'||pr.table_name as obj
  from dba_tab_privs pr
  where 1=1
    and REGEXP_LIKE(pr.grantee, '&grantee$', 'i')
    and pr.table_name not like 'BIN%' and pr.table_name not like 'TMP_%' and pr.owner!= 'SYS'
--    and not exists(select 1 from dba_tab_privs pr1 where 1=1 and pr1.grantee in ('GR_PRAPEX', 'GR_PRCOLL', 'GR_PRCORE', 'GR_PRGEO', 'GR_PRLEGAL', 'GR_PRLOAD', 'GR_PRREPORT', 'GR_PRTRAN', 'GR_TBL_PRAPEX', 'GR_TBL_PRCOLL', 'GR_TBL_PRCORE', 'GR_TBL_PRREPORT', 'GR_TBL_PRTRAN') and pr1.table_name = pr.table_name and pr1.privilege = pr.privilege)
    and pr.privilege not in ('REFERENCES')  
  union
  select 2 as pp
         , 'grant '||r.GRANTED_ROLE||' to &user'||case r.ADMIN_OPTION when 'YES' then ' with grant option;' else ';' end as ddl_stmnt
         , r.grantee
         , '' as obj
  from dba_role_privs r
  where 1=1
    and REGEXP_LIKE(r.GRANTEE, '&grantee$', 'i')
  union
  select 3 as pp
         , 'grant '||s.privilege||' to &user'||case s.ADMIN_OPTION when 'YES' then ' with admin option;' else ';' end as ddl_stmnt
         , s.grantee
         , '' as obj
  from dba_sys_privs s
  where 1=1
    and REGEXP_LIKE(s.GRANTEE, '&grantee$', 'i')
  union
  select 4 as pp
         , 'alter user &user default role '||listagg(r.granted_role, ', ') within group (order by r.grantee) over(partition by r.grantee)||';' as ddl_stmnt
         , r.grantee           
         , '' as obj
  from dba_role_privs r
  where 1=1
    and REGEXP_LIKE (r.grantee, '^&grantee$', 'i')    
  order by 1      
)
select c.*, wm_concat(obj) over (partition by 1) from c where 1=1 and pp in (1)
  
;

--связь объекта с файлом данных
select a.segment_name,a.file_id,b.file_name Datafile_name from dba_extents a, dba_data_files b where a.file_id=b.file_id and a.segment_name='SMS_DISPATCH_ALL';

--ALTER USER "PROD_UPD" IDENTIFIED BY ""
--ALTER SYSTEM SET resource_manager_plan='FORCE:INTERNAL_PLAN' SCOPE=BOTH;
--ALTER SYSTEM SET resource_manager_plan='STNB_PLAN' SCOPE=BOTH;

select * from  V$RSRC_PLAN_HISTORY order by start_time desc;

select * 
from dba_rsrc_consumer_group_privs rs
where 1=1
  and REGEXP_LIKE (rs.granted_group, 'lim', 'i')
;


select * from V$parameter where REGEXP_LIKE (name, 'size', 'i')
;--параметры инициализации

--Просмотр дожобов
select 'DBMS_SCHEDULER.disable('''||j.owner||'.'||j.job_name||''');' as ddl_dis,
       'DBMS_SCHEDULER.enable('''||j.owner||'.'||j.job_name||''');' as ddl_eneb
       , 'DBMS_SCHEDULER.CREATE_JOB(job_name =>'''||j.owner||'.'||j.job_name||''', job_type => '''||j.job_type||''',job_action => '''||j.job_action||''', start_date => '''||j.start_date||''',repeat_interval => '''||j.repeat_interval||''',end_date => '''||j.end_date||''', job_class =>'''||j.job_class||''',auto_drop => FALSE );' as ddl2
       , j.owner, j.job_name, j.job_style, j.job_type, j.job_action, j.schedule_type, j.repeat_interval, j.state, j.last_start_date, j.logging_level, j.run_count
       , j.job_action, j.job_class, j.*
from dba_scheduler_jobs j
where 1=1
--  and REGEXP_LIKE (j.state, 'run', 'i')
--  and  REGEXP_LIKE (j.job_name , 'HC_POST_ACT_RESULT', 'i')
  and REGEXP_LIKE (j.job_action, 'PCK_SYNERGY', 'i')
--  and REGEXP_LIKE (j.owner, 'SKIP_AREA', 'i')
--  and j.schedule_type not in ('IMMEDIATE', 'ONCE') 
--  and REGEXP_LIKE (j.last_start_date, '31-aug', 'i')
;

select * from dba_errors e where 1=1 and REGEXP_LIKE (e.name, 'PL.*SQL', 'i') ;
select * from dba_scheduler_running_jobs;

select * from dba_jobs_running jr order by jr.THIS_DATE desc;

select * from dba_jobs;

select to_char( j.log_date, 'DD.MM.YYYY HH24:MI:SS') as dt_log, to_char( j.actual_start_date, 'DD.MM.YYYY HH24:MI:SS') as dt_st,  j.*
from dba_scheduler_job_run_details j
where 1=1
  and j.log_date > trunc(sysdate)
--  and not REGEXP_LIKE (j.status, 'SUCCEEDED', 'i') linux 64 bit max memory per process
--  and REGEXP_LIKE (j.additional_info, 'ORA-06512', 'i') 
  and  REGEXP_LIKE (j.job_name , '673763181', 'i')
--  and REGEXP_LIKE (j.job_action, 'PCK_ACT_RESULT_ROBOT', 'i')
--  and REGEXP_LIKE (j.owner, 'prcore', 'i')
--  and REGEXP_LIKE (j.last_start_date, '31-aug', 'i')
order by j.log_date desc
;



--просмотр джобов и логов
select * from dba_scheduler_job_log dj 
where 1=1 
  and dj.log_date > trunc(sysdate)
--  and dj.job_class in ('JC_PCK_DELIVERY', 'JC_PCK_HC_PRINT')
--  and dj.status is null
--  and dj.status not in ('SUCCEEDED')
--  and REGEXP_LIKE (dj.status, 'f', 'i')
--  and REGEXP_LIKE (dj.owner, 'mssql', 'i')
--  and REGEXP_LIKE (dj.log_date, '31-jul-18 08', 'i')
  and REGEXP_LIKE (dj.job_name, 'PQH_TP88_LS552145797_LM09G', 'i')
--  and dj.log_id = 384839
order by 2 desc;

select dj.job_class, count(*)  from dba_scheduler_job_log dj 
where 1=1 
  and dj.log_date > trunc(sysdate)
--  and dj.status is null
--  and dj.status not in ('SUCCEEDED')
--  and REGEXP_LIKE (dj.status, 'f', 'i')
  and not REGEXP_LIKE (dj.owner, 'sys', 'i')
--  and REGEXP_LIKE (dj.log_date, '31-jul-18 08', 'i')
--  and REGEXP_LIKE (dj.job_name, 'JOB_REFRESH_ACTION_PARAM', 'i')
--  and dj.log_id = 384839
group by dj.job_class
order by 2 desc;

select (3169955784*0.8)*0.2/1024/1024 from dual
select *
from dba_scheduler_job_run_details j
where 1=1
--  and REGEXP_LIKE (j.state, 'run', 'i') 
  and  REGEXP_LIKE (j.job_name , 'JOB_HC_NOTIFICATION_STATUS_IP', 'i')
--  and REGEXP_LIKE (j.job_action, 'J_SEND_UPCOMING_CALL_LETTER', 'i')
--  and REGEXP_LIKE (j.owner, 'prcore', 'i')
--  and REGEXP_LIKE (j.last_start_date, '31-aug', 'i')
order by 1 desc
;

--просмотр джобов и логов
select dj.status, count(*)  from dba_scheduler_job_log dj 
where 1=1 
group by dj.status
order by 2 desc;



--begin dbms_scheduler.drop_job(job_name => 'PQH_TP50_LS29898470_O5WHR', force => true); end;

/*BEGIN
  DBMS_SCHEDULER.DROP_JOB(job_name => 'REPORT_AREA.LEGAL_CRYSTAL_J, REPORT_AREA.REPORT_FIELD_LEGAL_J' ,force => true );
END;*/


select t.trigger_body, t.* from dba_triggers t where 1=1 and t.base_object_type = 'DATABASE        ' and t.owner not like 'APEX%' and owner not in ('PRCORE', 'SYSMAN', 'PRCOLL', 'MDSYS', 'MSSQL_MIGR', 'OLAPSYS', 'XDB', 'PRLEGAL', 'FMW_BIPLATFORM');
select t.owner, count(*)  from dba_triggers t where 1=1 and t.owner not like 'APEX%' and owner not in ('PRCORE', 'SYSMAN', 'PRCOLL', 'MDSYS', 'MSSQL_MIGR', 'OLAPSYS', 'XDB', 'PRLEGAL', 'FMW_BIPLATFORM')  group by t.owner;

select * from dba_source ds
where REGEXP_LIKE (ds.text, 'ddl_events', 'i') ;

select * from dba_triggers t where 1=1 and  lower(prdba.pck_dba_toolbelt.f_getlong('')) like lower('%ddl_events%');
select * from dba_source ds where lower(ds.text) like lower('%ddl_events%') ;

--Просмотр директорий
select dr.*, 'CREATE OR REPLACE DIRECTORY "'||dr.directory_name||'" AS ''/u02/FRA/dpdump'';' from dba_directories dr 
where 1=1 
--  and REGEXP_LIKE (dr.directory_path, 'ekvifax', 'i') 
  and REGEXP_LIKE (dr.directory_name, 'fax', 'i') 
;

CREATE OR REPLACE DIRECTORY "EKVIFAXRD" AS '/mnt/priserv008/RD/';

--Таблица хранит историю DDL, заполняется тригером
select * from prcore.ddl_events cc 
where 1=1
--  and REGEXP_LIKE(cc.oradictobjname, 'hist', 'i') 
order by 1 desc;

select * from prcore.ddl_events_sql cc 
where 1=1
--  and REGEXP_LIKE (cc.oradictobjname, 'anal', 'i')
order by 1 desc;


--История релизов
select * from prod_upd.migr# order by dt_start desc;

--Аудит
select d.os_username, d.username, d.userhost, d.timestamp, d.owner, d.obj_name, d.action, d.action_name, d.returncode, d.*
--distinct 'select count(*) from '||d.owner||'.'||d.obj_name||';' 
from dba_audit_trail d
where 1=1
--  and REGEXP_LIKE (username, '^report_area', 'i')
--  and REGEXP_LIKE (d.obj_name, 'add_attr_debt', 'i')
--  and REGEXP_LIKE (d.action_name, 'alter', 'i')
--  and REGEXP_LIKE (d.os_username, 'mssql_migr', 'i') 
--  and d.action = 43
  and d.timestamp > trunc(sysdate)
--  and d.timestamp > sysdate -1
--order by d.timestamp desc
;

select d.*--os_username, d.username, d.userhost, d.timestamp, d.owner, d.obj_name, d.action, d.action_name, d.returncode, d.*
--distinct 'select count(*) from '||d.owner||'.'||d.obj_name||';' 
from dba_audit_object d
where 1=1
--  and REGEXP_LIKE (username, '^report_area', 'i')
--  and REGEXP_LIKE (d.obj_name, 'PCK_RTD_SOURCE', 'i')
--  and REGEXP_LIKE (d.action_name, 'compile', 'i')
--  and REGEXP_LIKE (d.os_username, 'mssql_migr', 'i') 
--  and d.action = 43
  and d.timestamp > trunc(sysdate)
--  and d.timestamp < trunc(sysdate)-1  
--  and d.timestamp > sysdate -1
order by d.timestamp desc
;

select d.EXTENDED_TIMESTAMP, a.name, d.*
--distinct 'select count(*) from '||d.owner||'.'||d.obj_name||';' 
from V$XML_AUDIT_TRAIL d, audit_actions a
where 1=1 and d.action = a.action
--  and REGEXP_LIKE(a.name, 'table', 'i') 
--  and REGEXP_LIKE (d.db_user, 'mssql', 'i') 
--  and REGEXP_LIKE (d.OBJECT_NAME, 'act_result', 'i')
  and d.EXTENDED_TIMESTAMP > trunc(sysdate)
  and REGEXP_LIKE (d.SQL_TEXT, 'compile', 'i')
--  and a.name = 'DROP TABLE' 
order by d.EXTENDED_TIMESTAMP desc
;


select * from V$parameter ps  where REGEXP_LIKE (ps.name, 'undo', 'i');

/*begin
--  DBMS_LOGMNR_D.BUILD(OPTIONS=> DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
  
  DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);
-- dbms_logmnr.start_logmnr;
end;*/

--История планов обслуживания
select * from  V$RSRC_PLAN_HISTORY;

--База данных и бэкапы
select * from v$database v;
select * from  V$RMAN_CONFIGURATION;
select * from v$backup_set order by COMPLETION_TIME desc;
select * from v$backup_archivelog_details;
select vdf.name, bdf.* from  V$BACKUP_DATAFILE bdf
inner join v$datafile vdf on bdf.file# = vdf.file#;
select * from v$flash_recovery_area_usage;

select * from dba_recyclebin where REGEXP_LIKE (owner, 'prcoll', 'i') and REGEXP_LIKE (original_name, 'PT_DEBT_O', 'i');

select to_date(max(ar.sys_dt_ins), 'DD.MM.YYYY HH24:MI:SS') from prcore.act_result ar where ar.sys_dt_ins > sysdate-1
union all
select max(ar.sys_dt_ins) from prcore.act_result@prod_sys ar where ar.sys_dt_ins > sysdate-1;

--This view can be used to find out the current archive gap that is blocking recovery. если пусто то всё ок.
select * from v$archive_gap;

--displays how much redo data generated by the primary database is not yet available on the standby database, showing how much redo data could be lost if the primary database were to crash at the time you queried this view. You can query this view on any instance of a standby database in a Data Guard configuration. If you query this view on a primary database, then the column values are cleared.
select * from V$DATAGUARD_STATS;
--Архив логи
select * from V$ARCHIVE_DEST;
select * from v$archived_log al
where 1=1
--  and al.SEQUENCE# between 375237 and	375551
--  and al.dest_id = 1
--  and al.APPLIED = 'YES'
order by stamp desc;

--размер и использование FRA
select rfd.NAME, rfd.SPACE_LIMIT/1024/1024/1024 as SPACE_LIMIT_GB,rfd.SPACE_USED/1024/1024/1024 as SPACE_USED_GB, rfd.SPACE_RECLAIMABLE/1024/1024/1024 as SPACE_RECLAIMABLE_GB, rfd.NUMBER_OF_FILES from V$RECOVERY_FILE_DEST rfd;

--Статус архивлогов, интересен параметр RECOVERY_MODE при накает в реальном вермени он должен быть MANAGED REAL TIME APPLY(нактывается редулог, не ожидая записи архивлога) 
select * from  V$ARCHIVE_DEST_STATUS;

--Редулоги
select l.BYTES/1024/1024 as mb, l.* from v$log l;
select l.* from v$logmnr_extents l;
select l.* from v$logfile l;
--Состояние процессов, интересен медиарекавери (MRP0)
select process, status, thread#, sequence#, block#, blocks from v$managed_standby;
--Имя базы, инстанса и т.д. CURRENT_SCN system change number ("счётчик коммитов")
select i.HOST_NAME ,name, instance_name, open_mode, database_role, flashback_on , current_scn from v$database db,v$instance i; 

--Посмотреть edition релиза
SELECT sys_context('USERENV', 'CURRENT_EDITION_NAME') FROM DUAL;

select * from V$parameter ps  where REGEXP_LIKE (ps.name, 'back', 'i');


select ps.name, ps.value as stnb, pp.value as prod from V$parameter ps  
inner join v$parameter@prod_sys pp on ps.name = pp.name
where REGEXP_LIKE (ps.name, 'log', 'i')
;--параметры инициализации для сравнения с продом

select * from V$DIAG_INFO
;-- если забыл где алерт логи

--Список ивентов
SELECT 
  eq_name "Enqueue", 
  ev.name "Enqueue Type", 
  eq.req_description "Description"
FROM v$enqueue_statistics eq, v$event_name ev
WHERE eq.event#=ev.event#
  and REGEXP_LIKE (ev.name, 'contention', 'i')
ORDER BY ev.name;

--время выполнения ивентов
SELECT sid, total_waits, time_waited
  FROM v$session_event
 WHERE REGEXP_LIKE (event, 'object', 'i') 
  and total_waits>0
 ORDER BY 3,2
;

SELECT QCSID, SID, INST_ID "Inst", SERVER_GROUP "Group", SERVER_SET "Set",
  NAME "Stat Name", VALUE
FROM GV$PX_SESSTAT A, V$STATNAME B
WHERE A.STATISTIC# = B.STATISTIC# AND NAME LIKE 'PHYSICAL READS'
  AND VALUE > 0 ORDER BY QCSID, QCINST_ID, SERVER_GROUP, SERVER_SET;
  
SELECT 
  a.ksppinm Param , 
  b.ksppstvl SessionVal ,
  c.ksppstvl InstanceVal,
  a.ksppdesc Descr 
FROM 
  x$ksppi a , 
  x$ksppcv b , 
  x$ksppsv c
WHERE 
  a.indx = b.indx AND 
  a.indx = c.indx AND 
  a.ksppinm LIKE '/_%' escape '/'
--and a.ksppinm like '_optimizer%'
ORDER BY
1;
  
--корзина
select * from dba_recyclebin dr
where 1=1
--  and REGEXP_LIKE (dr.type, 'view', 'i') 
  and  REGEXP_LIKE (dr.owner, 'prcoll', 'i') 
order by droptime desc
;
/* Determine which latch is causing the highest amount of contention.

To find the problem latches since database startup, run the following query:*/

SELECT n.name, l.sleeps
  FROM v$latch l, v$latchname n 
  WHERE n.latch#=l.latch# and l.sleeps > 0 order by l.sleeps desc
;

--To see latches that are currently a problem on the database run:

SELECT n.name, SUM(w.p3) Sleeps
  FROM V$SESSION_WAIT w, V$LATCHNAME n
 WHERE w.event = 'log file sync'
   AND w.p2 = n.latch#
 GROUP BY n.name;

--Take action based on the latch with the highest number of sleeps.

--Типа нагрузка на диски, но хер его знает...
SELECT Disk_Reads DiskReads, Executions, SQL_ID, SQL_Text SQLText, 
   SQL_FullText SQLFullText 
FROM
(
   SELECT Disk_Reads, Executions, SQL_ID, LTRIM(SQL_Text) SQL_Text, 
      SQL_FullText, Operation, Options, 
      Row_Number() OVER 
         (Partition By sql_text ORDER BY Disk_Reads * Executions DESC) 
         KeepHighSQL
   FROM
   (
       SELECT Avg(Disk_Reads) OVER (Partition By sql_text) Disk_Reads, 
          Max(Executions) OVER (Partition By sql_text) Executions, 
          t.SQL_ID, sql_text, sql_fulltext, p.operation,p.options
       FROM v$sql t, v$sql_plan p
       WHERE t.hash_value=p.hash_value AND p.operation='TABLE ACCESS' 
       AND p.options='FULL' AND p.object_owner NOT IN ('SYS','SYSTEM')
       AND t.Executions > 1
   ) 
   ORDER BY DISK_READS * EXECUTIONS DESC
)
WHERE KeepHighSQL = 1
AND rownum <=5;

--Топ 20 объектов по которым в данный момент выполняется full table scans or index fast full scans
SELECT * FROM
(SELECT SUBSTR(O.OWNER, 1, 15) OWNER,
         SUBSTR(O.OBJECT_NAME, 1, 35) OBJECT,
         COUNT(*) BLOCKS,
         DECODE(O.OBJECT_TYPE, 'TABLE', 'FULL TABLE SCAN',
                               'INDEX', 'FAST FULL SCAN',
                               'OTHER') "SCAN TYPE"
FROM DBA_OBJECTS O, X$BH B
WHERE B.OBJ = O.DATA_OBJECT_ID AND
STANDARD.BITAND(B.FLAG, 524288) > 0 AND
O.OWNER != 'SYS'
GROUP BY O.OWNER, O.OBJECT_NAME, O.OBJECT_TYPE
ORDER BY COUNT(*) DESC)
WHERE ROWNUM <=20;  

--Топ 20 объектов по количеству чтений в контексте full table scans and index fast full scans
SELECT * FROM
(SELECT SUBSTR(SA.SQL_TEXT, 1, 68) SQL_TEXT,
         SA.DISK_READS DISK_READS
FROM V$SQLAREA SA WHERE
(SA.ADDRESS, SA.HASH_VALUE) IN
(SELECT ADDRESS, HASH_VALUE FROM V$SQL_PLAN
WHERE OPERATION = 'TABLE ACCESS' AND
       OPTIONS = 'FULL' OR
       OPERATION = 'INDEX' AND
       OPTIONS LIKE 'FAST FULL%')
ORDER BY 2 DESC)
WHERE ROWNUM <=20;

  
--сгенить скрипты для сбора статистики по таблицам и индексам 
select 'EXEC DBMS_STATS.gather_table_stats('''||t.owner||''','''||t.table_name||''');' from dba_tables t
where 1=1
--  and last_analyzed < sysdate -30 
--  and owner like 'PRCOLL%'
  and table_name in ('ARCHLOGS', 'PT_DEBT', 'CSV_ARCH', 'PROC_QUEUE')
--  and REGEXP_LIKE (t.table_name, 'legal_task', 'i')  
  ;

select 'EXEC DBMS_STATS.gather_index_stats('''||owner||''','''||index_name||''');' as stmt, i.*
from dba_indexes i
where 1=1 
--  and last_analyzed < sysdate -30 
--  and owner like 'PR%'
  and table_name in ('ARCHLOGS', 'PT_DEBT', 'CSV_ARCH', 'PROC_QUEUE')
--  and REGEXP_LIKE (i.table_name, 'PT_DEBT', 'i') 
;


select di.table_name,  di.index_name, dip.partition_name,   

       di.status AS index_status, dip.status AS partition_status 
  from dba_indexes di 
  join dba_ind_partitions dip 
    on dip.index_name = di.index_name 
 where di.status <> 'VALID' 
   and di.owner = 'MSSQL_MIGR' 
   --and di.index_name like '%OI%'
   ;

select distinct 'ALTER INDEX ' || di.owner || '.' || dip.index_name || ' REBUILD PARTITION ' || partition_name as ddl 
  from dba_indexes di 
  join dba_ind_partitions dip 
    on dip.index_name = di.index_name 
 where di.status <> 'VALID' 
   and di.owner = 'PRCOLL' 
   and di.index_name like '%OI%';
   
   select * from dba_objects where last_ddl_time > trunc(sysdate) - 7 and owner like 'PR%' and object_type like 'PACKAGE%'
;


--посмотреть точки востановления
SELECT NAME, SCN, TIME, DATABASE_INCARNATION#, GUARANTEE_FLASHBACK_DATABASE,STORAGE_SIZE/1024/1024/1024 as STORAGE_SIZE_GB FROM V$RESTORE_POINT;

select rfd.NAME, rfd.SPACE_LIMIT/1024/1024/1024 as SPACE_LIMIT_GB,rfd.SPACE_USED/1024/1024/1024 as SPACE_USED_GB, rfd.SPACE_RECLAIMABLE/1024/1024/1024 as SPACE_RECLAIMABLE_GB, rfd.NUMBER_OF_FILES from V$RECOVERY_FILE_DEST rfd;

select * from v$instance;
        
--создать точку востановления
CREATE RESTORE POINT "before_test_point_09-07-2017_09:52" GUARANTEE FLASHBACK DATABASE;

select 'before_test_point_'||to_char(sysdate, 'dd-mm-yyyy_hh24:mi') from dual;

--флэшбэк к точке
FLASHBACK DATABASE TO RESTORE POINT 'before_upgrade';
FLASHBACK DATABASE TO SCN 202381;


--По сехемам (В этом случае будет использоваться DBMS_STATS.AUTO_SAMPLE_SIZE, что позволит ораклу собрать статистику наилучшим образом)
declare
  cursor sql_c is
    select 'DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '''||username||''');' as sql_bl, username from dba_users where username not like 'PRISTAV%'  and (username like 'PR%' or username = 'MSSQL_MIGR');
begin
  dbms_output.put_line('declare'||chr(10)||'  str_v varchar2(4000):= '''';'||chr(10)||'begin');
  for cur in sql_c loop
    dbms_output.put_line('  '||cur.sql_bl);
    dbms_output.put_line('  '||'dbms_output.put_line(''done '||cur.username||' ''||to_char(sysdate, ''DD.MM.YYYY HH24:MI:SS''));');  
    dbms_output.put_line('  str_v:= str_v||case when str_v is not null then chr(10) end||''<TR><TD>''||''done '||cur.username||' ''||to_char(sysdate, ''DD.MM.YYYY HH24:MI:SS'')||chr(10)||''<TR><TD>'';');
  end loop;
  dbms_output.put_line(
                       chr(10)||'  equifax.eq_upload.res_v:= prcore.pck_sess.f_Create_Sess(5);'||chr(10)||'    prcore.pck_email_queue.p_send_email('||chr(10)||'    pr_sender => ''GATHER_SCHEMA_STATS'''||
                       chr(10)||'    ,pr_mail_to => ''<pristav13988@pristav.int>'''||chr(10)||'    ,pr_subject => ''GATHER_SCHEMA_STATS'''||
                       chr(10)||'    ,pr_message_text => ''<TABLE>''||''<TR><TD>''||str_v||''</TD></TR>''||''</TABLE>'''||chr(10)||'   );'
                      );
  dbms_output.put_line(
                       chr(10)||'exception'||chr(10)||'  when others then'||chr(10)||'    DBMS_OUTPUT.PUT_LINE (''Unexpected error ''||SQLCODE||''   ''||SQLERRM);'||
                       chr(10)||'    str_v:= str_v||case when str_v is not null then chr(10) end||''<TR><TD>''||''DBMS_STATS.GATHER_SCHEMA_STATS operation failed! ''||SQLCODE||''   ''||SQLERRM||''   ''||to_char(sysdate, ''DD.MM.YYYY HH24:MI:SS'')||chr(10)||''<TR><TD>'';'||
                       chr(10)||'    equifax.eq_upload.res_v:= prcore.pck_sess.f_Create_Sess(5);'||chr(10)||'    prcore.pck_email_queue.p_send_email('||chr(10)||'      pr_sender => ''GATHER_SCHEMA_STATS'''||
                       chr(10)||'      ,pr_mail_to => ''<pristav13988@pristav.int>'''||chr(10)||'      ,pr_subject => ''GATHER_SCHEMA_STATS'''||
                       chr(10)||'      ,pr_message_text => ''<TABLE>''||''<TR><TD>''||str_v||''</TD></TR>''||''</TABLE>'''||chr(10)||'   );'||chr(10)||'    raise;'
                      );
  dbms_output.put_line(chr(10)||'end;'); 
end;


--Дисконект пачки сессий
declare
  errm_v varchar2(512):= NULL;
  flag_v int:= 0;
  flag2_v int:= 0;
  cursor list_c is
    select distinct 
    coalesce(sb.sid, w.BLOCKER_SID) as b_sid, coalesce(sb.SERIAL#, w.BLOCKER_SESS_SERIAL#) as b_ser
    , 'alter system DISCONNECT SESSION ''' || coalesce(sb.sid, w.BLOCKER_SID) || ',' || coalesce(sb.SERIAL#, w.BLOCKER_SESS_SERIAL#) || ''' immediate' as stmn
    ,(select o.object_name from all_objects o where o.object_id = s.session_edition_id) as edition_name
    ,s.sid, p.SPID, q.SQL_ID, q.SQL_TEXT, s.USERNAME, s.osuser, s.EVENT, s.WAIT_CLASS, s.LOGON_TIME, s.MODULE, s.machine, s.action, s.program, s.status,  pvs.degree, pvs.REQ_DEGREE, trunc((sysdate-s.logon_time)*24*60*60, 2) as dur_sec, s.SQL_EXEC_START, s.PREV_EXEC_START
    from v$session s
    left outer join v$process p on s.pADDR = p.ADDR
    left outer join v$sql q on s.SQL_HASH_VALUE = q.HASH_VALUE
    left outer join V$PX_SESSION pvs on s.SID = pvs.SID
    left outer join v$wait_chains w on s.sid = w.sid
    left outer join v$session sb on s.BLOCKING_SESSION = sb.sid
    where 1=1
    --  and s.SID in (2865)
      and REGEXP_LIKE (s.STATUS, '^active', 'i')
      and s.TYPE = 'USER'
    --  and REGEXP_LIKE(s.event, 'latch|lock', 'i') 
    --  and w.BLOCKER_SID is not null
      and REGEXP_LIKE (s.OSUSER, 'PRISTAV13988', 'i')
      and upper(q.SQL_TEXT) like upper('%compile%')
    order by s.LOGON_TIME asc, s.SQL_EXEC_START desc;
begin
  loop  --для цепочки
    for cur in list_c loop
      begin
        flag_v:= flag_v+1;
--        DBMS_OUTPUT.PUT_LINE(cur.stmn||'           '||flag_v);        
        execute immediate cur.stmn;
        dbms_lock.sleep(1);  --для цепочки
      exception
        when others then
          errm_v:= sqlerrm;
          DBMS_OUTPUT.PUT_LINE ('Messege '|| errm_v); continue;
      end;
    end loop;
    exit when flag_v = 0 or flag2_v > 5; --для цепочки
    flag_v:= 0; --для цепочки
  end loop; --для цепочки
end;

declare
  errm_v varchar2(512):= NULL;
  cursor list_c is
    select 'alter system DISCONNECT SESSION ''' || sid || ',' || serial# || ''' immediate' as sqt--, s.*  --для дисконекта сессий подходящих под условие
    from v$session s
    where 1=1
      and REGEXP_LIKE (s.status, '^ACTIVE', 'i') and not REGEXP_LIKE (s.username, '^pristav13988|prod_upd', 'i') and s.username!= 'SYS' 
      /*and REGEXP_LIKE(s.WAIT_CLASS, 'concur', 'i') and REGEXP_LIKE (s.username, 'apex', 'i') and REGEXP_LIKE (s.osuser, 'pristav14015', 'i')*/
      --and (trunc((sysdate-s.SQL_EXEC_START)*24*60*60, 2) > 3600)
      ;
begin
    for cur in list_c loop
      begin
        flag_v:= flag_v+1;
--        DBMS_OUTPUT.PUT_LINE(cur.sqt||'           '||flag_v);        
        execute immediate cur.sqt;
      exception
        when others then
          errm_v:= sqlerrm;
          DBMS_OUTPUT.PUT_LINE ('Messege '|| errm_v); continue;
      end;
    end loop;
end;

  
-- Предоставить доступ к пачке таблиц
begin
  FOR x IN (SELECT t.TABLE_NAME, t.owner FROM dba_tables t where t.owner = 'SEGM')
  LOOP   
   EXECUTE IMMEDIATE 'GRANT SELECT ON '||x.owner||'.'||x.table_name||' TO pristav13589'; 
  END LOOP;
end;   


declare
  TYPE obj_list IS TABLE OF varchar2(1024);
  obj_list_v obj_list;
  sql_stmnt varchar2(4000);
begin
  obj_list_v := obj_list('prcore.employee','prcore.app_user','prcore.participant','prcoll.add_attr_debt','prcore.legal');
  for i in obj_list_v.first..obj_list_v.last loop
    sql_stmnt:= 'GRANT ALTER, DELETE, INSERT, UPDATE, SELECT ON '||obj_list_v(i)||' TO NODE_JS';
--    EXECUTE IMMEDIATE sql_stmnt;
    DBMS_OUTPUT.PUT_LINE(sql_stmnt);
  end loop;
end; 

--скопировать привилегии с учётки другой учётке, в том числе на объекты исходной учётки
declare
  sql_stmnt varchar2(4000);
  
  cursor c1 is 
    with c as
    (
      select 1 as pp
             , 'grant '||pr.privilege||' on '||pr.owner||'.'||pr.table_name||' to &user'||case pr.grantable when 'YES' then ' with grant option' else '' end as ddl_stmnt
             , pr.grantee
             , pr.owner||'.'||pr.table_name as obj
      from dba_tab_privs pr
      where 1=1
        and REGEXP_LIKE(pr.grantee, '&grantee$', 'i')
        and pr.table_name not like 'BIN%' and pr.table_name not like 'TMP_%' and pr.owner!= 'SYS'
    --    and not exists(select 1 from dba_tab_privs pr1 where 1=1 and pr1.grantee in ('GR_PRAPEX', 'GR_PRCOLL', 'GR_PRCORE', 'GR_PRGEO', 'GR_PRLEGAL', 'GR_PRLOAD', 'GR_PRREPORT', 'GR_PRTRAN', 'GR_TBL_PRAPEX', 'GR_TBL_PRCOLL', 'GR_TBL_PRCORE', 'GR_TBL_PRREPORT', 'GR_TBL_PRTRAN') and pr1.table_name = pr.table_name and pr1.privilege = pr.privilege)
        and pr.privilege not in ('REFERENCES')  
      union
      select 2 as pp
             , 'grant '||r.GRANTED_ROLE||' to &user'||case r.ADMIN_OPTION when 'YES' then ' with grant option' else '' end as ddl_stmnt
             , r.grantee
             , '' as obj
      from dba_role_privs r
      where 1=1
        and REGEXP_LIKE(r.GRANTEE, '&grantee$', 'i')
      union
      select 3 as pp
             , 'grant '||s.privilege||' to &user'||case s.ADMIN_OPTION when 'YES' then ' with admin option' else '' end as ddl_stmnt
             , s.grantee
             , '' as obj
      from dba_sys_privs s
      where 1=1
        and REGEXP_LIKE(s.GRANTEE, '&grantee$', 'i')
      union
      select 4 as pp
             , 'alter user &user default role '||listagg(r.granted_role, ', ') within group (order by r.grantee) over(partition by r.grantee)||'' as ddl_stmnt
             , r.grantee           
             , '' as obj
      from dba_role_privs r
      where 1=1
        and REGEXP_LIKE (r.grantee, '^&grantee$', 'i')    
      order by 1      
    )
    select c.ddl_stmnt from c-- where 1=1 and pp in (1)
    union all
    select 'grant all on '||o.owner||'.'||o.object_name||' to '||'&user with grant option' as ddl_stmnt 
    from dba_objects o
    where 1=1
      and REGEXP_LIKE (o.owner, '^&grantee$', 'i')
      and REGEXP_LIKE(o.object_type, '^table$|view|syn|pac|proc|func', 'i')
    ;
  cursor c2 is
    select u.username as gr from dba_users u where REGEXP_LIKE (u.username, '^prnode$', 'i');
--    select r.role as gr from dba_roles r where REGEXP_LIKE (r.role, 'gr_prcore', 'i');
    
begin
  for cur in c2 loop
    for cur2 in c1 loop
      begin
        sql_stmnt:= cur2.ddl_stmnt;
        EXECUTE IMMEDIATE sql_stmnt;
        DBMS_OUTPUT.PUT_LINE(sql_stmnt);
      exception
        when others then
          DBMS_OUTPUT.PUT_LINE ('Messege '|| sqlerrm); continue;
      end;
    end loop;
  end loop;
end; 

--Компиляция объектов по схемно, в прошлы раз помогло когда не могли перекомпилировать пакеты выборочно
begin
  DBMS_UTILITY.compile_schema(schema => 'PRCOLL');
  DBMS_UTILITY.compile_schema(schema => 'PRCORE');
--  DBMS_UTILITY.compile_schema(schema => 'PRREPORT');
end;  

--Поиск невалидных пакетов каждую минуту и перекомпиляция если найдены
declare
--  stop_time   date:= to_date('06-12-2018 18:00:00','dd-mm-yyyy hh24:mi:ss');--sysdate+1/24/60;--to_date('23-06-2017 20:30:00','dd-mm-yyyy hh24:mi:ss');
  
  cursor invalid_c is
    select distinct
    case
      when o.object_type in ('FUNCTION', 'PROCEDURE', 'TRIGGER', 'VIEW') then 'alter '||o.object_type||' '||o.owner||'.'||o.object_name||' compile'
      when o.object_type in ('PACKAGE', 'PACKAGE BODY') then 'alter package '||o.owner||'.'||o.object_name||' compile '|| case object_type when 'PACKAGE BODY' then 'body' else 'specification' end
    end as compile_stm, o.owner, o.object_name, o.status--, o.*
    from dba_objects o
    where 1=1
      and o.status!= 'VALID' and (o.owner like 'PR%' or o.owner like 'MSSQL%') and o.owner not like 'PRISTAV%' and (o.object_type like 'PAC%' or o.object_type like 'TRIG%' or o.object_type like 'TYPE%'/* or o.object_type like 'SYN%'*/)-- and o.object_name not in ('PCK_DATAFLUX','pck_migr_phase1','PCK_AGGREGATE_CALC','PCK_LEGAL_NOTIFICATION','TMP_CREATE')
--      and REGEXP_LIKE (o.owner, '^pr', 'i')
--      and REGEXP_LIKE (o.object_type, 'pac', 'i')
--      and not REGEXP_LIKE (o.object_name, 'PCK_DATAFLUX|pck_migr_phase1|PCK_AGGREGATE_CALC|PCK_LEGAL_NOTIFICATION', 'i')
--      and not REGEXP_LIKE (o.owner, '^PRDQ$', 'i')
    ;
    
begin
  dbms_output.put_line(to_char(sysdate, 'DD.MM.YYYY HH24:MI:SS'));  
--  while 1=1 loop
    for cur in invalid_c loop
      begin
        execute immediate cur.compile_stm;
        dbms_output.put_line(to_char(sysdate, 'DD.MM.YYYY HH24:MI:SS')||'|   '||cur.compile_stm);            
      exception
        when others then
          DBMS_OUTPUT.PUT_LINE (SQLCODE||': '||SQLERRM); continue;
      end;      
    end loop;
--    dbms_lock.sleep(30);    
--    exit when sysdate >=stop_time;
--  end loop;
end;


begin
  prjobs.pck_clean_utils.p_main(pr_is_send_mail => false);
end;  

