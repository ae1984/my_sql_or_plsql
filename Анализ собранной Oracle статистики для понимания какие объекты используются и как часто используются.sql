/*
    нужно ещё ускорить и автоматизировать
*/
/* *******************************************************************************************************
    схема
*/

-- объекты схемы
drop table T_RDWH_TMP_OBJSCH;
create table T_RDWH_TMP_OBJSCH as
select distinct
  t.OWNER
  ,t.OBJECT_NAME
  ,t.OBJECT_TYPE
  ,t.created
  ,t.last_ddl_time
  --,t.*
from all_objects t
left join all_objects a on    a.OWNER = 'U1' 
                          and t.object_type like '%TABLE%' 
                          and a.OBJECT_NAME = t.OBJECT_NAME
                          and a.OBJECT_TYPE not like '%TABLE%'
where    t.OWNER = 'U1' 
     and t.object_type in ('VIEW','MATERIALIZED VIEW','TABLE')
     and a.OBJECT_NAME is null
     --and t.created >= trunc(sysdate)

	 
	 
-- собираем исходный код всех представлений
drop table T_TMP_RDWH_DDL_MVIEW_VIEW0519;
create table T_TMP_RDWH_DDL_MVIEW_VIEW0519 as
select
   sysdate as DT 
   ,upper(tt.OBJECT_NAME) as OBJECT_NAME
   ,object_type
   ,tt.created
   ,tt.last_ddl_time
   ,dbms_metadata.get_ddl(replace(tt.object_type,' ','_'),tt.OBJECT_NAME) as ddl#
from T_RDWH_TMP_OBJSCH tt
where tt.object_type in ('VIEW','MATERIALIZED VIEW')



-- строим взаимосвязи объектов и исх.кодов
create table T_TMP_RECALC_REF0519 as --46:41
select /*+ parallel(64)*/ t.object_name as obj ,t.ddl#,b.object_name as ref_obj,b.object_type 
from u1.T_TMP_RDWH_DDL_MVIEW_VIEW0519 t
--join u1.T_RDWH_PROC_OBJECT a on upper(a.object_name) = upper(t.object_name)
left join u1.T_TMP_RDWH_DDL_MVIEW_VIEW0519 b on
                              REGEXP_LIKE (upper(t.ddl#), --'(^|[^[\w]]|\s|;|\)|$|\(|\.)'||upper(b.object_name)||'([^[\w]]|\s|;|\)|$|\.)')     
                                                          '(^|[^[\w]]|\s|;|\)|$|\(|"|\.)'||upper(b.object_name)||'([^[\w]]|\s|;|\)|$|"|\.)') -- это магия!!!    ))))                            
                              and b.object_name <> t.object_name



--строим дерево
drop table  T_TMP_TREE_OBJ0519
create table T_TMP_TREE_OBJ0519 as
select /*+ parallel(32)*/ * from (
select --distinct
    level as lvl, lpad(' ',4*(level-1)) || t.obj||' -> '||nvl(lower(t.ref_obj),to_char(CONNECT_BY_ISLEAF))||' ('||lower(t.object_type)||')' as s --, t.obj, t.ref_obj
    ,t.is_use
    --, t.object_type --, t.ddl#
    --,CONNECT_BY_ISCYCLE
    , CONNECT_BY_ISLEAF
    ,SYS_CONNECT_BY_PATH(lower('('||t.obj||')'), ' -> ') as Path#
    ,t.obj
    ,t.ref_obj
from (
        select tt.* 
             ,case when aa.object_name is null then 0 else 1 end is_use 
        from T_TMP_RECALC_REF0519 tt
        left join (select distinct object_name from T_TMP_USERSQL_REF1) aa on aa.object_name = tt.obj 
        where tt.ref_obj is null 
              or  (tt.obj <> 'M_REF_RESTR_CONS_EXT' and tt.ref_obj <> 'V_DATA_ALL') --зацикливание
              and (tt.obj <> 'M_REF_RESTR_CONS_EXT' and tt.ref_obj <> 'V_CONTRACT_CAL') --зацикливание
              and (tt.obj <> 'M_REJ_KN_J1' and tt.ref_obj <> 'M_TMP_J_SC_DEL_COMB_A_KN_2') --зацикливание
              and (tt.obj <> 'M_REJ_SCORE_EKT_FRAUD_8' and tt.ref_obj <> 'M_REJECTED_CONTRACTS') --зацикливание              
) t
start with t.obj in (
        select distinct t.obj from (
        select 
            t.obj
            , CONNECT_BY_ISLEAF
        from (
                select tt.*                     
                from T_TMP_RECALC_REF0519 tt
                where tt.ref_obj is null 
                      or  (tt.obj <> 'M_REF_RESTR_CONS_EXT' and tt.ref_obj <> 'V_DATA_ALL') --зацикливание
                      and (tt.obj <> 'M_REF_RESTR_CONS_EXT' and tt.ref_obj <> 'V_CONTRACT_CAL') --зацикливание
                      and (tt.obj <> 'M_REJ_KN_J1' and tt.ref_obj <> 'M_TMP_J_SC_DEL_COMB_A_KN_2') --зацикливание
                      and (tt.obj <> 'M_REJ_SCORE_EKT_FRAUD_8' and tt.ref_obj <> 'M_REJECTED_CONTRACTS') --зацикливание                      
        ) t
        start with t.ref_obj is null 
        connect by  
            prior t.obj = t.ref_obj
        ) t where  t.CONNECT_BY_ISLEAF =1   
) 
connect by --NOCYCLE 
    prior t.ref_obj = t.obj
) a




/* *******************************************************************************************************
   пользователи
*/



                                        

--собираем все или почти все запросы, которые выполнялись пользователями
create table T_TMP_RDWH_USQL0505 as
select t.PARSING_SCHEMA_NAME, t.SQL_ID, t.SQL_TEXT,t.SQL_FULLTEXT 
from v$sql t
where t.PARSING_SCHEMA_NAME not in (
          'DHK_USER'
          ,'SYSTEM'
          ,'SYS'
          ,'MDSYS'
          ,'AUDSYS'
          ,'CTXSYS'
          ,'DBSNMP'
          ,'ODMRSYS' 
      )
union all
select t.PARSING_SCHEMA_NAME, t.SQL_ID, t.SQL_TEXT,t.SQL_FULLTEXT
from T_RDWH_SQLAREA_X_SEC t  --это заполняется по job'у
left join v$sql a on a.SQL_ID = t.SQL_ID 
where t.PARSING_SCHEMA_NAME not in (
          'DHK_USER'
          ,'SYSTEM'
          ,'SYS'
          ,'MDSYS'
          ,'AUDSYS'
          ,'CTXSYS'
          ,'DBSNMP'
          ,'ODMRSYS'      
       ) and a.SQL_ID is null

--нужные объекты схемы	   
create table T_TMP_RDWH_ALLOBJ0505 as
select distinct
   t.object_name
   ,t.object_type
from all_objects t
where t.OWNER = 'U1' and t.object_type in ('VIEW','MATERIALIZED VIEW','TABLE','TABLE PARTITION','TABLE SUBPARTITION')
--удаляем таблицы, которые имеют мат.представление
delete from T_TMP_RDWH_ALLOBJ0505
where object_name in (select object_name from T_TMP_RDWH_ALLOBJ0505 t where t.object_type = 'MATERIALIZED VIEW')
      and object_type = 'TABLE';	   
	   
--строим взаимосвязи
create table T_TMP_USERSQL_REF3 as --523:17мин
select /*+ parallel(16)*/
   t.parsing_schema_name as user#
   ,t.sql_id
   ,t.sql_fulltext
   ,b.object_name
   ,b.object_type
from (
      select * from U1.T_TMP_RDWH_USQL0505 t
      where t.sql_text not like ('SELECT /* DS_SVC */ /*+ dynamic_sampling(0)%')
            --and t.parsing_schema_name not in ('U1')  
) t
left join u1.T_TMP_RDWH_ALLOBJ0505 b on
                              REGEXP_LIKE (upper(t.sql_fulltext), '(^|[^[\w]]|\s|;|\)|$|\(|"|\.)'||upper(b.object_name)||'([^[\w]]|\s|;|\)|$|"|\.)')  -- это магия!!!  )))  

    