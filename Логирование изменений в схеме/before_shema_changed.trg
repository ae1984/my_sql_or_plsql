create or replace trigger before_shema_changed
  before ddl on SCHEMA
declare
  old_txt  clob;
  sql_rows ora_name_list_t;
  rows     NUMBER;
  sql_txt  clob := null;
  prog     schema_changes.program%type;
  v_err    varchar2(100);
begin
/*  begin
    select program
      into prog
      from v$session
     where sid = (select SYS_CONTEXT('USERENV', 'SID') from dual);
  exception
    when others then*/
      prog := null;
  --end;
  v_err:='_';
  begin
    rows := ora_sql_txt(sql_rows);
    FOR i IN 1 .. rows LOOP
      sql_txt := sql_txt || sql_rows(i);
    END LOOP;
  exception
    when others then
      sql_txt := null;
      v_err:= v_err||' 1 '||'Произошла ошибка(exception)!';
  end;
  begin
    case upper(ora_dict_obj_type)
      when 'PACKAGE BODY' then
        select dbms_metadata.get_ddl('PACKAGE', upper(ora_dict_obj_name))
          into old_txt
          from dual;
      when 'SNAPSHOT' then
        select dbms_metadata.get_ddl('MATERIALIZED_VIEW',
                                     upper(ora_dict_obj_name))
          into old_txt
          from dual;     
      else
        select dbms_metadata.get_ddl(upper(ora_dict_obj_type),
                                     upper(ora_dict_obj_name))
          into old_txt
          from dual;
    end case;
  exception
    when others then
      old_txt := null;
      v_err:= v_err||' 2 '||'Произошла ошибка(exception)!';
  end;
  insert into schema_changes
    (obj_type,
     obj_name,
     old_text,
     change_date,
     sh_user,
     os_user,
     user_host,
     user_ip,
     sql_text,
     ora_event,
     program,
     err)
  values
    (upper(ora_dict_obj_type),
     upper(ora_dict_obj_name),
     old_txt,
     sysdate,
     upper(user),
     upper(SYS_CONTEXT('USERENV', 'OS_USER')),
     upper(SYS_CONTEXT('USERENV', 'HOST')),
     SYS_CONTEXT('USERENV', 'IP_ADDRESS'),
     sql_txt,
     upper(ora_sysevent),
     upper(prog),
     v_err);
end;
/
