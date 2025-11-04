-- таблица users 
create table users (
id serial primary key,
name TEXT, 
email TEXT, 
role TEXT, 
updated_at TIMESTAMP default current_timestamp);

-- таблица user_audit
create table users_audit (
id serial primary key,
user_id integer,
changed_at timestamp default current_timestamp,
changed_by TEXT,
field_changed TEXT,
old_value TEXT,
new_value TEXT);

--Функция логирования изменений
create or replace function log_user_update()
returns trigger as $$
begin
    -- Проверяем изменение имени 
    if new.name is distinct from old.name then
       insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
       values (old.id, current_user, 'name', old.name, new.name);
    end if;

    -- Проверяем изменение email
    if new.email is distinct from old.email then
       insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
       values (old.id, current_user, 'email', old.email, new.email);
    end if;

    -- Проверяем изменение роли
    if new.role is distinct from old.role then
       insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
       values (old.id, current_user, 'role', old.role, new.role);
    end if;  
   
    -- Обновляем время изменения
    new.updated_at:=current_timestamp;
    return new;
end;
$$ language plpgsql;


-- Cоздание триггера на таблицу users
create trigger trg_log_user_changes
after update on users 
for each row 
execute function log_user_update();


-- Установка расширения pg_cron 
create extension if not exists pg_cron;

-- Функция для ежедневного экспорта свежих изменений 
create or replace function export_users_audit_csv()
returns void as $$
declare
     export_date TEXT:=to_char(current_date-1, 'YYYY_MM_DD');
     export_path TEXT:= '/tmp/users_audit_export_' || export_date || '.csv';
begin
    execute format(
        $sql$
            copy (
                select id, user_id, changed_at, changed_by, field_changed, old_value, new_value
                from users_audit
                where changed_at::date = current_date - 1
                order by changed_at
            )
            to %L
            with CSV HEADER
        $sql$,
        export_path
    );
end;
$$ language plpgsql;

SELECT cron.schedule(
    'daily_audit_export',
    '0 3 * * *',
    $$SELECT export_users_audit_csv();$$
);

UPDATE users
SET name = 'Vania Ivanov'
WHERE id = 1;

SELECT * FROM users_audit ORDER BY changed_at DESC;


SELECT export_users_audit_csv();

















