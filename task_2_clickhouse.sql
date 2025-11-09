-- создаем таблицу user_events событий пользователя

CREATE TABLE IF NOT EXISTS user_events (
        user_id UInt32,
        event_type String,
        points_spent UInt32,
        event_time DateTime)
ENGINE=MergeTree() 
ORDER BY (event_time, user_id)
TTL event_time+INTERVAL 30 DAY DELETE;

-- создаем агрегированную таблицу logs_agg на основе движка AggregationMergeTree 

CREATE TABLE IF NOT EXISTS logs_agg (
        event_date Date,
        event_type String,
        uniq_users_state AggregateFunction(uniq, UInt32),
        points_spent_state AggregateFunction(sum, UInt32),
        action_count_state AggregateFunction(count, UInt32))
ENGINE=AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date+INTERVAL 180 DAY DELETE;

-- создаем материализованное представление logs_mv, которое будет срабатывать автоматически и считывать промежуточные состояния и записывать их в logs_agg

CREATE MATERIALIZED VIEW logs_mv
TO logs_agg
AS 
SELECT 
   toDate(event_time) as event_date,
   event_type,
   uniqState(user_id) as uniq_users_state,
   sumState(points_spent) as points_spent_state,
   countState() as action_count_state
from user_events
group by event_date, event_type;

-- запрос для вставки тестовых данных

INSERT INTO user_events VALUES

(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),


(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),


(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),


(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),


(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),


(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- запрос с группировками по быстрой аналитике по дням

select event_date,
       event_type,
       uniqMerge(uniq_users_state) as unique_users,
       sumMerge(points_spent_state) as total_spent, 
       countMerge(action_count_state) as total_action
from logs_agg
group by event_date,
         event_type
order by event_date;   

-- Retention: сколько пользователей вернулись в течение следующих 7 дней

WITH day0 AS ( SELECT user_id, 
                      MIN(toDate(event_time)) AS first_day 
               FROM user_events 
               GROUP BY user_id ), 
     returned AS (SELECT d.first_day as first_day, 
                         countDistinctIf(e.user_id, dateDiff('day', d.first_day, toDate(e.event_time)) BETWEEN 1 AND 7) AS returned_users, 
                         countDistinct(d.user_id) AS total_day0_users 
              FROM day0 d LEFT JOIN user_events e ON d.user_id = e.user_id 
              GROUP BY d.first_day ) 

SELECT first_day,
       total_day0_users AS total_users_day_0, 
       returned_users AS returned_in_7_days, 
       round(returned_users / total_day0_users * 100, 2) AS retention_7d_percent
FROM returned 
ORDER BY first_day;

