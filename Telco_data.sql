-- Project 2

CREATE TABLE event 
  AS SELECT * 
  FROM telecommunication.event;
CREATE TABLE cell
  AS SELECT * 
  FROM telecommunication.cell;
CREATE TABLE demographic 
  AS SELECT * 
  FROM telecommunication.demographic;

 
-- 2.4 4.	Create a table with average duration of stay of each customer at each cell that customer stayed at least 5 minutes.

CREATE TABLE event_duration 
  AS SELECT 
    e1.subscriber_id, 
    e1.aircomcellid, 
    FROM_UNIXTIME(e1.time) AS login_time,
  COALESCE((e1.time - e2.time)/60, 0) AS duration_minutes
  FROM event AS e1
  LEFT JOIN event AS e2
    ON  e2.subscriber_id = e1.subscriber_id 
    AND e2.time = (
    
        SELECT MAX(time)
        FROM event
        WHERE subscriber_id = e1.subscriber_id
        AND time < e1.time
        
        )
  ORDER BY e1.subscriber_id, login_time;

CREATE TABLE event_duration_5 
  AS SELECT * 
  FROM event_duration 
  WHERE duration_minutes > 5;

CREATE TABLE tab_assgn2_4 
  AS SELECT DISTINCT 
    AVG(duration_minutes) 
      OVER (PARTITION BY subscriber_id ORDER BY aircomcellid) 
      AS avg_duration_minutes,
    subscriber_id,
	aircomcellid 
FROM event_duration_5;

-- 2.5

-- Create a table with histogram showing how many subscribers stayed on average at each cell more than 5 minutes 
-- between 5 to 100 minutes, how many stayed between 100 to 200 minutes and so on.

CREATE TABLE tab_duration_A 
  AS SELECT DISTINCT 
    SUM(duration_minutes) 
      OVER (PARTITION BY subscriber_id) 
      AS sum_duration,
	subscriber_id,
	aircomcellid 
FROM event_duration_5
WHERE aircomcellid = 'A';

select floor(sum_duration/100)*100 as bin_floor, count(*)
from tab_duration_A  
group by 1
order by 1;

CREATE TABLE tab_duration_B 
  AS SELECT DISTINCT 
    SUM(duration_minutes) 
      over (partition by subscriber_id) 
      as sum_duration,
	subscriber_id,
	aircomcellid 
FROM event_duration_5
WHERE aircomcellid = 'B';

select floor(sum_duration/100)*100 as bin_floor, count(*)
from tab_duration_B  
group by 1
order by 1;

CREATE TABLE tab_duration_C 
  AS SELECT DISTINCT 
    SUM(duration_minutes) 
      over (partition by subscriber_id) 
      as sum_duration,
	subscriber_id,
	aircomcellid 
FROM event_duration_5
WHERE aircomcellid = 'C';

select floor(sum_duration/100)*100 as bin_floor, count(*)
from tab_duration_C
group by 1
order by 1;

-- 2.6 Which customer_id has longest avg dwell time (don’t consider stays at cells that were less than 5 minutes) 
-- and which customer_id has shortest.

with avgmin_allsubs_tbl as 
  (
  SELECT DISTINCT 
    AVG(duration_minutes) 
      over (partition by subscriber_id) 
      AS avg_duration_minutes,
	subscriber_id
  FROM event_duration_5
  ),
rank_tbl as 
  (
  select 
    avg_duration_minutes,
    subscriber_id, 
    rank() over (order by -avg_duration_minutes) as rnk
  from
    avgmin_allsubs_tbl 
  )
select * 
  from rank_tbl 
  where rnk = 1;

-- longest average dwell time 8f14e45fceea167a5a36dedd4bea2543 (code above)

with avgmin_allsubs_tbl as 
  (
  SELECT DISTINCT 
    AVG(duration_minutes) 
      over (partition by subscriber_id) 
      AS avg_duration_minutes,
	subscriber_id
  FROM event_duration_5
  ),
rank_tbl as 
  (
  select 
    avg_duration_minutes,
    subscriber_id, 
    rank() over (order by avg_duration_minutes) as rnk
  from
    avgmin_allsubs_tbl 
  )
select * 
  from rank_tbl 
  where rnk = 1;

-- shortest average dwell time fe9fc289c3ff0af142b6d3bead98a923 (code above)

-- 2.7 What is the total bytesdown and bytesup usage of netflix between 
-- 8 to 10 pm? Compare this usage with netflix usage between 8 and 10 am.

SELECT SUM(bytesdown) 
  from event 
  where service = 'Netflix' AND hour BETWEEN 20 AND 22;

-- bytesdown 20-22 75,450,717

SELECT SUM(bytesup) 
  from event 
  where service = 'Netflix' AND hour BETWEEN 20 AND 22;

-- bytesup 20-22 1,149,564

SELECT SUM(bytesdown)
 from event 
 where service = 'Netflix' AND hour BETWEEN 8 AND 10;

-- bytesdown 8-10 10,648,423

SELECT SUM(bytesup) 
  from event 
  where service = 'Netflix' AND hour BETWEEN 8 AND 10;

-- bytesup 8-10 554,314

-- 2.8 On average which service has highest bytesdown?

with sum_bd_tbl as 
  (
  SELECT DISTINCT 
    SUM(bytesdown) over (partition by service) AS sum_bytesdown,
	service
  FROM event
  ),
rank_tbl as 
  (
  select 
    sum_bytesdown,
    service, 
    rank() over (order by sum_bytesdown DESC) as rnk
  from
    sum_bd_tbl 
  )
select * 
  from rank_tbl 
  where rnk = 1;

-- Instagram

-- 2.9 Which cell_id has highest load (bytesdown + bytesup) on Youtube?

SELECT 
  SUM(bytesdown + bytesup) 
  FROM event 
  WHERE service = 'YouTube';

-- 167,295,545

-- 2.10 Which hour of the day we hb  ave highest data consumption on each cells? Do all cells are on highest peak 
-- at the same hour of the day?

with sum_bd_cell_tbl as 
  (
  SELECT DISTINCT 
    SUM(bytesdown + bytesup) OVER (PARTITION BY aircomcellid order by hour) AS bytesdown_cell_hour, 
		hour, 
		aircomcellid
	from event
	),
rank_tbl as
  (
  select 
    bytesdown_cell_hour,
    hour,
    aircomcellid, 
    rank() over (partition by aircomcellid order by bytesdown_cell_hour DESC) as rnk
  FROM sum_bd_cell_tbl 
  )
select * 
  from rank_tbl 
  where rnk = 1;
 
 -- the same for all cells, 9 am

-- 11.What portion of males and females use netflix? What portion of males and females use instagram? 
-- Which service is mostly used (duration) by males?

CREATE TABLE event_demog AS
  (
  SELECT 
    event.*, 
    demographic.gender,
    demographic.age
  FROM event
    LEFT JOIN demographic 
    ON event.subscriber_id= demographic.subscriber_id);

CREATE TABLE event_demog_sub AS 
  SELECT * 
  FROM event_demog where aircomcellid  = 'A';

select DISTINCT 
  ( 
    (
      (
      SELECT 
        COUNT(*) as count_female_nflx 
        FROM event_demog
        WHERE service = 'Netflix' AND gender = 'Female'
      ) 
      / 
      (
      SELECT 
        COUNT(*) as count_female 
        FROM event_demog
        where gender = 'Female'
      )
    ) 
    * 100 
  ) 
  AS perc_female_nflx
  from event_demog as percentage;


select DISTINCT 
  ( 
    (
      (
      SELECT 
        COUNT(*) as count_male_nflx 
        FROM event_demog
        WHERE service = 'Netflix' AND gender = 'Male'
      ) 
      / 
      (
      SELECT 
        COUNT(*) as count_male 
        FROM event_demog
        where gender = 'Male'
      )
    ) 
    * 100
  ) 
  AS perc_male_nflx
  from event_demog as percentage;

-- Netflix: male and female users at 15%

select DISTINCT 
  ( 
    (
      (
      SELECT 
        COUNT(*) as count_female_insta 
        FROM event_demog
        WHERE service = 'Instagram' AND gender = 'Female'
      ) 
      / 
      (
      SELECT 
        COUNT(*) as count_female 
        FROM event_demog
        where gender = 'Female'
      )
    )
    * 100 
  ) 
  AS perc_female_insta
  from event_demog as percentage;

select DISTINCT 
  ( 
    (
      (
      SELECT 
        COUNT(*) as count_male_insta 
        FROM event_demog
        WHERE service = 'Instagram' AND gender = 'Male'
      ) 
      / 
      (
      SELECT 
        COUNT(*) as count_male 
        FROM event_demog
        where gender = 'Male'
      )
    ) 
    * 100 
  ) 
  AS perc_male_insta
  from event_demog as percentage;

-- Netflix: male and female users at 30%

CREATE TABLE event_duration_demog AS 
  SELECT
    e1.subscriber_id, 
    FROM_UNIXTIME(e1.time) as login_time,
    COALESCE((e1.time - e2.time)/60, 0) AS duration_minutes,
    e1.service, 
    e1.gender 
  FROM event_demog  AS e1
    LEFT JOIN event_demog AS e2
      ON  e2.subscriber_id  = e1.subscriber_id 
      AND e2.time = 
        (
        SELECT 
          MAX(time)
            FROM event
            WHERE subscriber_id = e1.subscriber_id
            AND time < e1.time
        )
   order by 
     e1.subscriber_id, 
     login_time;

with sum_service_duration_tbl as 
  (
  SELECT DISTINCT
    SUM(duration_minutes) over (partition by service) as duration_service, 
	service, 
    gender
  from event_duration_demog 
  where gender = 'Male'
  ),
rank_tbl as
  (
  select 
    service,
    gender,
    duration_service, 
    rank() over (order by duration_service DESC) as rnk
  FROM sum_service_duration_tbl 
  )
select * 
  from rank_tbl 
  where rnk = 1;

-- men use Instagram for the longest amount of time
