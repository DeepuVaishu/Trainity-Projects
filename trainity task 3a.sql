CREATE DATABASE job_analysis;

USE job_analysis;

CREATE TABLE job_data (
    job_id INT,
    actor_id INT,
    event VARCHAR(20),
    language VARCHAR(20),
    time_spent INT,
    org VARCHAR(100),
    ds DATE
);


DESCRIBE job_data;
ALTER TABLE job_data ADD COLUMN ds_date DATE;
UPDATE job_data
SET ds_date = STR_TO_DATE(ds, '%m/%d/%Y');
SELECT ds, ds_date FROM job_data LIMIT 10;
ALTER TABLE job_data DROP COLUMN ds;
ALTER TABLE job_data CHANGE ds_date ds DATE;

SELECT * FROM job_data;

SELECT 
  ds AS review_date,
  COUNT(*) AS jobs_reviewed
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;

WITH daily_throughput AS (
  SELECT
    ds,
    COUNT(*) AS total_events,
    SUM(time_spent) AS total_time_spent,
    (COUNT(*) / SUM(time_spent)) AS throughput
  FROM job_data
  GROUP BY ds
),
rolling_avg AS (
  SELECT 
    ds,
    throughput,
    ROUND(AVG(throughput) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 6) AS rolling_7_day_avg_throughput
  FROM daily_throughput
)
SELECT * FROM rolling_avg;

SELECT 
    language,
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_data WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'), 2) AS language_percentage
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY language
ORDER BY language_percentage DESC;

SELECT 
    ds, job_id, actor_id, event, language, time_spent, org, 
    COUNT(*) AS duplicate_count
FROM 
    job_data
GROUP BY 
    ds, job_id, actor_id, event, language, time_spent, org
HAVING 
    COUNT(*) > 1;



 

