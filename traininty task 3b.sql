CREATE DATABASE metric_spike_analysis;
USE metric_spike_analysis;

select * from users;

ALTER TABLE users
ADD COLUMN created_at_dt DATETIME,
ADD COLUMN activated_at_dt DATETIME;

UPDATE users
SET created_at_dt = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i'),
    activated_at_dt = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');

SELECT created_at, created_at_dt, activated_at, activated_at_dt
FROM users
LIMIT 10;

ALTER TABLE users
DROP COLUMN created_at,
DROP COLUMN activated_at;

select * from users;

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\events.csv"
INTO TABLE EVENTS
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT *  FROM events;

DESC events;

ALTER TABLE events
ADD COLUMN occurred_at_dt DATETIME;

UPDATE events
SET occurred_at_dt = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE events
DROP COLUMN occurred_at;

SELECT *  FROM events;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT *  FROM email_events;

DESC email_events;

ALTER TABLE email_events
ADD COLUMN occurred_at_dt DATETIME;

UPDATE email_events
SET occurred_at_dt = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE email_events
DROP COLUMN occurred_at;

SELECT *  FROM email_events;

/*Tasks:
Weekly User Engagement:
Objective: Measure the activeness of users on a weekly basis.
Your Task: Write an SQL query to calculate the weekly user engagement.*/

SELECT
    YEAR(occurred_at_dt) AS year,
    WEEK(occurred_at_dt) AS week,
    COUNT(DISTINCT user_id) AS active_users
FROM
    events
GROUP BY
    YEAR(occurred_at_dt), WEEK(occurred_at_dt)
ORDER BY
    year, week;
 


/*User Growth Analysis:
Objective: Analyze the growth of users over time for a product.
Your Task: Write an SQL query to calculate the user growth for the product.*/

SELECT 
    YEAR(created_at_dt) AS year,
    WEEK(created_at_dt) AS week,
    COUNT(user_id) AS new_users
FROM users
GROUP BY YEAR(created_at_dt), WEEK(created_at_dt)
ORDER BY year, week;


/*Weekly Retention Analysis:
Objective: Analyze the retention of users on a weekly basis after signing up for a product.
Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.*/

WITH user_signup_week AS (
    SELECT 
        user_id,
        MIN(DATE(activated_at_dt)) AS signup_date,
        WEEK(MIN(DATE(activated_at_dt))) AS signup_week
    FROM users
    GROUP BY user_id
),
user_events AS (
    SELECT 
        user_id,
        WEEK(DATE(occurred_at_dt)) AS event_week,
        DATE(occurred_at_dt) AS event_date
    FROM events 
),
retention AS (
    SELECT 
        s.signup_week,
        e.event_week,
        COUNT(DISTINCT e.user_id) AS retained_users
    FROM user_signup_week s
    JOIN user_events e ON s.user_id = e.user_id
    WHERE e.event_week >= s.signup_week
    GROUP BY s.signup_week, e.event_week
)
SELECT 
    signup_week,
    event_week,
    retained_users
FROM retention
ORDER BY signup_week, event_week;

/*Weekly Engagement Per Device:
Objective: Measure the activeness of users on a weekly basis per device.
Your Task: Write an SQL query to calculate the weekly engagement per device.*/

SELECT
    YEAR(occurred_at_dt) AS year,
    WEEK(occurred_at_dt) AS week,
    device,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM
    events
GROUP BY
    year, week, device
ORDER BY
    year, week, device;

/*Email Engagement Analysis:
Objective: Analyze how users are engaging with the email service.
Your Task: Write an SQL query to calculate the email engagement metrics.
Please note that for each task, you should also provide insights and interpretations of the results obtained from your queries.*/
SELECT
    action,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events
FROM
    email_events
GROUP BY
    action
ORDER BY
    total_events DESC;


