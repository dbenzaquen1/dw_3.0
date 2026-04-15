SELECT
  FORMAT_DATE('%F', d) AS id,
  d AS full_date,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(WEEK FROM d) AS year_week,
  EXTRACT(DAY FROM d) AS year_day,
  EXTRACT(YEAR FROM d) AS fiscal_year,
  FORMAT_DATE('%Q', d) AS fiscal_qtr,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE('%B', d) AS month_name,
  FORMAT_DATE('%Y-%m', d) AS month_year,
  DATE_SUB(d, INTERVAL EXTRACT(DAYOFWEEK FROM d) - 1 DAY) AS week_start_date,
  FORMAT_DATE('%w', d) AS week_day,
  FORMAT_DATE('%A', d) AS day_name,
  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN false ELSE true END) AS day_is_weekday,
  DATE_ADD(d, INTERVAL 6 - EXTRACT(DAYOFWEEK FROM d) DAY) AS end_of_week,
  DATE_SUB(DATE_ADD(d, INTERVAL 1 MONTH), INTERVAL EXTRACT(DAY FROM DATE_ADD(d, INTERVAL 1 MONTH)) DAY) AS end_of_month,
  CASE
    WHEN d = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 'yesterday'
    WHEN d = CURRENT_DATE() THEN 'today'
    WHEN d = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) THEN 'tomorrow'
    ELSE FORMAT_DATE('%F', d)
  END AS relative_day
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d
)
