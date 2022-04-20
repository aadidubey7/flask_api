import datetime

last_day_of_prev_month = datetime.date.today().replace(day=1) - \
    datetime.timedelta(days=1)
first_day_of_prev_month = datetime.date.today().replace(
    day=1) - datetime.timedelta(days=last_day_of_prev_month.day)

sort_columns = ["num_of_tests", "num_of_fail", "test_start_date", "pdd_score", "country_name", "connection_score"]

sql_query = """
SELECT 
    one.num_of_tests,
    one.num_of_fail,
    one.test_start_date,
    one.pdd_score,
    one.country_id,
    one.country_name,
	one.company_name,
    one.call_description_id,
    two.connection_score
FROM
    (
        SELECT
            COUNT(A.id) AS num_of_tests,
            NULL AS num_of_fail,
            A.test_start_date,
            A.pdd_score,
            A.country_id,
            A.country_name,
			A.company_name,
            A.call_description_id
        FROM (
        SELECT
            jp.id as id,
            jp.id AS job_id, 
            strftime('%d-%m-%Y', jp.call_start_time) AS test_start_date,
            ROUND((julianday(jp.call_connect_time) - julianday(jp.call_start_time)) * 86400) as pdd_score,
            jp.call_description_id,
            cc.id AS country_id,
            cc.country_name AS country_name,
			c.name AS company_name
        FROM job_processing AS jp
        JOIN number AS num ON num.id = jp.number_id
        JOIN country_code AS cc ON cc.id = num.country_code_id
		JOIN company AS c ON c.id = num.company_id
        WHERE 
            jp.call_start_time >= :first_day_of_prev_month AND 
            jp.call_start_time <= :last_day_of_prev_month
        ) AS A
        WHERE A.call_description_id IS NULL
        GROUP BY A.country_id, strftime('%d-%m-%Y', cast(A.test_start_date as date))

    UNION 
        SELECT
            NULL AS num_of_tests,
            COUNT(A.id) AS num_of_fail,
            A.test_start_date,
            A.pdd_score,
            A.country_id,
            A.country_name,
			A.company_name,
            A.call_description_id
        FROM (
        SELECT
            jp.id as id,
            jp.id AS job_id, 
            strftime('%d-%m-%Y', jp.call_start_time) AS test_start_date,
            ROUND((julianday(jp.call_connect_time) - julianday(jp.call_start_time)) * 86400) as pdd_score,
            jp.call_description_id,
            cc.id AS country_id,
            cc.country_name AS country_name,
			c.name AS company_name
        FROM job_processing AS jp
        JOIN number AS num ON num.id = jp.number_id
        JOIN country_code AS cc ON cc.id = num.country_code_id
		JOIN company AS c ON c.id = num.company_id
        WHERE 
            jp.call_start_time >= :first_day_of_prev_month AND 
            jp.call_start_time <= :last_day_of_prev_month
        ) AS A
        WHERE A.call_description_id IS NOT NULL
        GROUP BY A.country_id, strftime('%d-%m-%Y', cast(A.test_start_date as date))
    ) AS one

LEFT JOIN 
    (
        SELECT
            ROUND(((COUNT(A.id) * 100.0 ) / A.total_test_cases), 2) || '%' AS connection_score,
            A.test_start_date
        FROM (
        SELECT
            (SELECT count(job_processing.id) FROM job_processing WHERE call_start_time >= :first_day_of_prev_month AND 
            call_start_time <= :last_day_of_prev_month) AS total_test_cases,
            jp.id as id,
            jp.id AS job_id, 
            strftime('%d-%m-%Y', jp.call_start_time) AS test_start_date,
            ROUND((julianday(jp.call_connect_time) - julianday(jp.call_start_time)) * 86400) as pdd_score,
            jp.call_description_id
        FROM job_processing AS jp
        WHERE 
            jp.call_start_time >= :first_day_of_prev_month AND 
            jp.call_start_time <= :last_day_of_prev_month
        ) AS A
        WHERE A.call_description_id IS NULL
        GROUP BY strftime('%d-%m-%Y', cast(A.test_start_date as date))
        ORDER BY A.test_start_date
    ) AS two 
    ON one.test_start_date = two.test_start_date

ORDER BY one.test_start_date
LIMIT :lower_limit, :upper_limit
"""