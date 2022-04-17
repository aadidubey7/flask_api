from flask import Flask
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import text

import datetime

app = Flask(__name__)
api = Api(app)

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///assessment.db"
db = SQLAlchemy(app)

Base = automap_base()
Base.prepare(db.engine, reflect=True)
job = Base.classes.job

last_day_of_prev_month = datetime.date.today().replace(day=1) - \
    datetime.timedelta(days=1)
first_day_of_prev_month = datetime.date.today().replace(
    day=1) - datetime.timedelta(days=last_day_of_prev_month.day)


class ReportResource(Resource):
    def get(self):
        report = []
        sql_query = text("""
        SELECT 
            num_of_tests,
            num_of_fail,
            test_start_date,
            pdd_score,
            country_id,
            country_name,
            call_description_id
        FROM
            (
                SELECT
                    COUNT(A.id) AS num_of_tests,
                    NULL AS num_of_fail,
                    A.test_start_date,
                    A.pdd_score,
                    A.country_id,
                    A.country_name,
                    A.call_description_id
                FROM (
                SELECT
                    jp.id as id,
                    jp.id AS job_id, 
                    strftime('%d-%m-%Y', jp.call_start_time) AS test_start_date,
                    ROUND((julianday(jp.call_connect_time) - julianday(jp.call_start_time)) * 86400) as pdd_score,
                    jp.call_description_id,
                    cc.id AS country_id,
                    cc.country_name AS country_name
                FROM job_processing AS jp
                JOIN number AS num ON num.id = jp.number_id
                JOIN country_code AS cc ON cc.id = num.country_code_id
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
                    A.call_description_id
                FROM (
                SELECT
                    jp.id as id,
                    jp.id AS job_id, 
                    strftime('%d-%m-%Y', jp.call_start_time) AS test_start_date,
                    ROUND((julianday(jp.call_connect_time) - julianday(jp.call_start_time)) * 86400) as pdd_score,
                    jp.call_description_id,
                    cc.id AS country_id,
                    cc.country_name AS country_name
                FROM job_processing AS jp
                JOIN number AS num ON num.id = jp.number_id
                JOIN country_code AS cc ON cc.id = num.country_code_id
                WHERE 
                    jp.call_start_time >= :first_day_of_prev_month AND 
                    jp.call_start_time <= :last_day_of_prev_month
                ) AS A
                WHERE A.call_description_id IS NOT NULL
                GROUP BY A.country_id, strftime('%d-%m-%Y', cast(A.test_start_date as date))
            )
        ORDER BY
            test_start_date
        """)

        obj_executed = db.engine.execute(sql_query, {
            "first_day_of_prev_month": first_day_of_prev_month,
            "last_day_of_prev_month" : last_day_of_prev_month
        })
        results = obj_executed.fetchall()

        for result in results:
            data = {}
            data["test_start_date"] = result.test_start_date
            data["num_of_tests"] = result.num_of_tests
            data["num_of_fails"] = result.num_of_fail
            data["pdd_score"] = result.pdd_score
            data["country_name"] = result.country_name

            report.append(data)

        return report


api.add_resource(ReportResource, "/report")

if __name__ == "__main__":
    app.run(debug=True)
