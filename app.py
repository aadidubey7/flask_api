from flask import Flask
from flask_restful import Resource, Api, reqparse, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import text
import datetime
import sql_query

app = Flask(__name__)
api = Api(app)

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///assessment.db"
db = SQLAlchemy(app)

Base = automap_base()
Base.prepare(db.engine, reflect=True)
job = Base.classes.job

# Add request parser
req_parser = reqparse.RequestParser()
req_parser.add_argument('limit', type=int, required=False, location="args", help="The limit should be greater than 0 and less than 100.")
req_parser.add_argument('page', type=int, required=False, location="args", help="The page number should be greater than 1.")
req_parser.add_argument('sort', type=str, required=False, location="args", help="The sort column name should be in the string form.")
req_parser.add_argument('from', type=str, required=False, location="args", help="Please enter a valid from date.")
req_parser.add_argument('to', type=str, required=False, location="args", help="Please enter a valid to date.")


class ReportResource(Resource):
    def get(self):
        report = []
        
        args = req_parser.parse_args()
        lower_limit = 0
        upper_limit = 20
        
        # Change Limit (default is 20)
        lower_limit = 0
        if args['limit'] is not None and args['limit'] < 100:
            upper_limit = args['limit']
        else:
            upper_limit = 20
            
        # Change page number
        if args['page'] is not None and args['page'] > 1:
            lower_limit = upper_limit
            
        if args['from'] is not None and args['to'] is not None:
            try:
                from_date = datetime.datetime.strptime(args["from"], "%Y-%m-%d")
                to_date = datetime.datetime.strptime(args["to"], "%Y-%m-%d")
            except Exception as err:
                abort(400, message="Invalid From or To date provided. {}".format(err))
        else:
            from_date = sql_query.first_day_of_prev_month
            to_date = sql_query.last_day_of_prev_month

        obj_executed = db.engine.execute(sql_query.sql_query, {
            "first_day_of_prev_month": from_date,
            "last_day_of_prev_month" : to_date,
            "lower_limit": lower_limit,
            "upper_limit": upper_limit
        })
        results = obj_executed.fetchall()

        for result in results:
            data = {}
            data["test_start_date"] = result.test_start_date
            data["num_of_tests"] = result.num_of_tests
            data["num_of_fails"] = result.num_of_fail
            data["pdd_score"] = result.pdd_score
            data["country_name"] = result.country_name
            data["company_name"] = result.company_name
            data["connection_score"] = result.connection_score

            report.append(data)

        print(len(report))
        return report


api.add_resource(ReportResource, "/report")

if __name__ == "__main__":
    app.run(debug=True)
