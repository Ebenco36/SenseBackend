from App.api_utils import embase_access, ilove_access, furtherProcessEmbase
from flask import Flask, Blueprint, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from Controllers.DataController import dataPage, getFieldValues, get_row_by_id
from flask_cors import CORS
from rq import Queue
from redis import Redis

app = Flask(__name__)

# Configure SQLAlchemy job store using SQLite
jobstore = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}

scheduler = BackgroundScheduler(jobstores=jobstore)
scheduler.start()

app.config.from_mapping(
    RQ_REDIS_URL='redis://localhost:6379/0'  # Replace with your Redis URL
)
CORS(app)
redis_conn = Redis.from_url(app.config['RQ_REDIS_URL'])
queue = Queue(connection=redis_conn)


def my_queued_job(arg1, arg2):
    return f"Queued job executed with args: {arg1}, {arg2}"

# job = queue.enqueue(my_queued_job, 'arg1_value', 'arg2_value')

# Schedule the job to run every 5 seconds
# scheduler.add_job(my_job, 'interval', seconds=5)

bp = Blueprint('routes', __name__)

@app.route('/spool-from-embase', methods=['GET', 'POST'])
def embase_endpoint():
    embase_access()
    return jsonify({})

@app.route('/spool-from-embase-process', methods=['GET', 'POST'])
def embase_endpoint_processing():
    # add a job for this.
    furtherProcessEmbase()
    return jsonify({})

@app.route('/spool-from-ilove', methods=['GET', 'POST'])
def iLove_endpoint():
    ilove_access()
    return jsonify({})

@app.route('/page-content', methods=['GET', 'POST'])
def get():
    return dataPage()


@app.route('/get_row/<int:row_id>', methods=['GET'])
def getSingleDataById(row_id):
    return get_row_by_id(row_id)

@app.route('/page-filter', methods=['GET'])
def getFilters():
    return getFieldValues()

if __name__ == "__main__":
    app.run(debug=True)
