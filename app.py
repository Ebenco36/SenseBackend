from flask import Flask, Blueprint, request, current_app, jsonify
from App.api_utils import scraping, embase_access, convert_json_to_List_of_dict
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
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

redis_conn = Redis.from_url(app.config['RQ_REDIS_URL'])
queue = Queue(connection=redis_conn)

def my_queued_job(arg1, arg2):
    return f"Queued job executed with args: {arg1}, {arg2}"

# job = queue.enqueue(my_queued_job, 'arg1_value', 'arg2_value')

# Schedule the job to run every 5 seconds
# scheduler.add_job(my_job, 'interval', seconds=5)

bp = Blueprint('routes', __name__)

@app.route('/spool-from-embase', methods=['GET', 'POST'])
def index():
    embase_access()
    return jsonify({})

@app.route('/spool-from-ilove', methods=['GET', 'POST'])
def iLove():
    convert_json_to_List_of_dict()
    return jsonify({})

if __name__ == "__main__":
    app.run(debug=True)
