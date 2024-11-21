from flask import Blueprint, jsonify, request
from flask_apscheduler import APScheduler
from job import Job
schedular_controller = Blueprint('scheduler_controller', __name__, url_prefix='/scheduler')
scheduler = APScheduler()


@schedular_controller.route('/create', methods=['POST'])
def create_scheduler():
    json_data = request.get_json()
    func=getattr(Job, json_data.get('scheduled_task'), None)
    scheduler.add_job(id=json_data.get('id'), func=func, trigger='interval')
    return jsonify({'message': 'success'})


@schedular_controller.route('/start/<string:job_id>', methods=['GET'])
def start_scheduler(job_id):
    scheduler.run_job(job_id)
    return jsonify({'id': job_id, 'message': 'success'})


@schedular_controller.route('/stop/<string:job_id>', methods=['GET'])
def stop_scheduler(job_id):
    scheduler.pause_job(job_id)
    return jsonify({'id': job_id, 'message': 'success'})


def test_task():
    print('test task')

