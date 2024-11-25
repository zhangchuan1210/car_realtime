from flask import Blueprint, jsonify, request
from util.SchedulerUtil import SchedulerUtil
from services.scheduler_service import SchedulerService
schedular_controller = Blueprint('scheduler_controller', __name__, url_prefix='/scheduler')
@schedular_controller.route('/create', methods=['POST'])
def create_scheduler():
    json_data = request.get_json()
    func=getattr(SchedulerService(), json_data.get('scheduled_task'), None)
    SchedulerUtil.add_job(id=json_data.get('id'), func=func, trigger=json_data.get('trigger'),seconds=json_data.get('seconds'))
    return jsonify({'message': 'success'})


@schedular_controller.route('/start/', methods=['GET'])
def start_scheduler():
    SchedulerUtil.start()
    return jsonify({'id': '000','message': 'success'})


@schedular_controller.route('/stop/<string:job_id>', methods=['GET'])
def stop_scheduler(job_id):
    SchedulerUtil.stop(job_id)
    return jsonify({'id': job_id, 'message': 'success'})


