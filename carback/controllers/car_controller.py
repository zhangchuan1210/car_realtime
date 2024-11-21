from flask import Blueprint, jsonify, request
from services.car_service import CarService

page_controller = Blueprint('page_controller', __name__)

@page_controller.route('/cars/<int:car_id>', methods=['GET'])
def get_car(car_id):
    car = CarService.get_car_by_id(car_id)
    if car:
        return jsonify({'id': car.id, 'model': car.model, 'brand': car.brand, 'year': car.year, 'price': car.price})
    return jsonify({'error': 'Car not found'}), 404

@page_controller.route('/cars', methods=['GET'])
def get_all_cars():
    cars = CarService.get_all_cars()
    return jsonify([{'id': c.id, 'model': c.model, 'brand': c.brand, 'year': c.year, 'price': c.price} for c in cars])

@page_controller.route('/cars', methods=['POST'])
def add_car():
    data = request.get_json()
    model = data.get('model')
    brand = data.get('brand')
    year = data.get('year')
    price = data.get('price')
    car = CarService.add_car(model, brand, year, price)
    return jsonify({'id': car.id, 'model': car.model, 'brand': car.brand, 'year': car.year, 'price': car.price})

@page_controller.route('/cars/<int:car_id>', methods=['DELETE'])
def delete_car(car_id):
    car = CarService.delete_car(car_id)
    if car:
        return jsonify({'message': f'Car {car.model} deleted successfully'}), 200
    return jsonify({'error': 'Car not found'}), 404


