
from extensions import db
from models.car import Car

class CarService:
    def get_car_by_id(self,car_id):
        return db.session.execute("select * from user_t where id=?", car_id)

    def get_all_cars(self):
        return Car.query.all()

    def add_car(self,model, brand, year, price):
        car = Car(model=model, brand=brand, year=year, price=price)
        db.session.add(car)
        db.session.commit()
        return car

    def delete_car(self,car_id):
        car = Car.query.get(car_id)
        if car:
            db.session.delete(car)
            db.session.commit()
        return car
