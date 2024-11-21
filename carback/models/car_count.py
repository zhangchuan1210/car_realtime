from app import db

class CarCount(db.Model):
    __tablename__ = 'car_count'
    id = db.Column(db.Integer, primary_key=True)
    count = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return f"<CarCount {self.count}>"
