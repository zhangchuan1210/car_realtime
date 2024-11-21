from app import db

class Car(db.Model):
    __tablename__ = 'user_t'
    user_id = db.Column(db.Integer, primary_key=True)
    c_name = db.Column(db.String(255), nullable=True)
    c_type = db.Column(db.String(255), nullable=True)
    price = db.Column(db.String(255), nullable=True)
    price_section = db.Column(db.String(255), nullable=True)
    post_date = db.Column(db.String(255), nullable=True)
    buy_date = db.Column(db.String(255), nullable=True)
    buy_address = db.Column(db.String(255), nullable=True)
    oil_consume = db.Column(db.Float(255), nullable=True)
    user_score = db.Column(db.String(255), nullable=True)
    message = db.Column(db.Integer, nullable=True)
    def __repr__(self):
        return "<Car {self.user_id} {self.c_name} {self.c_type} {self.price} {self.price_section} {self.post_date}>"
