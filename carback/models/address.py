from app import db

class AddressSum(db.Model):
    __tablename__ = 'address_sum'
    id = db.Column(db.Integer, primary_key=True)
    city = db.Column(db.String(255), nullable=False)
    state = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f"<AddressSum {self.city}, {self.state}>"
