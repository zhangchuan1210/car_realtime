from app import db

class OilAvg(db.Model):
    __tablename__ = 'oil_avg'
    id = db.Column(db.Integer, primary_key=True)
    avg_consumption = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f"<OilAvg {self.avg_consumption}>"
