from app import db

class ScoreAvg(db.Model):
    __tablename__ = 'score_avg'
    id = db.Column(db.Integer, primary_key=True)
    avg_score = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f"<ScoreAvg {self.avg_score}>"
