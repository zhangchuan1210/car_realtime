from extensions import db

class ScoreRange(db.Model):
    __tablename__ = 'score_range'
    id = db.Column(db.Integer, primary_key=True)
    min_score = db.Column(db.Float, nullable=False)
    max_score = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f"<ScoreRange {self.min_score} - {self.max_score}>"
