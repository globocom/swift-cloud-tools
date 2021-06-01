from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()

class SaveDeleteModel():

    def save(self):
        if self.id == None:
            db.session.add(self)
        db.session.commit()
        return '', 200

    def delete(self):
        db.session.delete(self)
        db.session.commit()
        return '', 200


class ExpiredObject(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    account = db.Column(db.String(80), nullable=False)
    container = db.Column(db.String(255), nullable=False)
    obj = db.Column('object', db.String(255), nullable=False)
    date = db.Column(db.DateTime, nullable=False)

    def __init__(self, app_name=None, name=None):
        self.account = account
        self.container = container
        self.obj = obj
        self.date = date
