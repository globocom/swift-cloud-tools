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


class DeletedObject(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.String(255), unique=True)
    date = db.Column(db.DateTime, nullable=False)

    def __init__(self, app_name=None, name=None):
        self.path = path
        self.date = date

    def __repr__(self):
        return '<{}>'.format(self.path)
