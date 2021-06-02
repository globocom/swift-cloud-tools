# -*- coding: utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

db = SQLAlchemy()


class SaveDeleteModel():

    def save(self):
        try:
            if self.id == None:
                db.session.add(self)
            db.session.commit()
            return 'ok', 200
        except Exception as e:
            if '1062' in str(e):
                return 'Duplicate entry', 409
            else:
                return str(e.orig), 500

    def delete(self):
        try:
            db.session.delete(self)
            db.session.commit()
            return 'ok', 200
        except Exception as e:
            if 'is not persisted' in str(e):
                return 'Not found', 404
            else:
                return str(e), 500


class ExpiredObject(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    account = db.Column(db.String(80), nullable=False)
    container = db.Column(db.String(255), nullable=False)
    obj = db.Column('object', db.String(255), nullable=False)
    date = db.Column(db.DateTime, nullable=False)

    def __init__(self, account=None, container=None, obj=None, date=None):
        self.account = account
        self.container = container
        self.obj = obj
        self.date = date

    __table_args__ = (
        db.UniqueConstraint(
            account,
            container,
            obj,
            name='uk_expobj_account_container_obj'
        ),
    )

    def find_expired_object(account, container, obj):
        expired_object = ExpiredObject.query.filter(
            func.lower(ExpiredObject.account)==account.lower()
        ).filter(
            func.lower(ExpiredObject.container)==container.lower()
        ).filter(
            func.lower(ExpiredObject.obj)==obj.lower()
        )

        if expired_object.count() > 0:
            return expired_object.first()
        else:
            return None
