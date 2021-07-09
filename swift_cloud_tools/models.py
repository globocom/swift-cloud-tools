# -*- coding: utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import func
from datetime import datetime

db = SQLAlchemy(session_options={'autocommit': True})


class SaveDeleteModel():

    def save(self):
        try:
            if self.id == None:
                db.session.add(self)
            db.session.commit()
            return "ok", 201
        except Exception as e:
            if '1062' in str(e):
                return "Duplicate entry", 409
            else:
                return str(e.args), 500

    def delete(self):
        try:
            db.session.delete(self)
            db.session.commit()
            return 'ok', 201
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

    def save(self):
        db.session.begin()
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Expired object '{}/{}/{}' created".format(
                self.account, self.container, self.obj), status

        return msg, status

    def delete(self):
        db.session.begin()
        msg, status = SaveDeleteModel.delete(self)

        if status == 201:
            return "Expired object '{}/{}/{}' deleted".format(
                self.account, self.container, self.obj), status

        return msg, status


class TransferProject(db.Model, SaveDeleteModel):
    __table_args__ = (
        db.UniqueConstraint('project_id', 'project_name', 'environment', name='transfer_project_unique'),
    )

    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False)
    project_name = db.Column(db.String(64), nullable=False)
    environment = db.Column(db.String(10), nullable=False)
    object_count = db.Column(db.Integer, default=0, nullable=True)
    bytes_used = db.Column(BIGINT(unsigned=False), default=0, nullable=True)
    initial_date = db.Column(db.DateTime, nullable=True)
    final_date = db.Column(db.DateTime, nullable=True)

    def __init__(self, project_id=None, project_name=None, environment=None,
        object_count=None, bytes_used=None, initial_date=None, final_date=None):
        self.project_id = project_id
        self.project_name = project_name
        self.environment = environment
        self.object_count = object_count
        self.bytes_used = bytes_used
        self.initial_date = initial_date
        self.final_date = final_date

    def find_transfer_project(project_id):
        transfer_project = TransferProject.query.filter(
            func.lower(TransferProject.project_id)==project_id
        )

        if transfer_project.count() > 0:
            return transfer_project.first()
        else:
            return None

    def save(self):
        db.session.begin()
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Transfer project '{}' environment '{}' created".format(
                self.project_name, self.environment), status

        return msg, status

    def delete(self):
        db.session.begin()
        msg, status = SaveDeleteModel.delete(self)

        if status == 201:
            return "Transfer project '{}' environment '{}' deleted".format(
                self.project_name, self.environment), status

        return msg, status
