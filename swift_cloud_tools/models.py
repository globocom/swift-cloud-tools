# -*- coding: utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import func
from datetime import datetime

db = SQLAlchemy(session_options={'autocommit': True})


class SaveDeleteModel():

    def save(self):
        try:
            db.session.begin()
            status = 200
            if self.id == None:
                db.session.add(self)
                status = 201
            db.session.commit()
            return "ok", status
        except Exception as e:
            if '1062' in str(e):
                return "Duplicate entry", 409
            else:
                return str(e.args), 500

    def delete(self):
        try:
            db.session.begin()
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
            func.lower(ExpiredObject.account) == account.lower()
        ).filter(
            func.lower(ExpiredObject.container) == container.lower()
        ).filter(
            func.lower(ExpiredObject.obj) == obj.lower()
        )

        if expired_object.count() > 0:
            return expired_object.first()
        else:
            return None

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Expired object '{}/{}/{}' created".format(
                self.account, self.container, self.obj), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Expired object '{}/{}/{}' deleted".format(
                self.account, self.container, self.obj), status

        return msg, status


class TransferProject(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False, unique=True)
    project_name = db.Column(db.String(64), nullable=False)
    environment = db.Column(db.String(10), nullable=False)
    container_count_swift = db.Column(db.Integer, default=0, nullable=True)
    object_count_swift = db.Column(db.Integer, default=0, nullable=True)
    bytes_used_swift = db.Column(BIGINT(unsigned=False), default=0, nullable=True)
    last_object = db.Column(db.String(255), nullable=True)
    count_error = db.Column(db.Integer, default=0, nullable=True)
    container_count_gcp = db.Column(db.Integer, default=0, nullable=True)
    object_count_gcp = db.Column(db.Integer, default=0, nullable=True)
    bytes_used_gcp = db.Column(BIGINT(unsigned=False), default=0, nullable=True)
    initial_date = db.Column(db.DateTime, nullable=True)
    final_date = db.Column(db.DateTime, nullable=True)

    def __init__(self, project_id, project_name, environment, container_count_swift=None,
                 object_count_swift=None, bytes_used_swift=None, last_object=None, count_error=None,
                 container_count_gcp=None, object_count_gcp=None, bytes_used_gcp=None,
                 initial_date=None, final_date=None):
        self.project_id = project_id
        self.project_name = project_name
        self.environment = environment
        self.container_count_swift = container_count_swift
        self.object_count_swift = object_count_swift
        self.bytes_used_swift = bytes_used_swift
        self.last_object = last_object
        self.count_error = count_error
        self.container_count_gcp = container_count_gcp
        self.object_count_gcp = object_count_gcp
        self.bytes_used_gcp = bytes_used_gcp
        self.initial_date = initial_date
        self.final_date = final_date

    def to_dict(self):
        tp = {
            "project_id": self.project_id,
            "project_name": self.project_name,
            "environment": self.environment,
            "container_count_swift": self.container_count_swift,
            "object_count_swift": self.object_count_swift,
            "bytes_used_swift": self.bytes_used_swift,
            "last_object": self.last_object,
            "count_error": self.count_error,
            "container_count_gcp": self.container_count_gcp,
            "object_count_gcp": self.object_count_gcp,
            "bytes_used_gcp": self.bytes_used_gcp,
            "initial_date": "",
            "final_date": ""
        }

        if self.initial_date:
            tp["initial_date"] = datetime.strftime(
                self.initial_date, "%Y-%m-%d %H:%M:%S")

        if self.final_date:
            tp["final_date"] = datetime.strftime(
                self.final_date, "%Y-%m-%d %H:%M:%S")

        return tp

    def find_transfer_project(project_id):
        transfer_project = TransferProject.query.filter(
            func.lower(TransferProject.project_id) == project_id
        )

        if transfer_project.count() > 0:
            return transfer_project.first()
        else:
            return None

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Transfer project '{}' environment '{}' created".format(
                self.project_name, self.environment), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Transfer project '{}' environment '{}' deleted".format(
                self.project_name, self.environment), status

        return msg, status


class TransferProjectError(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    object_error = db.Column(db.String(255), nullable=False)
    transfer_project_id = db.Column(db.Integer, db.ForeignKey('transfer_project.id'))
    created = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, object_error=None, transfer_project_id=None, created=None):
        self.object_error = object_error
        self.transfer_project_id = transfer_project_id
        self.created = created

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Transfer project error '{}' - {} created".format(
                self.object_error, self.transfer_project_id), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Transfer project error '{}' - {} deleted".format(
                self.object_error, self.transfer_project_id), status

        return msg, status


class ContainerInfo(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False, index=True)
    container_name = db.Column(db.String(255), nullable=False)
    updated = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, project_id=None, container_name=None, updated=None):
        self.project_id = project_id
        self.container_name = container_name
        self.updated = updated

    __table_args__ = (
        db.UniqueConstraint(project_id, container_name),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def find_container_info(project_id, container_name):
        container_info = (ContainerInfo.query
            .filter(ContainerInfo.project_id == project_id)
            .filter(ContainerInfo.container_name == container_name))

        if container_info.count() > 0:
            return container_info.first()
        else:
            return None

    def account_data(project_id, list_containers=False):
        data = ContainerInfo.query.filter(ContainerInfo.project_id == project_id)

        if data.count() == 0:
            return None

        bytes_used = 0
        object_count = 0
        containers = []

        for item in data:
            bytes_used += item.bytes_used
            object_count += item.object_count
            if list_containers:
                containers.append({
                    'name': item.container_name,
                    'count': item.object_count,
                    'bytes': item.bytes_used,
                    'last_modified': item.updated.isoformat()
                })

        return {
            'container_count': data.count(),
            'bytes_used': bytes_used,
            'object_count': object_count,
            'containers': containers
        }


class ProjectHostname(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False)
    hostname = db.Column(db.String(100), nullable=False)
    updated = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, project_id=None, hostname=None, updated=None):
        self.project_id = project_id
        self.hostname = hostname
        self.updated = updated

    __table_args__ = (
        db.UniqueConstraint(project_id, hostname),
    )

    def find_project_hostname(project_id, hostname):
        project_hostname = (ProjectHostname.query
            .filter(func.lower(ProjectHostname.project_id) == project_id)
            .filter(func.lower(ProjectHostname.hostname) == hostname))

        if project_hostname.count() > 0:
            return project_hostname.first()
        else:
            return None

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Project '{}' Hostname '{}' created".format(
                self.project_id, self.hostname), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Project '{}' Hostname '{}' deleted".format(
                self.project_id, self.hostname), status

        return msg, status


class TransferContainer(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False, index=True)
    project_name = db.Column(db.String(64), nullable=False)
    container_name = db.Column(db.String(255), nullable=False)
    environment = db.Column(db.String(10), nullable=False)
    container_count_swift = db.Column(db.Integer, default=0, nullable=True)
    object_count_swift = db.Column(db.Integer, default=0, nullable=True)
    bytes_used_swift = db.Column(BIGINT(unsigned=False), default=0, nullable=True)
    last_object = db.Column(db.String(255), nullable=True)
    count_error = db.Column(db.Integer, default=0, nullable=True)
    container_count_gcp = db.Column(db.Integer, default=0, nullable=True)
    object_count_gcp = db.Column(db.Integer, default=0, nullable=True)
    bytes_used_gcp = db.Column(BIGINT(unsigned=False), default=0, nullable=True)
    initial_date = db.Column(db.DateTime, nullable=True)
    final_date = db.Column(db.DateTime, nullable=True)

    def __init__(self, project_id, project_name, container_name, environment, container_count_swift=None,
                 object_count_swift=None, bytes_used_swift=None, last_object=None, count_error=None,
                 container_count_gcp=None, object_count_gcp=None, bytes_used_gcp=None,
                 initial_date=None, final_date=None):
        self.project_id = project_id
        self.project_name = project_name
        self.container_name = container_name
        self.environment = environment
        self.container_count_swift = container_count_swift
        self.object_count_swift = object_count_swift
        self.bytes_used_swift = bytes_used_swift
        self.last_object = last_object
        self.count_error = count_error
        self.container_count_gcp = container_count_gcp
        self.object_count_gcp = object_count_gcp
        self.bytes_used_gcp = bytes_used_gcp
        self.initial_date = initial_date
        self.final_date = final_date

    __table_args__ = (
        db.UniqueConstraint(project_id, container_name),
    )

    def to_dict(self):
        tp = {
            "project_id": self.project_id,
            "project_name": self.project_name,
            "container_name": self.container_name,
            "environment": self.environment,
            "container_count_swift": self.container_count_swift,
            "object_count_swift": self.object_count_swift,
            "bytes_used_swift": self.bytes_used_swift,
            "last_object": self.last_object,
            "count_error": self.count_error,
            "container_count_gcp": self.container_count_gcp,
            "object_count_gcp": self.object_count_gcp,
            "bytes_used_gcp": self.bytes_used_gcp,
            "initial_date": "",
            "final_date": ""
        }

        if self.initial_date:
            tp["initial_date"] = datetime.strftime(
                self.initial_date, "%Y-%m-%d %H:%M:%S")

        if self.final_date:
            tp["final_date"] = datetime.strftime(
                self.final_date, "%Y-%m-%d %H:%M:%S")

        return tp

    def find_transfer_container(project_id, container_name):
        transfer_container = (TransferContainer.query
            .filter(func.lower(TransferContainer.project_id) == project_id)
            .filter(func.lower(TransferContainer.container_name) == container_name)
        ).all()

        if transfer_container.count() > 0:
            return transfer_container
        else:
            return None

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Transfer project '{}' container '{}' environment '{}' created".format(
                self.project_name, self.container_name, self.environment), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Transfer project '{}' container '{}' environment '{}' deleted".format(
                self.project_name, self.container_name, self.environment), status

        return msg, status


class TransferContainerError(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    object_error = db.Column(db.String(255), nullable=False)
    transfer_container_id = db.Column(db.Integer, db.ForeignKey('transfer_container.id'))
    created = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, object_error=None, transfer_container_id=None, created=None):
        self.object_error = object_error
        self.transfer_container_id = transfer_container_id
        self.created = created

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Transfer container error '{}' - {} created".format(
                self.object_error, self.transfer_container_id), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Transfer container error '{}' - {} deleted".format(
                self.object_error, self.transfer_container_id), status

        return msg, status


class ProjectContainerHostname(db.Model, SaveDeleteModel):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), nullable=False)
    container_name = db.Column(db.String(255), nullable=False)
    hostname = db.Column(db.String(100), nullable=False)
    updated = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, project_id=None, hostname=None, updated=None):
        self.project_id = project_id
        self.container_name = container_name
        self.hostname = hostname
        self.updated = updated

    __table_args__ = (
        db.UniqueConstraint(project_id, container_name, hostname),
    )

    def find_project_container_hostname(project_id, container_name, hostname):
        project_container_hostname = (ProjectContainerHostname.query
            .filter(func.lower(ProjectContainerHostname.project_id) == project_id)
            .filter(func.lower(ProjectContainerHostname.container_name) == container_name)
            .filter(func.lower(ProjectContainerHostname.hostname) == hostname))

        if project_container_hostname.count() > 0:
            return project_container_hostname.first()
        else:
            return None

    def save(self):
        msg, status = SaveDeleteModel.save(self)

        if status == 201:
            return "Project '{}' container '{}' Hostname '{}' created".format(
                self.project_id, self.container_name, self.hostname), status

        return msg, status

    def delete(self):
        msg, status = SaveDeleteModel.delete(self)

        if status == 200:
            return "Project '{}' container '{}' Hostname '{}' deleted".format(
                self.project_id, self.container_name, self.hostname), status

        return msg, status
