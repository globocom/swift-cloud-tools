# -*- coding: utf-8 -*-
import requests
import json


class Utils(Object):

    def __init__(self, host):
        super(Utils, self).__init__()
        self.host = host

    def insert_expirer(self):
        headers = {'Content-type': 'application/json'}
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg",
            "date": "2021-06-01 12:15:00"
        }
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.host
            ), data=json.dumps(data), headers=headers
        )
