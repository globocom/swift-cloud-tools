import requests
import json

from flask import Flask
from flask_testing import LiveServerTestCase


class Healthcheck(LiveServerTestCase):

    def create_app(self):
        app = Flask(__name__)
        app.config['TESTING'] = True
        app.config['LIVESERVER_PORT'] = 8943
        app.config['LIVESERVER_TIMEOUT'] = 10
        return app

    def setUp(self):
        self.host = 'http://0.0.0.0:5000'

    def test_flask_application_is_up_and_running(self):
        response = requests.get('{}/v1/healthcheck/'.format(self.host))

        self.assertEqual(response.status_code, 200)
