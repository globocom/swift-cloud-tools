web: gunicorn -b 0.0.0.0:$PORT swift_cloud_tools.wsgi --timeout 60 --log-level INFO
expirer: ./run_service expirer
transfer: ./run_service transfer
health: ./run_service health
