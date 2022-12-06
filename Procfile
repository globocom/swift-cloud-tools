web: gunicorn -b 0.0.0.0:$PORT swift_cloud_tools.wsgi --timeout 60 --log-level INFO
expirer: ./run_service expirer
transfer: ./run_service transfer
# transfer_container: ./run_service transfer_container
health: ./run_service health
