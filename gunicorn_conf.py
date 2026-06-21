import os
import multiprocessing

bind = '0.0.0.0:5000'
workers = int(os.environ.get('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))
worker_class = 'sync'
worker_connections = 1000
timeout = 30
keepalive = 2

loglevel = 'info'
accesslog = '-'
errorlog = '-'


def on_starting(server):
    pass


def post_fork(server, worker):
    os.environ['GUNICORN_WORKER_ID'] = str(worker.abspid or worker.pid or 0)
