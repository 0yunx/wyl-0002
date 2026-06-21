import os
import multiprocessing

bind = '0.0.0.0:5000'

workers = int(os.environ.get('GUNICORN_WORKERS', '1'))
worker_class = os.environ.get('GUNICORN_WORKER_CLASS', 'gthread')
threads = int(os.environ.get('GUNICORN_THREADS', str(multiprocessing.cpu_count() * 2 + 4)))
worker_connections = 1000
timeout = 30
keepalive = 2

loglevel = 'info'
accesslog = '-'
errorlog = '-'


def on_starting(server):
    server.log.info('=' * 60)
    server.log.info('Gunicorn 启动配置')
    server.log.info('  workers: %d (建议保持 1，避免 MQTT 重复订阅 & SQLite 并发写)', workers)
    server.log.info('  worker_class: %s (%s)', worker_class,
                   'IO 密集型推荐 gthread，配合 threads 并发' if worker_class == 'gthread' else '')
    server.log.info('  threads: %d', threads)
    server.log.info('=' * 60)


def post_fork(server, worker):
    os.environ['GUNICORN_WORKER_ID'] = str(worker.abspid or worker.pid or 0)
    server.log.info('Worker %s 已启动 (pid=%s)', worker.abspid or worker.pid or 0, os.getpid())
