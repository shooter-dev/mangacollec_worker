FROM python:3.13

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN mkdir volumes && pip install --upgrade pip && pip install -r requirements.txt

COPY ./worker_app/ /app/

CMD ["celery", "-A", "tasks", "worker", "-n", "worker_docker@%n", "-Q", "call_serie_api,save_image_volume,save_database_volume", "-c", "1", "--loglevel=info"]
