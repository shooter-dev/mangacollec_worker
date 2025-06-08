import os

from sqlalchemy import Engine, create_engine

#IMAGE
FOLDER_TO_IMAGE_VOLUME = "./images/volume/"

# CSV
FOLDER_TO_CSV = "./csv/"

#MANGACOLLEC
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')

#RABBITMQ
MQ_USER = os.environ.get('RABBITMQ_DEFAULT_USER')
MQ_PASSWORD = os.environ.get('RABBITMQ_DEFAULT_PASS')
MQ_HOST = os.environ.get('RABBITMQ_DEFAULT_URL')

#DATABASE
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_TABLE = os.environ.get('POSTGRES_TABLE')

ENGINE: Engine = create_engine(
        (
            f"postgresql+psycopg2://{POSTGRES_USER}:"
            f"{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:"
            f"{POSTGRES_PORT}/"
            f"{POSTGRES_DB}"
        )
    )