# worker_app/_tasks_en_cour.py
import datetime
import json
import os
import time

from logging import Logger
from typing import Dict, List, Tuple

import pandas as pd

from celery import Celery, Task

import requests
from celery.utils.log import get_task_logger
from mangacollec_api.entity.author import Author
from mangacollec_api.entity.edition import Edition
from mangacollec_api.entity.genre import Genre
from mangacollec_api.entity.job import Job
from mangacollec_api.entity.kind import Kind
from mangacollec_api.entity.publisher import Publisher
from mangacollec_api.entity.task import Task as Tache
from mangacollec_api.entity.serie import Serie
from mangacollec_api.entity.serie_end import SerieEndpointEntity
from mangacollec_api.entity.volume import Volume
from pandas import DataFrame, Series
from sqlalchemy import create_engine, Engine

from mangacollec_api.client import MangaCollecAPIClient
from mangacollec_api.endpoints.serie_endpoint import SerieEndpoint

from config import MQ_USER, MQ_PASSWORD, MQ_HOST, POSTGRES_DB, POSTGRES_PORT, POSTGRES_HOST, POSTGRES_PASSWORD, \
    POSTGRES_USER, POSTGRES_TABLE, CLIENT_ID, CLIENT_SECRET

broker = f"pyamqp://{MQ_USER}:{MQ_PASSWORD}@{MQ_HOST}//"

app = Celery("worker_app", broker=broker, backend="db+sqlite:///results.db")

app.conf.update(
    timezone="Europe/Paris",
    task_routes={
        "_tasks_en_cour.fetch_serie_url": {"queue": "serie-get-url"},
        "_tasks_en_cour.save_image_volume": {"queue": "save_image_volume"},
    },
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)


logger: Logger = get_task_logger(__name__)


def save_image_from_url(url: str, name: str, path: str, proxy) -> bytes:
    image = requests.get(url, proxies=proxy, stream=True)
    f_ext = os.path.splitext(url)[-1]
    chemin = f"{path}{name}{f_ext}"
    with open(chemin, "wb") as f:
        f.write(image.content)

    return image.content


def get_image_from_url(url) -> bytes:
    if url is not None:
        response = requests.get(url)
        if response.status_code == 200:
            return response.content


def is_image_volume_existe_from_local(name, path):
    path = f"{path}/{name}.jpg"
    if os.path.exists(path):
        return True
    return False


class LoggedRetryTask(Task):
    autoretry_for = (Exception,)
    retry_kwargs = {"max_retries": 3, "countdown": 5}
    retry_backoff = True
    retry_jitter = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f"[-ÉCHEC-][TACHE][{self.name}][id={task_id}] - {exc}")

    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f"[-SUCCES-][TACHE][{self.name}][id={task_id}] => {retval}")


@app.task(bind=True, name="save_image_volume")
def task_save_image_volume(self, name: str, url: str, proxy: Dict):
    path = "./volumes/"

    try:
        if url is None:
            # print(f"[PROCESS VOLUME : {name}][IMAGE][NOT FOUND]")

            return False

        if is_image_volume_existe_from_local(name, path):
            # print(f"[PROCESS VOLUME : {name}][IMAGE][EXISTE]")

            return True

        # print(f"[PROCESS VOLUME : {name}][IMAGE][TELECHARGEMENT]")

        save_image_from_url(url, name, path, proxy)
        time.sleep(1)

        return True
    except Exception as e:
        # print(f"[PROCESS VOLUME : {name}][IMAGE][ERROR] {e}")
        return False


@app.task(bind=True, name="save_database_volume", base=LoggedRetryTask)
def task_save_database_volume(self, datas):
    engine: Engine = create_engine(
        (
            f"postgresql+psycopg2://{POSTGRES_USER}:"
            f"{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:"
            f"{POSTGRES_PORT}/"
            f"{POSTGRES_DB}"
        )
    )

    df: DataFrame = pd.DataFrame(datas)
    #df.set_index("id")

    df.to_sql(name=POSTGRES_TABLE, con=engine, if_exists="append")

    folder = "./csv"

    os.makedirs(folder, exist_ok=True)  # Crée le dossier s’il n'existe pas

    # filename = f"{str(df['serie_title'][0]).replace(' ', '_').replace('\'', '')}.csv"
    
    # df.to_csv(path_or_buf=os.path.join(folder, filename))
    
    # try:
    #
    # except Exception as e:
    #     raise self.retry(exc=e, countdown=60)


@app.task(bind=True, name="call_serie_api", base=LoggedRetryTask)
def task_call_serie_api(self: Task, id: str, proxy: Dict) -> bool:
    """

    :param id: ID serie
    :param proxy:
    :return:
    """

    proxy = format_proxy(proxy)

    client_mangacollec = MangaCollecAPIClient(
        client_id= CLIENT_ID,
        client_secret=CLIENT_SECRET,
        proxy=proxy

    )

    serie_endpoint = SerieEndpoint(client_mangacollec)

    serie_endpoint_entity: SerieEndpointEntity = serie_endpoint.get_series_by_id_v2(id)

    # netoyage donnee

    datas: List[Dict[str, any]] = []



    serie: Serie = serie_endpoint_entity.serie
    genre: Genre = serie_endpoint_entity.type
    kinds: List[str] = [str(kind) for kind in serie_endpoint_entity.kinds]
    tasks: List[Tache] = serie_endpoint_entity.tasks
    jobs: List[Job] = serie_endpoint_entity.jobs
    authors: List[Author] = serie_endpoint_entity.authors
    editions: List[Edition] = serie_endpoint_entity.editions
    publisher: Publisher = serie_endpoint_entity.publishers[0]
    volumes: List[Volume] = serie_endpoint_entity.volumes



    for volume in volumes:
        data_volume = _init_data_volume(volume)

        _add_serie_to_data(data_volume, serie)

        _add_genre_to_data(data_volume, genre)

        for edition in editions:
            print(edition)

            if data_volume['edition_id'] == edition.id:
                data_volume['edition_title'] = edition.title
                data_volume['edition_parent_id'] = edition.parent_edition_id
                data_volume['edition_volumes_count'] = edition.volumes_count
                data_volume['edition_last_volume_number'] = edition.last_volume_number
                data_volume['edition_commercial_stop'] = edition.commercial_stop
                data_volume['edition_not_finished'] = edition.not_finished
                data_volume['edition_follow_editions_count'] = edition.follow_editions_count

        datas.append(data_volume)

    # print(list_volume_images)

    # envoy a la queue pour traitement donnee volumes
    app.send_task("save_database_volume", args=[datas], queue="save_database_volume")

    # envoy a la queue pour traitement images volumes
    # [
    #     app.send_task("save_image_volume", args=[id, url, proxy], queue="save_image_volume")
    #     for id, url in list_volume_images.items()
    # ]

    return True


def _add_genre_to_data(data_volume, genre):
    data_volume['type_id'] = genre.id
    data_volume['type_title'] = genre.title
    data_volume['type_to_display'] = genre.to_display


def _add_serie_to_data(data_volume: Dict[str, any], serie: Serie) -> None:
    data_volume['serie_id'] = serie.id
    data_volume['serie_title'] = serie.title
    data_volume['serie_adult_content'] = serie.adult_content
    data_volume['serie_editions_count'] = serie.editions_count
    data_volume['serie_tasks_count'] = serie.tasks_count
    data_volume['serie_kinds_ids'] = serie.kinds_ids


def _init_data_volume(volume: Volume) -> Dict[str, any]:
    data_volume: Dict[str, any] = {}

    data_volume = _init_date_to_data(data_volume)

    data_volume = data_volume | volume.to_dict()

    return data_volume


def format_proxy(proxy) -> Dict:
    """

    :param proxy:
    :return:
    """
    return {"http": f"http://{proxy}"}


def clean_serie_donnee(data_endpoint: SerieEndpointEntity) -> Tuple[List[Dict], Dict[str, str]]:
    """

    :param data_endpoint:
    :return:
    """
    datas: List[Dict] = []
    list_url_volume: Dict[str, str] = {}

    serie: Serie = data_endpoint.serie
    publisher: Publisher = data_endpoint.publishers[0]
    editions: List[Edition] = data_endpoint.editions
    genre: Genre = data_endpoint.type
    authors: List[Author] = data_endpoint.authors
    jobs: List[Job] = data_endpoint.jobs
    tasks: List[Tache] = data_endpoint.tasks
    list_kinds: List[str] = [str(kind) for kind in data_endpoint.kinds]


    from dataclasses import is_dataclass

    print('DD ---->', data_endpoint)
    for volume in data_endpoint.volumes:
        datas: Dict = volume

        _init_date_to_data(datas)

        if datas["image_url"]:
            list_url_volume[datas["id"]] = datas["image_url"]

        datas["kind"] = list_kinds

        datas["serie"] = serie.title

        datas["genre"] = genre.title

        datas["publisher"] = publisher.title

        for edition in editions:
            if datas["edition_id"] == edition.id:
                datas["edition_title"] = edition.title

                datas["edition_volumes_count"] = edition.volumes_count

                datas["commercial_stop"] = edition.commercial_stop

                datas["last_volume_number"] = edition.last_volume_number

                datas["not_finished"] = edition.not_finished

                datas["follow_editions_count"] = edition.follow_editions_count

                datas["parent_edition_id"] = edition.parent_edition_id
        # [x for x in list if id ==  x['id']]
        taches: List[Dict] = []
        for task in tasks:
            # if task['id']:
            #     pass
            author = [x for x in authors if task.author_id ==  x.id]
            job = [x for x in jobs if task.job_id ==  x.id]
            author = author[0]
            job = job[0]
            taches.append({
                "name": author['name'],
                "first_name": author['first_name'],
                "tasks_count": author['tasks_count'],
                "job": job['title']
            })
            volume['authors'] = json.dumps(taches)

            # volume['job'] = job['title']
            #
            # volume['author_name'] = author['name']
            # volume['author_first_name'] = author['first_name']
            # volume['author_tasks_count'] = author['tasks_count']

        datas.append(volume)

    return datas, {"": ""}


def _init_date_to_data(data: Dict[str, any]) -> Dict:
    now = datetime.datetime.now()

    data['YEAR'] = now.year

    data['MONTH'] = now.month

    data['DAY'] = now.day

    data['HOUR'] = now.hour

    return data

if __name__ == '__main__':
    # Démarre le worker via la CLI intégrée
    import sys
    from celery.__main__ import main as celery_main

    app.start()
