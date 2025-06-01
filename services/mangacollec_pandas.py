from mangacollec_api.entity.edition import Edition
from mangacollec_api.entity.serie import Serie
from pandas.core.interchange.dataframe_protocol import DataFrame


def serie_add_pandas(df: DataFrame, serie: Serie) -> DataFrame:
    df['serie_id'] = serie.id
    df['serie_title'] = serie.title


def edition_add_pandas(df: DataFrame, edition: Edition) -> DataFrame:
    df['edition_id'] = edition.id
    df['edition_title'] = edition.title
    df['edition_title'] = edition.title
    df['edition_volumes_count'] = edition.volumes_count
    df['edition_commercial_stop'] = edition.commercial_stop
    df['edition_last_volume_number'] = edition.last_volume_number
    df['edition_not_finished'] = edition.not_finished
    df['edition_follow_editions_count'] = edition.follow_editions_count
    df['edition_parent_edition_id'] = edition.parent_edition_id
