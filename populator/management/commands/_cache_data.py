from django.db import connection
from django.db import utils
import psycopg2
import logging
from website.models import Dataset, ResolvableObject
from populator.models import ResolvableObjectMigration
from datetime import date, datetime


def sync_datasets(migration_dataset_ids):
    #  TODO what about datasets + records which get deleted then added again?
    Dataset.objects.exclude(id__in=migration_dataset_ids).update(deleted_date=date.today())
    ResolvableObject.objects.exclude(dataset__id__in=migration_dataset_ids).update(deleted_date=date.today())


def merge_in_new_data(reset=False):
    logger = logging.getLogger(__name__)
    # if reset:
    #     reset()
    #     return

    start = datetime.now()
    with connection.cursor() as cursor:
        cursor.execute(get_insert_history_and_update_sql())
    logger.info('inserted data changes {}'.format(datetime.now() - start))
    start = datetime.now()

    with connection.cursor() as cursor:
        cursor.execute(get_add_new_records_sql())
    logger.info('added new records {}'.format(datetime.now() - start))
    start = datetime.now()

    sql = """
        UPDATE website_resolvableobject
            SET deleted_date = CURRENT_DATE
        WHERE id IN (
            SELECT old.id
            FROM website_resolvableobject AS old
            LEFT JOIN populator_resolvableobjectmigration AS new ON new.id = old.id
            WHERE new.id IS NULL AND old.deleted_date IS NULL)
        """
    with connection.cursor() as cursor:
        cursor.execute(sql)
    logger.info('added deleted dates {}'.format(datetime.now() - start))


def reset():
    try:
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE populator_history, website_resolvableobject')
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO website_resolvableobject SELECT * FROM populator_resolvableobjectmigration')
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS populator_resolvableobjectmigration')
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(e)
        logger.error('cache data exception')


def get_insert_history_and_update_sql():
    # Insert data changes into history table and update data in main table
    return """
         WITH updated AS (
           SELECT new.id as id, jsonb_diff_val(old.data, new.data) AS changed_data, new.data AS data
           FROM populator_resolvableobjectmigration as new
           INNER JOIN website_resolvableobject AS old on new.id = old.id and new.dataset_id = old.dataset_id
           WHERE jsonb_diff_val(old.data, new.data) != '{}'
         ), updated_id AS (
             INSERT INTO populator_history(resolvable_object_id, changed_data, changed_date)
             SELECT id, changed_data, CURRENT_DATE
             FROM updated
         )
         UPDATE website_resolvableobject SET data = (SELECT data FROM updated)
         WHERE website_resolvableobject.id = (SELECT id FROM updated)
         """


def get_add_new_records_sql():
    # Add new records as darwincoreobjects, may be faster to use https://stackoverflow.com/questions/19363481/select-rows-which-are-not-present-in-other-table
    return """
        INSERT INTO website_resolvableobject(id, data, type, dataset_id, created_date)
        SELECT new.id, new.data, new.type, new.dataset_id, CURRENT_DATE
        FROM populator_resolvableobjectmigration AS new
        LEFT JOIN website_resolvableobject AS old ON new.id = old.id
        WHERE old.id IS NULL
        """
