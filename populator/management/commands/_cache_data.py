from django.db import connection
import logging


def merge_in_new_data(reset=False):
    if reset:
        reset()
        return

    with connection.cursor() as cursor:
        # Insert data changes into history table and update data in main table
        sql = """
              WITH updated AS (
                SELECT new.id as id, jsonb_diff_val(old.data, new.data) AS changed_data, new.data AS data
                FROM populator_resolvableobjectmigration as new
                INNER JOIN website_resolvableobject AS old on new.id = old.id
                WHERE jsonb_diff_val(old.data, new.data) != '{}'
              ), updated_id AS (
                  INSERT INTO populator_history(resolvable_object_id, changed_data, changed_date)
                  SELECT id, changed_data, CURRENT_DATE
                  FROM updated
              )
              UPDATE website_resolvableobject SET data = (SELECT data FROM updated)
              WHERE website_resolvableobject.id = (SELECT id FROM updated)
              """
        cursor.execute(sql)

    with connection.cursor() as cursor:
        # Add new records as darwincoreobjects, may be faster to use https://stackoverflow.com/questions/19363481/select-rows-which-are-not-present-in-other-table
        sql = """
        INSERT INTO website_resolvableobject(id, data, type, dataset_id, created_date)
        SELECT new.id, new.data, new.type, new.dataset_id, CURRENT_DATE
        FROM populator_resolvableobjectmigration AS new
        LEFT JOIN website_resolvableobject AS old ON new.id = old.id
        WHERE old.id IS NULL
        """
        cursor.execute(sql)

    with connection.cursor() as cursor:
        # Add deleted date
        sql = """
        UPDATE website_resolvableobject
            SET deleted_date = CURRENT_DATE
        WHERE id IN (
            SELECT old.id
            FROM website_resolvableobject AS old
            LEFT JOIN populator_resolvableobjectmigration AS new ON new.id = old.id
            WHERE new.id IS NULL AND old.deleted_date IS NULL)
        """
        cursor.execute(sql)


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
