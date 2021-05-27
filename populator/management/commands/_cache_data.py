from django.db import connection
import logging
from website.models import Dataset, ResolvableObject
from populator.models import ResolvableObjectMigration
from datetime import date, datetime


def sync_datasets(migration_dataset_ids):
    #  TODO what about datasets + records which get deleted then added again?
    Dataset.objects.exclude(id__in=migration_dataset_ids).update(deleted_date=date.today())
    ResolvableObject.objects.exclude(dataset__id__in=migration_dataset_ids).update(deleted_date=date.today())


def merge_in_new_data(reset=False, step=10000):
    logger = logging.getLogger(__name__)
    # if reset:
    #     reset()
    #     return
    start = datetime.now()
    with connection.cursor() as cursor:
        cursor.execute('select count(*) from populator_resolvableobjectmigration;')
        count = cursor.fetchone()[0]
    log_time(start, 'count complete')

    _max = step if count <= step else count + step
    #try:
    for i in range(0, _max, step):
        start = datetime.now()
        log_time(start, 'starting on offset {} and step {}'.format(i, step))
        create_temp_updated_table()
        log_time(start, 'created temp updated table')

        start = datetime.now()
        populate_temp_updated_table(offset=i, limit=step)
        log_time(start, 'populated temp updated table')

        start = datetime.now()
        insert_history()
        log_time(start, 'inserted history')

        start = datetime.now()
        update_website_resolvableobject()
        log_time(start, 'updated pre existing records')

    start = datetime.now()
    log_time(start, 'adding new records starting now')
    with connection.cursor() as cursor:
        cursor.execute(get_add_new_records_sql())

    log_time(start, 'added all new records')
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
    log_time(start, 'added deleted dates')
    #connection.close()
    #except:
    #    import pdb; pdb.set_trace()


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


def log_time(start, message):
    logger = logging.getLogger(__name__)
    time_string = datetime.now() - start
    logger.info('{}    - time taken - {}'.format(message, str(time_string)[:7]))


def create_temp_updated_table():
    with connection.cursor() as cursor:
        cursor.execute("""DROP TABLE IF EXISTS temp_updated;
                          CREATE TABLE temp_updated (id TEXT PRIMARY KEY, changed_data JSONB, data JSONB);""")


def populate_temp_updated_table(limit, offset):
    with connection.cursor() as cursor:
        cursor.execute("""INSERT INTO temp_updated(id, changed_data, data)
                          SELECT new.id as id, jsonb_diff_val(old.data, new.data) AS changed_data, new.data AS data
                          FROM
                            (SELECT * FROM populator_resolvableobjectmigration as new
                            ORDER BY new.id LIMIT {} OFFSET {}) AS new
                          INNER JOIN website_resolvableobject AS old
                              ON new.id = old.id AND new.dataset_id = old.dataset_id
                          WHERE jsonb_diff_val(old.data, new.data) != '{{}}'
                          ORDER BY new.id;""".format(limit, offset))


def insert_history():
    with connection.cursor() as cursor:
        cursor.execute("""INSERT INTO populator_history(resolvable_object_id, changed_data, changed_date)
                          SELECT id, changed_data, CURRENT_DATE
                          FROM temp_updated""")


def update_website_resolvableobject():
    with connection.cursor() as cursor:
        cursor.execute("""UPDATE website_resolvableobject 
                          SET data = temp_updated.data
                          FROM temp_updated
                          WHERE website_resolvableobject.id = temp_updated.id""")


def get_add_new_records_sql():
    # Add new records as darwincoreobjects, may be faster to use https://stackoverflow.com/questions/19363481/select-rows-which-are-not-present-in-other-table
    return """
        INSERT INTO website_resolvableobject(id, data, type, dataset_id, created_date)
        SELECT new.id, new.data, new.type, new.dataset_id, CURRENT_DATE
        FROM populator_resolvableobjectmigration AS new
        LEFT JOIN website_resolvableobject AS old ON new.id = old.id
        WHERE old.id IS NULL
        """

# https://stackoverflow.com/questions/56733112/how-to-create-new-database-connection-in-django
#connections.ensure_defaults('default')
#connections.prepare_test_settings('default')
#db = connections.databases['default']
#backend = load_backend(db['ENGINE'])
#return backend.DatabaseWrapper(db, 'default') #returns connection object, i.e. connection.cursor()