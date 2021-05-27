from django.db import connection, transaction, utils
from zipfile import ZipFile, BadZipFile
from datetime import datetime, timedelta
import psycopg2 as p
import re
import logging
import os


def get_dataset_id(zip_file_location):
    with ZipFile(zip_file_location) as zf:
        with zf.open('eml.xml') as f:
            eml = str(f.read())
            uuid_regex = '[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
            return re.search(r'alternateIdentifier>(' + uuid_regex + ')</alternateIdentifier', eml).group(1)


def import_dwca(dataset_id, zip_file_location='/tmp/tmp.zip'):
    logger = logging.getLogger(__name__)
    supported_cores = ['occurrence.txt', 'event.txt', 'taxon.txt']
    count = 0
    try:
        with ZipFile(zip_file_location) as zf:  #
            logger.info(zf.namelist())
            for file_name in [file_name for file_name in zf.namelist() if file_name in supported_cores]:
                with zf.open(file_name) as f:
                    logger.info('about to import ' + file_name)
                    now = datetime.now()
                    create_temp_table(get_columns(f.readline()))
                    try:
                        import_file(f)
                    except p.errors.CharacterNotInRepertoire as e:
                        logger.error(e)
                        logger.error('file_name')
                        continue
                    except p.errors.BadCopyFileFormat as e:
                        logger.error(e)
                        logger.error('file_name')
                        continue
                    logger.info('fin copy from stdin, took {}'.format(str(datetime.now() - now)))
                    now = datetime.now()

                    core_type = re.sub('\.txt$', '', file_name)
                    if not sync_id_column(get_core_id(core_type)):
                        logger.info('Warning: could not sync id column for {}'.format(core_type))
                        return 0

                    purlfriendly_id_column()
                    record_duplicates(dataset_id, core_type)
                    remove_duplicates()
                    logger.info('fin get duplicates, took {}'.format(datetime.now() - now))
                    now = datetime.now()
                    create_index()
                    logger.info('fin creating index {}, took {}'.format(count, datetime.now() - now))
                    now = datetime.now()
                    count += insert_json_into_migration_table(dataset_id, core_type)
                    logger.info('fin inserted {}, took {}'.format(count, datetime.now() - now))
    except BadZipFile:
        logger.error('Bad zip')
        return 0

    return count


def import_event(dataset_id, event_file, occurrence_file, extensions):
    # Copy event to temp, set eventid, import into separate table (event table?)
    # Import occurrence as usual, set parent_id = eventid
    # Import extensions (e.g. mof, ??), into extensions, parent_id
    pass


def get_columns(firstline):
    return firstline.decode("utf-8").rstrip().lower().split('\t')


def import_file(f):
    with connection.cursor() as cursor:
        cursor.copy_from(file=f, table='temp', null="")


def create_temp_table(columns):
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);')


def get_core_id(core_type):
    CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
    try:
        return CORE_ID_MAPPINGS[core_type]
    except KeyError as e:
        logger = logging.getLogger(__name__)
        logger.error('Key error for core type {}'.format(core_type))
        return False


def sync_id_column(id_column):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE temp ADD COLUMN id text")
            cursor.execute("UPDATE temp SET id = %s" % id_column)
        with connection.cursor() as cursor:  # Some NHM datasets have purl IDs in othercatalognumbers
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='othercatalognumbers';")
            if cursor.fetchone()[0] == 1:
                cursor.execute("UPDATE temp SET othercatalognumbers = REPLACE(othercatalognumbers, 'http://purl.org/nhmuio/id/', '')")
                # Sometimes there are multiple UUIDs in one column, when one specimen is spread over several sheets. This is a mistake, but nothing can be done now. Just make the first one work.
                cursor.execute("UPDATE temp SET othercatalognumbers = REGEXP_REPLACE(othercatalognumbers, '^([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}).+', '\\1')")
                # Sometimes there are duplicate uuids in othercatalognumbers for multiple records when we have 2 or more specimens on 1 sheet. This is a mistake, but cannot be fixed now. Do not resolve any of these.
                duplicates = 'SELECT othercatalognumbers FROM temp GROUP BY othercatalognumbers HAVING count(id) > 1'
                cursor.execute("UPDATE temp SET othercatalognumbers = '' WHERE othercatalognumbers IN (" + duplicates + ")")
                # Finally, put othercatalognumbers in the id column if they look like they are valid
                cursor.execute("UPDATE temp SET id = othercatalognumbers WHERE othercatalognumbers ~ '[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'")
    except p.errors.UndefinedColumn:
        logger = logging.getLogger(__name__)
        logger.error('Undefined column')
        return False
    except utils.ProgrammingError:
        logger = logging.getLogger(__name__)
        logger.error('Programming error')
        return False
    return True


def purlfriendly_id_column():  # PURL breaks when there is a ":" in the URL
    with connection.cursor() as cursor:
        cursor.execute("UPDATE temp SET id = REPLACE(id, 'urn:uuid:', '')")
    with connection.cursor() as cursor:
        cursor.execute("UPDATE temp SET id = REPLACE(id, 'http://purl.org/nhmuio/id/', '')")


def add_dataset_id(dataset_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='datasetid';")
        if cursor.fetchone()[0] == 0:
            cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
            cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)


def record_duplicates(dataset_id, core_type, file='/code/duplicates.txt'):
    with connection.cursor() as cursor:
        query = ("""
        SELECT 
            temp.id AS id, 
            row_to_json(temp) AS new_data, 
            '{0}' AS new_datasetid, 
            '{1}' AS new_core_type, 
            j.data AS old_data, 
            j.dataset_id AS old_datasetid
        FROM temp 
        LEFT JOIN populator_resolvableobjectmigration AS j ON j.id = temp.id
        WHERE j.id IS NOT NULL
        """).format(dataset_id, core_type)
        outputquery = "COPY ({0}) TO STDOUT (DELIMITER '|')".format(query)

        with open(file, 'a') as f:
            cursor.copy_expert(outputquery, f)


def remove_duplicates():
    with connection.cursor() as cursor:
        select = 'SELECT temp.id FROM temp LEFT JOIN populator_resolvableobjectmigration as j on j.id = temp.id ' \
                 'WHERE j.id IS NOT NULL'
        query = 'DELETE FROM temp WHERE temp.id IN ({})'.format(select)
        cursor.execute(query)


def get_temp_count():
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM temp;')
        return cursor.fetchone()[0]


def insert_json_into_migration_table(dataset_id, core_type, step=300000):
    count = 0
    temp_count = get_temp_count()
    _max = step if temp_count <= step else temp_count + step
    for i in range(0, _max, step):
        logger = logging.getLogger(__name__)
        logger.info('Migration loop: {}'.format(i))
        make_json_sql = """SELECT temp.id, json_strip_nulls(row_to_json(temp)), '{0}', '{1}'
                           FROM temp ORDER BY temp.id LIMIT {2} OFFSET {3};""".format(dataset_id, core_type, step, i)
        insert_sql = 'INSERT INTO populator_resolvableobjectmigration(id, data, dataset_id, type) ' + make_json_sql
        with connection.cursor() as cursor:
            cursor.execute(insert_sql)
            count += cursor.rowcount
        #db = create_keepalive_connection()
        #with db.cursor() as cursor:
        #    cursor.execute(insert_sql)
        #    count += cursor.rowcount
        #db.close()
    return count


def create_index():
    #db = create_keepalive_connection()
    #with db.cursor() as cursor:
    #    cursor.execute('CREATE INDEX idx_id ON temp(id)')
    #db.close()
    with connection.cursor() as cursor:
        cursor.execute('CREATE INDEX idx_id ON temp(id)')


def create_keepalive_connection():
    db = p.connect(dbname=connection.settings_dict['NAME'],
                   user=connection.settings_dict['USER'],
                   password=connection.settings_dict['PASSWORD'],
                   host=connection.settings_dict['HOST'],
                   port=connection.settings_dict['PORT'],
                   keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    return db
