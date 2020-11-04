from django.db import connection, transaction, utils
from zipfile import ZipFile, BadZipFile
from datetime import datetime
import psycopg2 as p
import re
import logging


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
                    logger.info(now)
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
                    logger.info('fin')
                    logger.info(datetime.now() - now)

                    core_type = re.sub('\.txt$', '', file_name)
                    if not sync_id_column(get_core_id(core_type)):
                        return 0
                    get_duplicates(dataset_id, core_type)
                    count += insert_json_into_migration_table(dataset_id, core_type)
                    logger.info('inserted {}'.format(count))
    except BadZipFile:
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
        cursor.copy_expert(sql="COPY temp FROM stdin DELIMITER AS '\t'", file=f)


def create_temp_table(columns):
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);')


def get_core_id(core_type):
    print(core_type)
    CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
    try:
        return CORE_ID_MAPPINGS[core_type]
    except KeyError as e:
        return False


def sync_id_column(id_column):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE temp ADD COLUMN id text")
            cursor.execute("UPDATE temp SET id = %s" % id_column)
    except p.errors.UndefinedColumn:
        return False
    except utils.ProgrammingError:
        return False
    return True


def add_dataset_id(dataset_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='datasetid';")
        if cursor.fetchone()[0] == 0:
            cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
            cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)


def get_duplicates(dataset_id, core_type, file='/code/duplicates.txt'):
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


def insert_json_into_migration_table(dataset_id, core_type):
    make_json_sql = """SELECT temp.id, row_to_json(temp), '{0}', '{1}'
                       FROM temp LEFT JOIN populator_resolvableobjectmigration AS j ON j.id = temp.id
                       WHERE j.id IS NULL;""".format(dataset_id, core_type)
    insert_sql = 'INSERT INTO populator_resolvableobjectmigration(id, data, dataset_id, type) ' + make_json_sql
    with connection.cursor() as cursor:
        cursor.execute(insert_sql)
        return cursor.rowcount

