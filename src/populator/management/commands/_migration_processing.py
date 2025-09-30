from django.db import connection, transaction, utils
from zipfile import ZipFile, BadZipFile
from datetime import datetime, timedelta
import psycopg2 as p
import re
import logging
import os
import requests
import traceback


DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1422496265375059969/_2rkGt-ZgLfGSvOSVKufwVuCdO5Pp2F2wwJ4q8dHNEe8qriH_TW0qzwdNn3rO_C0Guzp"


def send_discord_error(dataset_id, error_message, error_type="Migration Error"):
    """Send error notification to Discord webhook"""
    try:
        embed = {
            "title": f"🚨 {error_type}",
            "description": f"**Dataset ID:** `{dataset_id}`\n**Error:** {error_message}",
            "color": 15158332,  # Red color
            "timestamp": datetime.now().isoformat(),
            "footer": {
                "text": "Resolver Migration System"
            }
        }
        
        payload = {
            "embeds": [embed]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code != 204:
            logging.getLogger(__name__).warning(f"Discord webhook failed: {response.status_code}")
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to send Discord notification: {e}")


def get_dataset_id(zip_file_location):
    with ZipFile(zip_file_location) as zf:
        with zf.open('eml.xml') as f:
            eml = str(f.read())
            uuid_regex = '[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
            return re.search(r'alternateIdentifier>(' + uuid_regex + ')</alternateIdentifier', eml).group(1)


def import_dwca(dataset_id, zip_file_location='/tmp/tmp.zip'):
    logger = logging.getLogger(__name__)
    supported_files = ['event.txt', 'occurrence.txt', 'taxon.txt', 'measurementorfact.txt']
    count = 0
    try:
        with ZipFile(zip_file_location) as zf:  #
            logger.info(zf.namelist())
            valid_files = [file_name.replace('.txt', '') for file_name in zf.namelist() if file_name in supported_files]
            core = get_core(valid_files)
            for file_name in valid_files:
                with zf.open(f'{file_name}.txt') as f:
                    logger.info('about to import ' + file_name)
                    now = datetime.now()
                    columns = get_columns(f.readline())
                    create_temp_table(columns)
                    logger.info(f'created empty temp table with columns: {columns}')
                    try:
                        import_file(f)
                    except p.errors.CharacterNotInRepertoire as e:
                        error_msg = f"Character encoding error in file {file_name}: {str(e)}"
                        logger.error(error_msg)
                        send_discord_error(dataset_id, error_msg, "Character Encoding Error")
                        continue
                    except p.errors.BadCopyFileFormat as e:
                        error_msg = f"Bad copy file format in file {file_name}: {str(e)}"
                        logger.error(error_msg)
                        send_discord_error(dataset_id, error_msg, "File Format Error")
                        continue
                    logger.info(f'fin copy from stdin, took {datetime.now() - now}')
                    now = datetime.now()

                    if not sync_id_column(get_id(file_name), get_id(core)):
                        error_msg = f"Could not sync ID column for file {file_name}"
                        logger.error(error_msg)
                        send_discord_error(dataset_id, error_msg, "ID Column Sync Error")
                        return 0

                    purlfriendly_id_columns()
                    record_duplicates(dataset_id, file_name)
                    remove_duplicates()
                    logger.info(f'fin get duplicates, took {datetime.now() - now}')
                    now = datetime.now()
                    create_index()
                    logger.info(f'fin creating index {count}, took {datetime.now() - now}')
                    now = datetime.now()
                    count += insert_json_into_migration_table(dataset_id, file_name)
                    logger.info(f'fin inserted {count}, took {datetime.now() - now}')
    except BadZipFile:
        error_msg = f"Bad zip file for dataset {dataset_id}"
        logger.error(error_msg)
        send_discord_error(dataset_id, error_msg, "Bad Zip File Error")
        return 0
    except Exception as e:
        error_msg = f"Unexpected error processing dataset {dataset_id}: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        send_discord_error(dataset_id, error_msg, "Unexpected Error")
        return 0
    finally:
        # Clean up temp table in case of any unexpected failures
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp')

    return count


def get_columns(firstline):
    return firstline.decode("utf-8").rstrip().lower().split('\t')


def import_file(f):
    with connection.cursor() as cursor:
        cursor.copy_from(file=f, table='temp', null="")


def create_temp_table(columns):
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);')


def get_core(file_list):
    for type in ['event', 'occurrence', 'taxon']:
        for file in file_list:
            if file == type:
                return type
    return False

def get_id(file_type):
    ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid', 'materialsample': 'materialsampleid'}
    try:
        return ID_MAPPINGS[file_type]
    except KeyError as e:
        logger = logging.getLogger(__name__)
        error_msg = f'Key error for core type {file_type}: {e}'
        logger.error(error_msg)
        send_discord_error('unknown', error_msg, "Core Type Key Error")
        return False


def sync_id_column(id_column, core_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE temp ADD COLUMN parent text")
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE temp ADD COLUMN id text")
            elif core_id and core_id != id_column:
                cursor.execute("UPDATE temp SET parent = id")  # So we do not lose the core ids
            cursor.execute("UPDATE temp SET id = %s" % id_column)
        with connection.cursor() as cursor:  # Some NHM datasets have purl IDs in materialsampleid
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='materialsampleid';")
            if cursor.fetchone()[0] == 1:
                cursor.execute("UPDATE temp SET materialsampleid = REPLACE(materialsampleid, 'http://purl.org/nhmuio/id/', '')")
                # Sometimes there are multiple UUIDs in one column, when one specimen is spread over several sheets. This is a mistake, but nothing can be done now. Just make the first one work.
                cursor.execute("UPDATE temp SET materialsampleid = REGEXP_REPLACE(materialsampleid, '^([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}).+', '\\1')")
                # Sometimes there are duplicate uuids in materialsampleid for multiple records when we have 2 or more specimens on 1 sheet. This is a mistake, but cannot be fixed now. Do not resolve any of these.
                duplicates = 'SELECT materialsampleid FROM temp GROUP BY materialsampleid HAVING count(id) > 1'
                cursor.execute("UPDATE temp SET materialsampleid = '' WHERE materialsampleid IN (" + duplicates + ")")
                # Finally, put materialsampleid in the id column if they look like they are valid
                cursor.execute("UPDATE temp SET id = materialsampleid WHERE materialsampleid ~ '[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'")
        with connection.cursor() as cursor:  # Force all IDs to lowercase
            cursor.execute("UPDATE temp SET id = LOWER(id)")
            cursor.execute("UPDATE temp SET parent = LOWER(parent)")
    except p.errors.UndefinedColumn:
        logger = logging.getLogger(__name__)
        error_msg = 'Undefined column error in sync_id_column'
        logger.error(error_msg)
        send_discord_error('unknown', error_msg, "Database Column Error")
        return False
    except utils.ProgrammingError as e:
        logger = logging.getLogger(__name__)
        error_msg = f'Programming error in sync_id_column: {e}'
        logger.error(error_msg)
        send_discord_error('unknown', error_msg, "Database Programming Error")
        return False
    return True


def purlfriendly_id_columns():  # PURL breaks when there is a ":" in the URL
    with connection.cursor() as cursor:
        cursor.execute("UPDATE temp SET id = REPLACE(id, 'urn:uuid:', '')")
        cursor.execute("UPDATE temp SET parent = REPLACE(parent, 'urn:uuid:', '')")
    with connection.cursor() as cursor:
        cursor.execute("UPDATE temp SET id = REPLACE(id, 'http://purl.org/nhmuio/id/', '')")
        cursor.execute("UPDATE temp SET parent = REPLACE(parent, 'http://purl.org/nhmuio/id/', '')")


def add_dataset_id(dataset_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='datasetid';")
        if cursor.fetchone()[0] == 0:
            cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
            cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)


def record_duplicates(dataset_id, core_type, file='/srv/duplicates.txt'):
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
        make_json_sql = (f"SELECT temp.id, json_strip_nulls(row_to_json(temp)), '{dataset_id}', '{core_type}', temp.parent"
                         f" FROM temp ORDER BY temp.id LIMIT {step} OFFSET {i};")
        insert_sql = 'INSERT INTO populator_resolvableobjectmigration(id, data, dataset_id, type, parent) ' + make_json_sql
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
