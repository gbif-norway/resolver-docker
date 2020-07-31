from django.db import connection

def _create_jsonb_diff_function_in_postgres():
    with connection.cursor() as cursor:
        sql = ("CREATE OR REPLACE FUNCTION jsonb_diff_val(val1 JSONB,val2 JSONB)"
                " RETURNS JSONB AS $$"
                " DECLARE"
                "   result JSONB;"
                "   v RECORD;"
                " BEGIN"
                "    result = val1;"
                "    FOR v IN SELECT * FROM jsonb_each(val2) LOOP"
                "      IF result @> jsonb_build_object(v.key,v.value)"
                "         THEN result = result - v.key;"
                "      ELSIF result ? v.key THEN CONTINUE;"
                "      ELSE"
                "         result = result || jsonb_build_object(v.key, NULL);"
                "      END IF;"
                "    END LOOP;"
                "    RETURN result;"
                " END;"
                " $$ LANGUAGE plpgsql;")
        cursor.execute(sql)

def merge_in_new_data():
    _create_jsonb_diff_function_in_postgres()

    with connection.cursor() as cursor:
        # Insert data changes into history table and update data in main table
        sql = """
              WITH updated AS (
                  SELECT
                  new.id AS uuid, jsonb_diff_val(old.data, new.data) AS changed_data, new.data as data
                  FROM replacement_table AS new
                  INNER JOIN website_darwincoreobject AS old ON new.id = old.id
                  WHERE jsonb_diff_val(old.data, new.data) != '{}'
              ), updated_id AS (
                  INSERT INTO website_history(darwin_core_object_id, changed_data, changed_date)
                  SELECT uuid, changed_data, CURRENT_DATE FROM updated
              )
              UPDATE website_darwincoreobject SET data = (SELECT data FROM updated)
              WHERE website_darwincoreobject.id = (SELECT uuid FROM updated)
              """
        cursor.execute(sql)

    with connection.cursor() as cursor:
        # Add new records as darwincoreobjects, may be faster to use https://stackoverflow.com/questions/19363481/select-rows-which-are-not-present-in-other-table
        sql = """
        INSERT INTO website_darwincoreobject(id, data, created_date)
        SELECT new.id, new.data, CURRENT_DATE
        FROM replacement_table AS new
        LEFT JOIN website_darwincoreobject AS old ON new.id = old.id
        WHERE old.id IS NULL
        """
        cursor.execute(sql)

    with connection.cursor() as cursor:
        # Add deleted date
        sql = """
        UPDATE website_darwincoreobject
            SET deleted_date = CURRENT_DATE
        WHERE id IN (
            SELECT old.id
            FROM website_darwincoreobject AS old
            LEFT JOIN replacement_table AS new ON new.id = old.id
            WHERE new.id IS NULL AND old.deleted_date IS NULL)
        """
        cursor.execute(sql)

def reset():
    try:
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE website_history, website_darwincoreobject')
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO website_darwincoreobject SELECT * FROM replacement_table')
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS replacement_table')
    except Exception as e:
        print(e)
        import pdb; pdb.set_trace()

