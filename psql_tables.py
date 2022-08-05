import psycopg2 as psycopg2
import datetime
import csv

# local import
import config as cfg


DATA_TIME = datetime.datetime.now()


def create_connection_to_psql(host, port, sslmode, dbname, user, password):
    conn = psycopg2.connect(f"""
            host={host}
            port={port}
            sslmode={sslmode}
            dbname={dbname}
            user={user}
            password={password}
            target_session_attrs=read-write
            """)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print(f'[INFO] : {DATA_TIME} >> Server version : \n{cur.fetchone()}')
        return conn


def create_offense_codes_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.SSLMODE,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
    # create table
        with conn.cursor() as cur:
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {cfg.TAB_OFFENSE_CODES}(
            code varchar, 
            name varchar 
            );"""
            cur.execute(sql_query)
            print(f'[INFO] : {DATA_TIME} >> Table created successful')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


def create_crime_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.SSLMODE,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
    # create table
        with conn.cursor() as cur:
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {cfg.TAB_CRIME}(
            incident_number varchar,
            offense_code varchar,
            offense_code_group varchar,
            offense_description varchar,
            district varchar,
            reporting_area varchar,
            shooting varchar,
            occurred_on_date varchar,
            year varchar,
            month varchar,
            day_of_week varchar,
            hour varchar,
            ucr_part varchar,
            street varchar,
            lat varchar,
            long varchar,
            location varchar
            );"""
            cur.execute(sql_query)
            print(f'[INFO] : {DATA_TIME} >> Table created successful')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


def insert_data_to_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.SSLMODE,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            with open(cfg.CSV_OFFENSE_CODES, newline='', encoding="ISO-8859-1") as csvfile:
                spamreader = csv.reader(csvfile)
                for row in spamreader:
                    sql_query = f"""
                        INSERT INTO {cfg.TAB_OFFENSE_CODES} (code, name)
                        VALUES('%s', '%s', );""" % (row[0], row[1])
                    cur.execute(sql_query)
            print(f'''[INFO] : {DATA_TIME} >> Data with offense codes from <'offense_codes.csv'> inserted in the table <{cfg.TAB_OFFENSE_CODES}>''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


def insert_data_to_crime_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.SSLMODE,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            with open(cfg.CSV_CRIME, newline='', encoding="ISO-8859-1") as csvfile:
                spamreader = csv.reader(csvfile)
                for row in spamreader:
                    sql_query = f"""
                        INSERT INTO {cfg.TAB_CRIME} (
                        incident_number,offense_code,offense_code_group,
                        offense_description,district,reporting_area,
                        shooting,occurred_on_date,year,month,day_of_week,
                        hour,ucr_part,street,lat,long,location)
                        VALUES('%s', '%s', '%s', '%s', '%s', 
                        '%s', '%s', '%s', '%s', '%s', 
                        '%s', '%s', '%s', '%s', '%s', 
                        '%s', '%s');""" \
                                % (row[0], row[1], row[2],  row[3],  row[4],
                                   row[5],  row[6],  row[7],  row[8],  row[9],
                                   row[10],  row[11],  row[12],  row[13],  row[14],
                                   row[15],  row[16])
                    # upload only 851 from 7000 lines of csv file
                    cur.execute(sql_query)
            print(f'''[INFO] : {DATA_TIME} >> Data with offense codes from <'offense_codes.csv'> inserted in the table <{cfg.TAB_OFFENSE_CODES}>''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')





# create_offense_codes_table()
# insert_data_to_table()
# create_crime_table()
# insert_data_to_crime_table()

if __name__ == '__main__':
    print('')
