import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This drops any existing tables so that we can execute the query. The list of tables dropped is in sql_queries.py We use cur and conn when accessing our PostGres SQL database'''
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error, not able to drop table'+ query)
            print(e)
    print("All of the tables have been successfully dropped")


   

def create_tables(cur, conn):
    '''This function creates the new staging and final tables: staging_events, staging_songs, songplays, users, songs, artists and time. We use cur and conn when accessing our PostGres SQL database. The list of created tables is found in sql_queries.py
    '''
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error, unable to create the tables' + query)
            print(e)
    print('All of the tables have been created')


def main():
    '''Combining the drop_tables and create_tables functions into one. This drops old tables and creates new ones'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()