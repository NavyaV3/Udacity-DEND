import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads each staging table using the queries in `copy_table_queries` list.
        :param cur: cursor connection object on Redshift
        :param conn: connection object on Redshift
    """
    
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into each table using the queries in `insert_table_queries` list.
        :param cur: cursor connection object on Redshift
        :param conn: connection object on Redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
     """
    - Reads AWS details from config file 
    
    - Establishes connection with the Redshift cluster.  
    
    - Loads data into staging tables.  
    
    - Inserts data in all the tables. 
    
    - Finally, closes the connection. 
    
    :param cur: cursor connection object on Redshift
    :param conn: connection object on Redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()