# -*- coding: utf-8 -*-
"""
Created on Fri Jun 13 2020

@author: gari.ciodaro.guerra

Set of functions to connect to AWS
redshift execute create and drop
statement defined in sql_queries.py
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """ Executes and commit drop tables
    queries.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """ Executes and commit create tables
    queries.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Connects to AWS Redshift. call functions
    create_tables and drop_tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    string_con="host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(
        string_con.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()