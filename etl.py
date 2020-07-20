# -*- coding: utf-8 -*-
"""
Created on Fri Jun 13 2020

@author: gari.ciodaro.guerra

Set of functions to connect to AWS
redshift. Loads data from S3, staging it
in redshift. Execute insert statement 
defined in sql_queries.py
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """ ETL. Executes and commit from 
    AWS S3 data to Redshift staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """ Executes and commit insert statements to Redshift
    tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Connects to AWS Redshift. call functions
    load_staging_tables and insert_tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    strig_conn="host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(strig_conn.format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()