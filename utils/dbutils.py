#!/usr/bin/env python
import os
import sys
import re
from typing import Tuple
from collections import defaultdict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.inspection import inspect

import logging

logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')

sys.path.append(os.path.join(os.path.dirname(__file__)))


class BaseSQLConnector:
    """
        Base class for SQL connectors
    """
    
    def __init__(self) -> None:
        self.var = None
    
    def _create_conn_str(self, configs: dict, db: str = "pgsql") -> str:
        """
            Create configuration from a dictioney
            :params configs: Configuration data as dictionery
            :params db: Database type. Supported DB type.
             Supported values PostgreSQL -> pgsql
            :returns conn_str: Connection string
        """
        pgsql_str = "postgresql+psycopg2://{0}:{1}@{2}/{3}"
        
        conn_str = None
        
        if db == "pgsql":
            conn_str = pgsql_str.format(configs.get('user_name'),
                                        configs.get('passwd'),
                                        configs.get('host'),
                                        configs.get('database'))
        
        return conn_str
    


class PgSQLConnector(BaseSQLConnector):
    """
    PostgreSQL Connctor with context manager examples
    ADopted from 
    https://medium.com/@ramojol/python-context-managers-and-the-with-statement-8f53d4d9f87
    """
    
   
    def __init__(self,config_data: dict) -> None:
        super().__init__()
        self.conn_str = self._create_conn_str(config_data,
                                               db='pgsql')
        self.db_session = None
        self.engine = None

    
    def __enter__(self):
        db_engine = create_engine(self.conn_str)
        self.engine = db_engine
        DBSession = sessionmaker()

        self.db_session = DBSession(bind=db_engine)
        return self

    def __exit__(self,exec_type,exec_val,exec_tb):
        self.db_session.close()



# class MySQLConnector(BaseSQLConnector):
#     raise NotImplementedError

# class MariaDBConnector(BaseSQLConnector):
#     raise NotImplementedError

# class HiveConnector(BaseSQLConnector):
#     raise NotImplementedError

# class ImpalaConnector(BaseSQLConnector):
#     raise NotImplementedError


def alch_query_to_dict(query_res: Query) -> dict:
    """
        Convert SQL Alcheny resuts to a Python dictionary
        This is excellent for using with pandas DataFrame
        :params query_res: Query result object
        :returns documents: data as Dictionary
        
        Reference - https://gist.github.com/garaud/bda10fa55df5723e27da
    """
    documents = defaultdict(list)
    
    for qobj in query_res:
        import pdb; pdb.set_trace()
        res_instance = inspect(qobj)
        for key,value in res_instance.attrs.items():
            documents[key].append(value.value)
    
    return documents
    

def parse_integrity_err(err_msg: str) -> Tuple[str,str]:
    """
        Parse the integrity error from the SQLAlchemy
        IntegrityError message_detail. The message_detail
        contains information such as ['column_name', 'constraint_name',
        'context', 'datatype_name', 'internal_position', 
        'internal_query', 'message_detail', 'message_hint', 
        'message_primary', 'schema_name', 'severity', 
        'source_file', 'source_function', 'source_line', 
        'sqlstate', 'statement_position', 'table_name']
        #REFERENCE - https://stackoverflow.com/questions/21540702/get-the-constraint-name-out-of-an-integrityerror-in-sqlalchemypostgres
        
        The message_detail contains information about 
        Which column caused the primary key error and the
        First value SQLAlchemy tried to insert,
        :param err_msg: Error message (message_detail)
        :returns pk_column: Primary key column name
        :returns pk_val: Primary key value (first one)
    """

    message_regex = r"(\(.*\))=(\(.*\))"

    pk_column = None
    pk_val = None

    pk_column_val_details = re.findall(message_regex,
                                       err_msg,
                                       re.MULTILINE)

    try:
        pk_column = pk_column_val_details[0][0]
        pk_column = pk_column.replace("(",'')
        pk_column = pk_column.replace(")",'')
        pk_val = pk_column_val_details[0][1]
        pk_val = pk_val.replace("(",'')
        pk_val = pk_val.replace(")",'')
    except Exception as err:
        logging.info("Error in processing the message {0}".format(err))


    return (pk_column, pk_val)

if __name__ == "__main__":
    connection_conf = {'user_name':'sweng',
                      'passwd':'*****',
                      'host':'myhost',
                      'database':'mydb'}

    with PgSQLConnector(connection_conf) as db:
        print(db.db_session)