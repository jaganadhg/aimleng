#!/usr/bin/env python
import os 
import sys 
import pdb
from operator import methodcaller
from sqlalchemy import select
import pandas as pd

from utils import PgSQLConnector
from models import ElectricityData
from utils import alch_query_to_dict

def query_check(config: dict) -> None:
    
    with PgSQLConnector(config) as sql:
        pdb.set_trace()
        recs = sql.db_session.query(ElectricityData).limit(5)
        
        for res in recs:
            print(res)
        #print(f"Date Time prop is {recs.date_time}")
        print(f"Hybrid wroks!!!")

def check_df(config: dict) -> None:
    with PgSQLConnector(config) as sql:
        records = sql.db_session.query(ElectricityData).limit(100)
        get_data = methodcaller('get_data')
        all_data = list(map(get_data,records))
        
        data_set = pd.DataFrame(all_data)
        

if __name__ == "__main__":  
    connection_conf = {'user_name':'dseng',
                      'passwd':'jagan123',
                      'host':'10.0.0.239',
                      'database':'dseng'}
    #query_check(connection_conf)
    check_df(connection_conf)