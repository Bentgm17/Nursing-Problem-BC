from ast import Raise
from unicodedata import name
import pyodbc
import pandas as pd
# import mysql.connector
# import pyodbc


class ExtractData():
    """
    A class used to get data from the postgresql dataframe containing the statistics of matches
    ...
    Attributes
    ----------
    conn : psycopg2.connect 
       The connection with the docker file
    Methods
    -------
    gen_ran_data(kwargs)
        Generate and return data on kwargs arguments
    data_on_playerid(player_id,args)
        Generates the property link
    data_on_matchid(player_id,args)
        Generates the property link
    """
    def __init__(self):
        """
        Parameters
        ----------
        url : str
            a formatted string which represents the related site link 
        adress : str
            a formatted string which represents the related adress
        """
        self.conn = pyodbc.connect(server='127.0.0.1,1401',
                                        driver='{ODBC Driver 17 for SQL Server}',
                                        database='qpz-florein-prod_bu_20220414-ANONYMOUS',
                                        user='SA',
                                        password='Assist2022')
    
    def check_read(self,**kwargs):
        df = pd.read_sql_query("SELECT TS.EmployeeId,TS.RelationId,TS.FromUtc,TS.UntilUtc,TS.RecurringTimeSlotDefinitionId,TSD.BulkUntilUtc from dbo.TimeSlots as TS,dbo.RecurringTimeSlotDefinitions as TSD where TSD.Id=TS.RecurringTimeSlotDefinitionId",con=self.conn)
        return df

    def get_data(self,get_var,_from):
        df = pd.read_sql_query("SELECT {} from dbo.{}".format(get_var,_from),con=self.conn)
        return df

    def close_conn(self):
        self.conn.close()

