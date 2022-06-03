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

    def get_data_from_date(self,get_var,_from,date_1,date_2):
        df = pd.read_sql_query("SELECT {} from dbo.{} where FromUtc between '{}' and '{}'".format(get_var,_from,date_1,date_2),con=self.conn)
        return df

    def get_contract_information_on_id(self,id):
        df = pd.read_sql_query("SELECT EC.AverageNumberOfHoursPerMonth from dbo.Employees E,dbo.Employments EMP, dbo.EmployeeContracts EC where EMP.Id=EC.EmploymentId and EMP.EmployeeId=E.Id and E.id={}".format(id),con=self.conn)
        return df

    def join_table(self,table_1,table_2,join_var1, join_var2):
        df = pd.read_sql_query("SELECT * from dbo.{} JOIN dbo.{} ON dbo.{}.{} = ".format(table_1,table_2,table_1,join_var1,table_2,join_var2),con=self.conn)
        return df

    def close_conn(self):
        self.conn.close()

