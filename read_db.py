from ast import Raise
from unicodedata import name
import pyodbc
import pandas as pd
# import mysql.connector
# import pymysql.cursors
import connectorx as cx
from dateutil.relativedelta import relativedelta
import datetime as dt


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
        self.connection="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS"
    
    def get_adres(self,_from):
        df = cx.read_sql(self.connection,"SELECT AD.ZipCode from dbo.{} as ITB, dbo.Addresses as AD where AD.Id=ITB.VisitAddressId".format(_from,id))
        return df

    def join_addresses(self,target):
        df=cx.read_sql(self.connection,"SELECT TS.Id,AD.ZipCode from dbo.{} as EMP, dbo.Addresses as AD, TimeSlots as TS where EMP.id=TS.EmployeeId and EMP.VisitAddressId=AD.Id and TS.TimeSlotType=0".format(target))
        return df

    def get_timeslots_info(self):
        df = cx.read_sql(self.connection,"""SELECT DISTINCT TS.Id, TS.EmployeeID, TS.RelationID, TS.FromUtc, TS.UntilUtc, EC.FromUtc as ContractFrom, EC.UntilUtc as ContractUntil, EC.AverageNumberOfHoursPerMonth, EC.NumberOfHoursPerWeek, 
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM InvalidEmployeeRelationCombinations AS IERC 
                                                            WHERE IERC.EmployeeID = TS.EmployeeId 
                                                            AND IERC.RelationId = TS.RelationId 
                                                            AND TS.UntilUtc > IERC.CreatedOnUtc) 
                                                            THEN 1 ELSE 0 END AS "ClientMismatch",
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM EmployeeCharacteristics AS EC, RelationCharacteristics AS RC
                                                            WHERE TS.EmployeeId = EC.EmployeeId
                                                            AND TS.RelationId = RC.RelationId
                                                            AND TS.TimeSlotType = 0
                                                            AND TS.UntilUtc > RC.CreatedOnUTC
                                                            AND EC.CharacteristicId = 2 AND RC.CharacteristicId = 21)
                                                            THEN 1 ELSE 0 END AS "DogAllergyMismatch", 
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM EmployeeCharacteristics AS EC, RelationCharacteristics AS RC
                                                            WHERE TS.EmployeeId = EC.EmployeeId
                                                            AND TS.RelationId = RC.RelationId
                                                            AND TS.TimeSlotType = 0
                                                            AND TS.UntilUtc > RC.CreatedOnUTC
                                                            AND EC.CharacteristicId = 3 AND RC.CharacteristicId = 27)
                                                            THEN 1 ELSE 0 END AS "CatAllergyMismatch", 
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM EmployeeCharacteristics AS EC, RelationCharacteristics AS RC
                                                            WHERE TS.EmployeeId = EC.EmployeeId
                                                            AND TS.RelationId = RC.RelationId
                                                            AND TS.TimeSlotType = 0
                                                            AND TS.UntilUtc > RC.CreatedOnUTC
                                                            AND EC.CharacteristicId = 4 AND RC.CharacteristicId = 33)
                                                            THEN 1 ELSE 0 END AS "OtherPetsAllergyMismatch", 
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM EmployeeCharacteristics AS EC, RelationCharacteristics AS RC
                                                            WHERE TS.EmployeeId = EC.EmployeeId
                                                            AND TS.RelationId = RC.RelationId
                                                            AND TS.TimeSlotType = 0
                                                            AND TS.UntilUtc > RC.CreatedOnUTC
                                                            AND EC.CharacteristicId = 5 AND RC.CharacteristicId = 37)
                                                            THEN 1 ELSE 0 END AS "SmokeAllergyMismatch"
                                            FROM TimeSlots AS TS, Employments as EM, EmployeeContracts as EC
                                            WHERE TS.TimeSlotType = 0 
                                            AND TS.EmployeeId = EM.EmployeeId 
                                            AND EM.Id = EC.EmploymentId 
                                            AND TS.UntilUtc >= EC.FromUtc 
                                            AND (TS.UntilUtc <= EC.UntilUtc OR EC.UntilUtc IS NULL)
                                            ORDER BY TS.UntilUtc""")
        return df

    def get_data(self,get_var,_from):
        df = cx.read_sql(self.connection,"SELECT {} from dbo.{}".format(get_var,_from))
        return df

    def close_conn(self):
        self.conn.close()