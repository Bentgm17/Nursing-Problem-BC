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
    def __init__(self,source):
        """
        Parameters
        ----------
        url : str
            a formatted string which represents the related site link 
        adress : str
            a formatted string which represents the related adress
        """
        self.connection=source
    
    def get_adres(self,_from):
        df = cx.read_sql(self.connection,"SELECT AD.ZipCode from dbo.{} as ITB, dbo.Addresses as AD where AD.Id=ITB.VisitAddressId".format(_from,id))
        return df

    def join_addresses(self,target):
        choice={'Employees':'EmployeeId','Relations':'RelationId'}
        df=cx.read_sql(self.connection,"SELECT TS.Id,AD.ZipCode from dbo.{} as EMP, dbo.Addresses as AD, TimeSlots as TS where EMP.id=TS.{} and EMP.VisitAddressId=AD.Id and TS.TimeSlotType=0 and TS.FromUtc>='2020-02-07'".format(target,choice[target]))
        return df

    def get_timeslots_info(self):
        df = cx.read_sql(self.connection,"""SELECT DISTINCT TS.Id, TS.EmployeeID, TS.RelationID, TS.FromUtc, TS.UntilUtc, EC.FromUtc as ContractFrom, EC.UntilUtc as ContractUntil, EC.AverageNumberOfHoursPerMonth, EC.NumberOfHoursPerWeek, 
                                            CASE WHEN EXISTS(SELECT * 
                                                            FROM InvalidEmployeeRelationCombinations AS IERC 
                                                            WHERE IERC.EmployeeID = TS.EmployeeId 
                                                            AND IERC.RelationId = TS.RelationId 
                                                            AND TS.UntilUtc > IERC.CreatedOnUtc) 
                                                            THEN 1 ELSE 0 END AS "ClientMismatch"
                                            FROM TimeSlots AS TS, Employments as EM, EmployeeContracts as EC
                                            WHERE TS.TimeSlotType = 0 
                                            AND TS.FromUtc > '2020-02-07'
                                            AND TS.EmployeeId = EM.EmployeeId 
                                            AND EM.Id = EC.EmploymentId 
                                            AND TS.UntilUtc >= EC.FromUtc 
                                            AND (TS.UntilUtc <= EC.UntilUtc OR EC.UntilUtc IS NULL)
                                            ORDER BY TS.UntilUtc""")
        return df

    def retrieve_zipcodes(self):
        df = cx.read_sql(self.connection, """SELECT X.EmployeeZipCode, Y.RelationZipCode
                                                FROM (SELECT DISTINCT AD.ZipCodeNumberPart as RelationZipCode
                                                        FROM Relations as R, Addresses as AD
                                                        WHERE R.VisitAddressId IS NOT NULL
                                                        AND AD.ZipCodeNumberPart IS NOT NULL
                                                        AND R.VisitAddressId = AD.Id) as Y, 
                                                    (SELECT DISTINCT AD.ZipCodeNumberPart as EmployeeZipCode
                                                        FROM Employees as E, Addresses as AD
                                                        WHERE AD.ZipCodeNumberPart IS NOT NULL
                                                        AND E.VisitAddressId IS NOT NULL
                                                        AND E.VisitAddressId = AD.Id
                                                        AND (E.EmployementEndDateUtc >= '2020-02-07' OR E.EmployementEndDateUtc IS NULL)) as X""")
        return df

    def retrieve_employee_data(self):
        df = cx.read_sql(self.connection,"""SELECT E.Id as EmployeeId, AD.ZipCodeNumberPart as EmployeeZipCode, EC.NumberOfHoursPerWeek, EC.AverageNumberOfHoursPerMonth, EC.FromUtc as ContractFrom, EC.UntilUtc as ContractUntil, 
                                                    CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EmCh WHERE EmCh.CharacteristicId = 2 AND EmCh.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasDogAllergy", 
                                                    CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EmCh WHERE EmCh.CharacteristicId = 3 AND EmCh.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasCatAllergy", 
                                                    CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EmCh WHERE EmCh.CharacteristicId = 4 AND EmCh.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasOtherPetsAllergy", 
                                                    CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EmCh WHERE EmCh.CharacteristicId = 5 AND EmCh.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasSmokeAllergy"
                                                FROM Employees as E, Addresses as AD, Employments as EM, EmployeeContracts as EC
                                                WHERE E.VisitAddressId IS NOT NULL
                                                AND AD.ZipCodeNumberPart IS NOT NULL
                                                AND E.VisitAddressId = AD.Id
                                                AND E.Id = EM.EmployeeId
                                                AND EM.Id = EC.EmploymentId
                                                AND EC.UntilUtc >= '2020-02-07'""")
        return df

    def retrieve_historical_timeslot_data(self):
        df = cx.read_sql(self.connection,"""SELECT DISTINCT TS.Id, TS.EmployeeID, TS.RelationID, TS.FromUtc, TS.UntilUtc, AD.ZipCodeNumberPart, DATEDIFF(minute, TS.FromUtc, TS.UntilUtc) as TimeSlotLength,
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 21 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasDog", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 27 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasCat", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 33 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasOtherPets", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 37 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "Smokes"
                                                FROM TimeSlots AS TS, Relations as R, Addresses as AD
                                                WHERE TS.TimeSlotType = 0 
                                                AND TS.FromUtc > '2020-02-07'
                                                AND TS.RelationId = R.Id
                                                AND R.VisitAddressId IS NOT NULL
                                                AND AD.ZipCodeNumberPart IS NOT NULL
                                                AND R.VisitAddressId = AD.Id
                                                ORDER BY TS.UntilUtc""")
        return df

    def retrieve_timeslot_data(self):
        df = cx.read_sql(self.connection, """SELECT DISTINCT TS.Id, TS.EmployeeID, TS.RelationID, TS.FromUtc, TS.UntilUtc, AD.ZipCodeNumberPart, DATEDIFF(minute, TS.FromUtc, TS.UntilUtc) as TimeSlotLength,
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 21 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasDog", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 27 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasCat", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 33 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasOtherPets", 
                                                    CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 37 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "Smokes"
                                                FROM TimeSlots AS TS, Relations as R, Addresses as AD
                                                WHERE TS.TimeSlotType = 0 
                                                AND TS.RecurringTimeSlotDefinitionId IS NULL
                                                AND TS.FromUtc > '2022-05-01'
                                                AND TS.UntilUtc < '2023-01-01'
                                                AND TS.RelationId = R.Id
                                                AND R.VisitAddressId IS NOT NULL
                                                AND AD.ZipCodeNumberPart IS NOT NULL
                                                AND R.VisitAddressId = AD.Id
                                                ORDER BY TS.UntilUtc""")
        return df

    def retrieve_client_mismatches(self):
        df = cx.read_sql(self.connection, """SELECT DISTINCT IERC.EmployeeId, IERC.RelationId, IERC.CreatedOnUTC
                                                FROM InvalidEmployeeRelationCombinations AS IERC
                                                ORDER BY IERC.CreatedOnUTC""")
        return df

    def get_data(self,get_var,_from):
        df = cx.read_sql(self.connection,"SELECT {} from dbo.{}".format(get_var,_from))

    def get_relation_characteristics(self):
        df = cx.read_sql(self.connection, """SELECT R.Id, 
                                                CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 21 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasDog", 
                                                CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 27 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasCat", 
                                                CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 33 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "HasOtherPets", 
                                                CASE WHEN EXISTS (SELECT * FROM RelationCharacteristics AS RC WHERE RC.CharacteristicId = 37 AND RC.RelationId = R.Id) THEN 1 ELSE 0 END AS "Smokes"
                                                FROM Relations as R""")
        return df

    def get_employee_characteristics(self):
        df = cx.read_sql(self.connection, """SELECT E.Id, 
                                                CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EC WHERE EC.CharacteristicId = 2 AND EC.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasDogAllergy", 
                                                CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EC WHERE EC.CharacteristicId = 3 AND EC.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasCatAllergy", 
                                                CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EC WHERE EC.CharacteristicId = 4 AND EC.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasOtherPetsAllergy", 
                                                CASE WHEN EXISTS (SELECT * FROM EmployeeCharacteristics AS EC WHERE EC.CharacteristicId = 5 AND EC.EmployeeId = E.Id) THEN 1 ELSE 0 END AS "HasSmokeAllergy"
                                                FROM Employees as E""")
        return df

    def get_data(self,get_var,_from,where=""):
        df = cx.read_sql(self.connection,"SELECT {} from dbo.{} {} and TS.FromUtc >= '2020-02-07'".format(get_var,_from,where))
        return df
