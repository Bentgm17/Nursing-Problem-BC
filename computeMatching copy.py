import re
import dataloading.read_db_copy as read_db
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
from time import time
import json
import os
import csv
from matchclassifier.model import BinaryClassification
import torch
import geopy.distance
from math import sin, cos, sqrt, atan2, radians
import sys



CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

class computeMatching:
    def __init__(self,source):
        self.extract = read_db.ExtractData(source)
        model=BinaryClassification(9,1)
        self.model=model.load_state_dict(torch.load(CURRENT_DIR+'/dataloading/checkpoint.pth',map_location=torch.device('cpu')))
        self.lat_long_dict=None
        self.timeslots_data=None
        self.employee_data = None
        self.relation_data = None
        self.visits=None
        self.date=''
        self.result={}

    def get_lat_long_dict(self):
        with open(CURRENT_DIR+"/dataloading/NL.csv", mode='r') as inp:
            reader = csv.reader(inp)
            dict_from_csv = {rows[1]:{"latitude":rows[9],"longitude":rows[10]} for rows in reader}
        return dict_from_csv

    def initialize_availability(self,keys):
        for k in self.employee_data.keys():
            self.employee_data[k]['Availability']={"01":1,"02":1,"11":1,"12":1,"21":1,"22":1,"31":1,"32":1,"41":1,"42":1,'51':1,'52':1,'61':1,'62':1}
    
    def compute_distance(self,lat_long_rel,lat_long_emp):
        R = 6373.0

        lat1 = radians(float(lat_long_rel['latitude']))
        lon1 = radians(float(lat_long_rel['longitude']))
        lat2 = radians(float(lat_long_emp['latitude'] ))
        lon2 = radians(float(lat_long_emp['longitude']))

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

    def compute_allergy_mismatch(self,relation,employee):
        return(np.nansum([employee["HasDogAllergy"]/relation["HasDog"] if employee["HasDog"] else 0,
                                      employee["HasCatAllergy"]/relation["HasCat"] if relation["HasCat"] else 0,
                                      employee["HasOtherPetsAllergy"]/relation["HasOtherPets"] if relation["HasOtherPets"] else 0,
                                      employee["HasSmokeAllergy"]/relation["Smokes"] if relation["Smokes"] else 0]) != 0)

    def compute_availability(self,id,allocation,weekday):
        if sum(allocation)==2: 
            return self.employee_data[id]['Availability'][str(weekday)+"1"]*\
                                                self.employee_data[id]['Availability'][str(weekday)+"2"]
        else:
            return allocation[0]*self.employee_data[id]['Availability'][str(weekday)+"1"]+\
                                                allocation[1]*self.employee_data[id]['Availability'][str(weekday)+"2"]
        
    def get_allocation_timeslot(self,from_utc,until_utc):
        if from_utc.time()<dt.time(12,00) and until_utc.time()>dt.time(12,00): 
            return [1,1]
        elif from_utc.time()<dt.time(12,00):
            return [1,0]
        else:
            return [0,1]

    def schedule_non_recurring(self,tv):
        allocation_timeslot=self.get_allocation_timeslot(tv['FromUtc'],tv['UntilUtc'])
        weekday=tv['UntilUtc'].weekday()
        outcome={}
        for ek,ev in self.employee_data.items():
            features=[]
            try:
                #ClientMismatch
                # TODO: add client mismatch
                #Hoursleftincontract
                Hoursleftincontract=self.employee_data[ek]['Hoursleft']
                features.append(Hoursleftincontract)
                #DaysSinceLastVisit
                days_since_last_visit=self.visits[tv['RelationId']][ek]['DaysSinceLastVisit']
                features.append(days_since_last_visit)
                #NumberOfPreviousVisits
                NumberOfPreviousVisits=self.visits[tv['RelationId']][ek]['NumberOfPreviousVisits']
                features.append(NumberOfPreviousVisits)
                #Allergymismatch
                allergy_mismatch=self.compute_allergy_mismatch(self.relation_data[tv['RelationId']],self.employee_data[ek])
                features.append(allergy_mismatch)
                #Distances
                distance=self.compute_distance(self.lat_long_dict[self.relation_data[tv['RelationId']]['ZipCode']],self.lat_long_dict[ev['ZipCode']])
                features.apppend(distance)
                #Availability
                availability=self.compute_availability(ek,allocation_timeslot,weekday)
                features.append(availability)

                score=self.model.predict(torch.tensor(features))       
                outcome['ek']=score
            except Exception as e:
                print(e)
        self.result[tv['Id']]=outcome
        return self.result
    
    def schedule(self,tv):
        if tv['RecurringTimeSlotDefinitionId']:#TO-DO: Is not null
            tv['EmployeeId'] # TO-DO: This returns employee, should be probaility dict
        else:
            self.schedule_non_recurring(tv)

    def refresh_availability(self,tv):
        for employee in  self.employee_data.keys():
            if employee!=tv['EmployeeId']:
                self.employee_data[employee]['Availability'][str(tv['UntilUtc'].weekday())+"1"]*=0.7
                self.employee_data[employee]['Availability'][str(tv['UntilUtc'].weekday())+"2"]*=0.7
    
    def refresh_days_since_last_visit(self,tv):
        for relation in self.visits.keys():
            for employee in self.visits[relation].keys():
                self.visits[relation][employee]['daysSinceLastVisit']+=1

    def reset_monthly_hours(self):
        #TODO: add MonthlyHours to employee_data in read_db.get_employee_characteristics()
        for employee in self.employee_data.keys():
            self.employee_data[employee]['Hoursleft']=self.employee_data[employee]['MonthlyHours']

    def refresh(self,tv,hard=False):
        self.refresh_availability(tv)
        self.refresh_days_since_last_visit(tv)
        if hard:
            self.reset_monthly_hours() ##TODO: add MonthlyHours to employee_data in read_db.get_employee_characteristics()

    def check_date(self,tv):
        if self.date.month != tv['UntilUtc'].month:
            self.refresh(tv,True)
        if self.date!=tv['UntilUtc'].date():
            self.refresh(tv)
    
    def update_availability(self,tv):
        allocation=self.get_allocation_timeslot(tv['FromUtc'],tv['UntilUtc'])
        self.employee_data[tv['EmployeeId']]['Availability'][str(tv['UntilUtc'].weekday())+"1"]=allocation[0]
        self.employee_data[tv['EmployeeId']]['Availability'][str(tv['UntilUtc'].weekday())+"2"]=allocation[1]
    
    def update_after_appointment(self,tv):
        self.visits[tv['RelationId']][tv['EmployeeId']]['daysSinceLastVisit']=0
        self.visits[tv['RelationId']][tv['EmployeeId']]['NumberOfPreviousVisits']+=1
        self.employee_data[tv['EmployeeId']]['Hoursleft']-=tv['Duration'] 
        self.update_availability(tv)

    def main(self): 
        self.lat_long_dict=self.get_lat_long_dict()
        self.timeslots_data=self.extract.get_data("TS.Id,TS.EmployeeId,TS.RelationId.FromUtc,TS.UntilUtc,TS.RecurringTimeSlotDefinitionId,TS.TimeSlotType,DATEDIFF(minute, TS.FromUtc, TS.UntilUtc) as Duration","TimeSlots TS","where (TS.TimeSlotType=0 or TS.TimeSlotType=1) and TS.CreatedOnUTC<=TS.FromUtc")
        self.employee_data = self.extract.get_employee_characteristics()
        self.relation_data = self.extract.get_relation_characteristics()
        self.initialize_availability()
        for tk,tv in tqdm(self.timeslots_data.items(),total=len(self.employee_data)):
            self.check_date(tv)
            if tv['TimeSlotType']==0:
                self.schedule(tv)
                self.update_after_appointment(tv)
            if tv['TimeSlotType']==1:
                self.update_availability(tv)
        return pd.DataFrame(dict).T

if __name__=="__main__":
    """
    Make sure to check every key exists or make new one with all dicts
    Check for data types as keys
    Do the TO-DO's
    Make model prediction work    
    """
    # Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
    Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod-2022-6-17-15-25-ANONYMOUS")
    df = Matching.main()