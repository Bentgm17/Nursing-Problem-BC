from regex import R
from sqlalchemy import column
import read_db
import time
import pgeocode
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from scipy.stats import expon
import random

class ComputeDataframe:

    def __init__(self):
        self.extract = read_db.ExtractData()
        self.train_df=None
        # self.df = self.extract.get_timeslots_info()
        # self.dict = self.df.set_index('Id').to_dict(orient='index')

    class Characteristics:
        def __init__(self,outer_class):
            self.outer_class=outer_class
            self.employees =self.set_employee_dict()
            self.relations =self.set_relation_dict()

        def set_employee_dict(self):
            df = self.outer_class.extract.get_employee_characteristics()
            return df.set_index('Id').to_dict(orient='index')

        def get_employee_dict(self):
            return self.employees

        def set_relation_dict(self):
            df = self.outer_class.extract.get_relation_characteristics()
            return df.set_index('Id').to_dict(orient='index')

        def get_relation_dict(self):
            return self.relations

    class Distance:

        def __init__(self,outer_class):
            self.outer_class=outer_class

        def get_distance(self,df):
            dist = pgeocode.GeoDistance('NL')
            distance=dist.query_postal_code(df['ZipCode_x'].apply(lambda x: "".join(x.split(" "))[:4]).to_list(),
                                            df['ZipCode_y'].apply(lambda x: "".join(x.split(" "))[:4]).to_list())
            return pd.DataFrame(distance,columns=['Distances']).set_index(df.index)

        def get_zipcode(self):
            employee_adres=self.outer_class.extract.join_addresses(target="Employees").dropna()
            relation_adres=self.outer_class.extract.join_addresses(target="Relations").dropna()
            df=pd.merge(employee_adres, relation_adres, on='Id',how='inner').set_index('Id')
            return self.get_distance(df)

        def get_distance_timeslots(self,):
            return self.get_zipcode()

    class TimeSeriesDetails:

        def __init__(self,outer_class):
            self.outer_class=outer_class
            self.out={}

        def retrieve_characteristics(self):
            Characteristics=self.outer_class.Characteristics(self.outer_class)
            employees = Characteristics.get_employee_dict()
            relations = Characteristics.get_relation_dict()
            return employees, relations

        def main(self):
            temp = {}
            dict = self.outer_class.extract.get_timeslots_info()
            dict = dict.set_index('Id').to_dict(orient='index')
            employees, relations = self.retrieve_characteristics()

            for k,v in tqdm(dict.items(),total=len(dict)):
                key = v["EmployeeID"], v["RelationID"]

                ## Calculate the remaining availability in each calendar month and week
                appointmentLength = (v["UntilUtc"] - v["FromUtc"]).total_seconds()/3600
                if key[0] in temp:
                    if temp[key[0]]["DateLastWorked"].year == v["FromUtc"].year:
                        if temp[key[0]]["DateLastWorked"].month == v["FromUtc"].month:
                            temp[key[0]]["HoursLeftInMonth"] = temp[key[0]]["HoursLeftInMonth"] - appointmentLength
                        else: 
                            temp[key[0]]["HoursLeftInMonth"] = v["AverageNumberOfHoursPerMonth"] - appointmentLength
                        if temp[key[0]]["DateLastWorked"].isocalendar().week == v["FromUtc"].isocalendar().week:
                            temp[key[0]]["HoursLeftInWeek"] = temp[key[0]]["HoursLeftInWeek"] - appointmentLength
                        else: 
                            temp[key[0]]["HoursLeftInWeek"] = v["NumberOfHoursPerWeek"] - appointmentLength
                    else: 
                        temp[key[0]]["HoursLeftInMonth"] = v["AverageNumberOfHoursPerMonth"] - appointmentLength
                        temp[key[0]]["HoursLeftInWeek"] = v["NumberOfHoursPerWeek"] - appointmentLength
                    temp[key[0]]["DateLastWorked"] = v["UntilUtc"]
                else:
                    temp[key[0]] = {"HoursLeftInMonth": v["AverageNumberOfHoursPerMonth"] - appointmentLength, 
                                    "HoursLeftInWeek": v["NumberOfHoursPerWeek"] - appointmentLength,
                                    "DateLastWorked": v["FromUtc"]}

                ## Calculate the days since last visit and the total number of visits
                if key in temp:
                    DaysSinceLastVisit = (v["UntilUtc"] - temp[key]["DateOfLastVisit"]).days
                    temp[key]["DateOfLastVisit"] = v["UntilUtc"] 
                    temp[key]["NumberOfPreviousVisits"] += 1
                else:
                    DaysSinceLastVisit = 0
                    temp[key] = {"DateOfLastVisit": v["UntilUtc"], "NumberOfPreviousVisits": 0}

                ## Calculate the number of months left in the contract
                NumberOfMonthsLeftInContract = (v["ContractUntil"] - v["UntilUtc"]).days/30

                ## Save all calculated variables in self.dict
                self.out[k] = {"ClientMismatch": v["ClientMismatch"],  
                                "HoursLeftInMonth": temp[key[0]]["HoursLeftInMonth"],
                                "HoursLeftInWeek": temp[key[0]]["HoursLeftInWeek"],
                                "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract,
                                "DaysSinceLastVisit": DaysSinceLastVisit,
                                "NumberOfPreviousVisits": temp[key]["NumberOfPreviousVisits"],
                                "EmployeeHasDogAllergy": employees[key[0]]["HasDogAllergy"],
                                "EmployeeHasCatAllergy": employees[key[0]]["HasCatAllergy"],
                                "EmployeeHasOtherPetsAllergy": employees[key[0]]["HasOtherPetsAllergy"],
                                "EmployeeHasSmokeAllergy": employees[key[0]]["HasSmokeAllergy"],
                                "RelationHasDog": relations[key[1]]["HasDog"],
                                "RelationHasCat": relations[key[1]]["HasCat"],
                                "RelationHasOtherPets": relations[key[1]]["HasOtherPets"],
                                "RelationSmokes": relations[key[1]]["Smokes"]}
            return pd.DataFrame.from_dict(self.out, orient = "index")

    class Availability:

        def __init__(self,outer_class):
            self.outer_class=outer_class
            self.out={}
        '''
        For Real life computation:


        def future_availability(self):
            df=self.outer_class.extract.get_data("TS.EmployeeId,Ts.FromUtc,TS.UntilUtc,TS.CreatedOnUtc","TimeSlots TS","where (TS.TimeSlotType=0 or TS.TimeSlotType=1) and TS.CreatedOnUTC<=TS.FromUtc")
            df.sort_values(by=['CreatedOnUtc'],inplace=True)
            dct=df.to_dict(orient='index')
            out={}
            for k,v in tqdm(dct.items(),total=len(dct)):
                out.setdefault(v['EmployeeId'],[]).append({'Date':v['UntilUtc'],'Day of the week':v['UntilUtc'].weekday(),'Time':v['UntilUtc'].time(),'RelationId':v['RelationId']})
            return df
        '''

        def compute_availibility(self,v,employee_id):
            if v['FromUtc'].time()<dt.time(12,00) and v['UntilUtc'].time()>dt.time(12,00): 
                return self.out[employee_id][str(v['UntilUtc'].weekday())+"1"]*self.out[employee_id][str(v['UntilUtc'].weekday())+"2"]
            elif v['FromUtc'].time()<dt.time(12,00):
                return self.out[employee_id][str(v['UntilUtc'].weekday())+"1"]
            else:
                return self.out[employee_id][str(v['UntilUtc'].weekday())+"2"]

        def refresh(self,v):
            for employee in self.out.keys():
                self.out[employee][str(v['UntilUtc'].weekday())+"1"]*=0.7
                self.out[employee][str(v['UntilUtc'].weekday())+"2"]*=0.7
        
        def update_employee(self,v):
            if v['FromUtc'].time()<dt.time(12,00) and v['UntilUtc'].time()>dt.time(12,00): 
                self.out[v['EmployeeId']].update({str(v['UntilUtc'].weekday())+"1": 1, str(v['UntilUtc'].weekday())+"2": 1})
            elif v['FromUtc'].time()<dt.time(12,00):
                self.out[v['EmployeeId']].update({str(v['UntilUtc'].weekday())+"1": 1})
            else:
                self.out[v['EmployeeId']].update({str(v['UntilUtc'].weekday())+"2": 1})

        def past_availability(self):
            df=self.outer_class.extract.get_data("TS.EmployeeId,Ts.FromUtc,TS.UntilUtc","TimeSlots TS","where (TS.TimeSlotType=0 or TS.TimeSlotType=1) and TS.CreatedOnUTC<=TS.FromUtc")
            df.sort_values(by=['UntilUtc'],inplace=True)
            dct=df.to_dict(orient='index')
            for i in df['EmployeeId'].unique():
                self.out[i]={"01":1,"02":1,"11":1,"12":1,"21":1,"22":1,"31":1,"32":1,"41":1,"42":1,'51':1,'52':1,'61':1,'62':1}
            date=df['UntilUtc'].iloc[0].date()
            keys=list(self.out.keys())
            availabilities=[]
            fake_availability=[]
            for k,v in tqdm(dct.items(),total=len(dct)):  
                availabilities.append(self.compute_availibility(v,v['EmployeeId']))
                fake_availability.append(self.compute_availibility(v,random.choice(keys)))
                if date!=v['UntilUtc'].date():
                    self.refresh(v)
                    date=v['UntilUtc'].date()
                self.update_employee(v)
            return pd.DataFrame(availabilities,columns=['Past Availability']),pd.DataFrame(fake_availability,columns=['Fake Availability'])

    class nonMatched:

        def __init__(self,outerclass,good_distance_ratio=0.2):
            self.outerclass=outerclass
            self.good_distance_ratio=good_distance_ratio

        def create_distance(self,ratio):
            g_distance=expon.rvs(scale=self.outerclass.train_df['Distances'].mean(), size=int((1-ratio)*len(self.outerclass.train_df)))
            b_distance=np.random.normal(self.outerclass.train_df['Distances'].mean()+8, 2, int(ratio*len(self.outerclass.train_df)))
            return pd.DataFrame(np.concatenate([b_distance,g_distance], axis=None),columns=['Distances'])
            
        def compute(self):
            offset=0
            distance=self.create_distance(self.good_distance_ratio)
            offset+=int(self.good_distance_ratio*len(self.outerclass.train_df))

    def func(self,x, a, b, c):
        return a * np.exp(-b * x) + c


    def main(self):
        _self=ComputeDataframe()
        dist=_self.Distance(self)
        distances=dist.get_distance_timeslots()
        tsd=_self.TimeSeriesDetails(self).main()
        df=tsd.merge(distances, how='inner',  left_index=True, right_on='Id')
        df['Label']=1
        self.train_df=df[df['Distances']<=20]
        non_matches=self.nonMatched(self)
        non_matches.compute()
        avb=_self.Availability(self)
        real_availability,fake_availability=avb.past_availability()

if __name__=="__main__":
    df=ComputeDataframe()
    df.main()


