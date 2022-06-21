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
from scipy.stats import skewnorm
from scipy.stats import poisson
import random

class ComputeDataframe:

    def __init__(self):
        self.extract = read_db.ExtractData()
        self.train_df=None
        # self.df = self.extract.get_timeslots_info()
        # self.dict = self.df.set_index('Id').to_dict(orient='index')

    class Characteristics:
        def __init__(self,outer_class):

            self.outer_class = outer_class
            self.employees = self.set_employee_dict()
            self.relations = self.set_relation_dict()

        def set_employee_dict(self):
            df = self.outer_class.extract.get_employee_characteristics()
            employees = df.set_index('Id').to_dict(orient='index')
            return employees

        def set_relation_dict(self):
            df = self.outer_class.extract.get_relation_characteristics()
            relations = df.set_index('Id').to_dict(orient='index')
            return relations

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

            employees = self.outer_class.Characteristics(self.outer_class).employees
            relations = self.outer_class.Characteristics(self.outer_class).relations

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

        def __init__(self,outerclass,good_distance_ratio=0.25):
            self.outerclass=outerclass
            self.good_distance_ratio=good_distance_ratio

        def create_distance(self):
            g_distance=expon.rvs(scale=self.outerclass.train_df['Distances'].mean(), size=int(np.ceil((1-self.good_distance_ratio)*len(self.outerclass.train_df))))
            b_distance=np.random.normal(self.outerclass.train_df['Distances'].mean()+8, 2, int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df))))
            return pd.DataFrame(np.concatenate([b_distance,g_distance], axis=None),columns=['Distances'])
            
        def compute(self):
            NumberOfMismatchesPerFactor = int(self.good_distance_ratio*len(self.outerclass.train_df))
            MaxDataLength = len(self.outerclass.train_df)

            distance=self.create_distance()[:MaxDataLength]
            ClientMismatch = self.create_client_mismatch()[:MaxDataLength]

            MismatchingTimeslots = self.create_mismatching_timeslot_details()
            MatchingTimeslots = self.create_fair_timeslot_details()
            TimeslotsDetails = pd.concat([MatchingTimeslots[:(NumberOfMismatchesPerFactor*2)], MismatchingTimeslots, MatchingTimeslots[(NumberOfMismatchesPerFactor*2):]], ignore_index = True)[:MaxDataLength]

            MismatchingCharacteristics = self.create_characteristic_mismatches()
            MatchingCharacteristics = self.create_fair_characteristics()
            Characteristics = pd.concat([MatchingCharacteristics, MismatchingCharacteristics], ignore_index = True)[:MaxDataLength]

            return pd.concat([distance, ClientMismatch, TimeslotsDetails, Characteristics], axis = 1)


        def create_client_mismatch(self):
            ClientMatch1 = np.zeros(int(np.ceil((self.good_distance_ratio)*len(self.outerclass.train_df))))
            ClientMismatch = np.ones(int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df))))
            ClientMatch2 = np.zeros(int(np.ceil((self.good_distance_ratio*2)*len(self.outerclass.train_df))))
            return pd.DataFrame(np.concatenate([ClientMatch1, ClientMismatch,ClientMatch2], axis=None),columns=['ClientMismatch'])

        def create_fair_timeslot_details(self):
            size = int((1-self.good_distance_ratio)*len(self.outerclass.train_df))
            HoursLeftInMonth = self.outerclass.train_df["HoursLeftInMonth"].sample(n = size, replace = True, ignore_index = True)
            HoursLeftInWeek = self.outerclass.train_df["HoursLeftInWeek"].sample(n = size, replace = True, ignore_index = True)
            NumberOfMonthsLeftInContract = self.outerclass.train_df["NumberOfMonthsLeftInContract"].sample(n = size, replace = True, ignore_index = True)
            DaysSinceLastVisit = self.outerclass.train_df["DaysSinceLastVisit"].sample(n = size, replace = True, ignore_index = True)
            NumberOfPreviousVisits = self.outerclass.train_df["NumberOfPreviousVisits"].sample(n = size, replace = True, ignore_index = True)
            return pd.DataFrame({"HoursLeftInMonth": HoursLeftInMonth, "HoursLeftInWeek": HoursLeftInWeek, "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract, "DaysSinceLastVisit": DaysSinceLastVisit, "NumberOfPreviousVisits": NumberOfPreviousVisits})

        def create_mismatching_timeslot_details(self):
            size = int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df)))
            HoursLeftInMonth = skewnorm.rvs(a = 4, loc = 0, scale = self.outerclass.train_df["HoursLeftInMonth"].std(), size = size)
            HoursLeftInWeek = skewnorm.rvs(a = 4, loc = 0, scale = self.outerclass.train_df["HoursLeftInWeek"].std(), size = size)
            NumberOfMonthsLeftInContract = expon.rvs(loc = 0, scale = self.outerclass.train_df["NumberOfMonthsLeftInContract"].std(), size = size)
            DaysSinceLastVisit = self.outerclass.train_df["DaysSinceLastVisit"].sample(n = size, replace = True, ignore_index = True)
            NumberOfPreviousVisits = poisson.rvs(mu = 2, loc = 0, size = int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df)*0.5)))
            NumberOfPreviousVisits = np.concatenate((NumberOfPreviousVisits, np.zeros(int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df)*0.5)))), axis = None)[:size]
            return pd.DataFrame({"HoursLeftInMonth": HoursLeftInMonth, "HoursLeftInWeek": HoursLeftInWeek, "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract, "DaysSinceLastVisit": DaysSinceLastVisit, "NumberOfPreviousVisits": NumberOfPreviousVisits})

        def create_fair_characteristics(self):
            size = int(np.ceil((1-self.good_distance_ratio)*len(self.outerclass.train_df)))
            EmployeeHasDogAllergy = self.outerclass.train_df["EmployeeHasDogAllergy"].sample(n = size, replace = True, ignore_index = True)
            EmployeeHasCatAllergy = self.outerclass.train_df["EmployeeHasCatAllergy"].sample(n = size, replace = True, ignore_index = True)
            EmployeeHasOtherPetsAllergy = self.outerclass.train_df["EmployeeHasOtherPetsAllergy"].sample(n = size, replace = True, ignore_index = True)
            EmployeeHasSmokeAllergy = self.outerclass.train_df["EmployeeHasSmokeAllergy"].sample(n = size, replace = True, ignore_index = True)
            RelationHasDog = self.outerclass.train_df["RelationHasDog"].sample(n = size, replace = True, ignore_index = True)
            RelationHasCat = self.outerclass.train_df["RelationHasCat"].sample(n = size, replace = True, ignore_index = True)
            RelationHasOtherPets = self.outerclass.train_df["RelationHasOtherPets"].sample(n = size, replace = True, ignore_index = True)
            RelationSmokes = self.outerclass.train_df["RelationSmokes"].sample(n = size, replace = True, ignore_index = True)
            return pd.DataFrame({"EmployeeHasDogAllergy": EmployeeHasDogAllergy, "EmployeeHasCatAllergy": EmployeeHasCatAllergy, "EmployeeHasOtherPetsAllergy": EmployeeHasOtherPetsAllergy, "EmployeeHasSmokeAllergy": EmployeeHasSmokeAllergy, "RelationHasDog": RelationHasDog, "RelationHasCat": RelationHasCat, "RelationHasOtherPets": RelationHasOtherPets, "RelationSmokes": RelationSmokes})

        def create_characteristic_mismatches(self):
            DogAllergyRatio = self.outerclass.train_df['EmployeeHasDogAllergy'].mean()
            CatAllergyRatio = self.outerclass.train_df['EmployeeHasCatAllergy'].mean()
            OtherPetsAllergyRatio = self.outerclass.train_df['EmployeeHasOtherPetsAllergy'].mean()
            SmokeAllergyRatio = self.outerclass.train_df['EmployeeHasSmokeAllergy'].mean()
            DogRatio = self.outerclass.train_df['RelationHasDog'].mean()
            CatRatio = self.outerclass.train_df['RelationHasCat'].mean()
            OtherPetsRatio = self.outerclass.train_df['RelationHasOtherPets'].mean()
            SmokesRatio = self.outerclass.train_df['RelationSmokes'].mean()

            RatioAllergy = int(np.ceil(self.good_distance_ratio*len(self.outerclass.train_df) / 4))

            DogAllergyList = np.concatenate((np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy * 3, p=[1-DogAllergyRatio, DogAllergyRatio])),axis=None)
            DogRatioList = np.concatenate((np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy * 3, p=[1-DogRatio, DogRatio])),axis=None)
            CatAllergyList = np.concatenate((np.random.choice([0,1], size=RatioAllergy, p=[1-CatAllergyRatio, CatAllergyRatio]), np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy * 2, p=[1-CatAllergyRatio, CatAllergyRatio])),axis=None)
            CatRatioList = np.concatenate((np.random.choice([0,1], size=RatioAllergy, p=[1-CatRatio, CatRatio]), np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy * 2, p=[1-CatRatio, CatRatio])),axis=None)
            OtherPetsAllergyList = np.concatenate((np.random.choice([0,1], size=RatioAllergy * 2, p=[1-OtherPetsAllergyRatio, OtherPetsAllergyRatio]), np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy, p=[1-OtherPetsAllergyRatio, OtherPetsAllergyRatio])),axis=None)
            OtherPetsRatioList = np.concatenate((np.random.choice([0,1], size=RatioAllergy * 2, p=[1-OtherPetsRatio, OtherPetsRatio]), np.ones(RatioAllergy), np.random.choice([0,1], size=RatioAllergy, p=[1-OtherPetsRatio, OtherPetsRatio])),axis=None)
            SmokeAllergyList = np.concatenate((np.random.choice([0,1], size=RatioAllergy * 3, p=[1-SmokeAllergyRatio, SmokeAllergyRatio]), np.ones(RatioAllergy)),axis=None)
            SmokesRatioList = np.concatenate((np.random.choice([0,1], size=RatioAllergy * 3, p=[1-SmokesRatio, SmokesRatio]), np.ones(RatioAllergy)),axis=None)

            return pd.DataFrame({'EmployeeHasDogAllergy': DogAllergyList, 
                    'EmployeeHasCatAllergy': CatAllergyList, 
                    'EmployeeHasOtherPetsAllergy': OtherPetsAllergyList, 
                    'EmployeeHasSmokeAllergy': SmokeAllergyList, 
                    'RelationHasDog': DogRatioList, 
                    'RelationHasCat': CatRatioList, 
                    'RelationHasOtherPets': OtherPetsRatioList, 
                    'RelationSmokes': SmokesRatioList})
            offset=0
            distance=self.create_distance(self.good_distance_ratio)
            offset+=int(self.good_distance_ratio*len(self.outerclass.train_df))

    def func(self,x, a, b, c):
        return a * np.exp(-b * x) + c


    def main(self): 
        _self=ComputeDataframe()
        dist=_self.Distance(self)

        distances=dist.get_distance_timeslots()
        plt.hist(distances, bins=100,range=[0,20])
        plt.show()

        tsd=_self.TimeSeriesDetails(self).main()
        df=tsd.merge(distances, how='inner',  left_index=True, right_on='Id')
        df['Label']=1
        self.train_df=df[df['Distances']<=20]

        print(self.train_df.head())

        non_matches=self.nonMatched(self)
        generated_data = non_matches.compute()

        avb=_self.Availability(self)
        real_availability,fake_availability=avb.past_availability()

        generated_data = pd.concat([generated_data, fake_availability.sample(n = generated_data.shape[0], ignore_index = True)], axis = 1)
        generated_data['Label']=0
        print(generated_data)

if __name__=="__main__":
    df=ComputeDataframe()
    df.main()