from regex import R
import read_db
import time
import pgeocode
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from scipy.stats import expon

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

        def future_availability(self):
            df=self.outer_class.extract.get_data("Id,EmployeeId,RelationId,UntilUtc","TimeSlots","where (TimeSlotType=0 or TimeSlotType=1) and CreatedOnUTC<=FromUtc")

            df.sort_values(by=['UntilUtc'],inplace=True)
            dct=df.to_dict(orient='index')
            out={}
            for k,v in tqdm(dct.items(),total=len(dct)):
                out.setdefault(v['EmployeeId'],[]).append({'Date':v['UntilUtc'],'Day of the week':v['UntilUtc'].weekday(),'Time':v['UntilUtc'].time(),'RelationId':v['RelationId']})
            return df

        def past_availability(self):
            df=self.outer_class.extract.get_data("Id,EmployeeId,RelationId,UntilUtc","TimeSlots","where (TimeSlotType=0 or TimeSlotType=1) and CreatedOnUTC<=FromUtc")
            print(df)
            df.sort_values(by=['UntilUtc'],inplace=True)
            dct=df.to_dict(orient='index')
            out={}
            for k,v in tqdm(dct.items(),total=len(dct)):
                out.setdefault(v['EmployeeId'],[]).append({'Date':v['UntilUtc'],'Day of the week':v['UntilUtc'].weekday(),'Time':v['UntilUtc'].time(),'RelationId':v['RelationId']})
            return df


    class nonMatched:

        def __init__(self,outerclass,good_distance_ratio=0.2):
            self.outerclass=outerclass
            self.good_distance_ratio=good_distance_ratio

        def create_distance(self,ratio):
            return expon.rvs(scale=self.outerclass.train_df['Distances'].mean(), size=ratio)
            
        def compute(self):
           distance=self.create_distance(int(self.good_distance_ratio*len(self.outerclass.train_df)))

    def func(self,x, a, b, c):
        return a * np.exp(-b * x) + c


    def main(self):
        _self=ComputeDataframe()
        dist=_self.Distance(self)
        distances=dist.get_distance_timeslots()
        # y=distances.hist(bins=100,range=[0, 20])
        # plt.show()
        # n, x, _ = plt.hist(distances,bins=100,range=[0, 20])
        # bin_centers = 0.5*(x[1:]+x[:-1])

        # popt, pcov = curve_fit(self.func, bin_centers, n)
        # print(popt)
        # print(pcov)

        # plt.plot(bin_centers, n, label='data')
        # plt.plot(bin_centers,self.func(bin_centers, *popt), label='fit')
        # plt.show()
        tsd=_self.TimeSeriesDetails(self).main()
        df=tsd.merge(distances, how='inner',  left_index=True, right_on='Id')
        df['Label']=1
        print(df)
        self.train_df=df
        non_matches=self.nonMatched(self)
        non_matches.compute()
        # tsd['Distance']=dist.get_distance_timeslots()
        # print(tsd)
        # avb=_self.Availability(self)
        # print(avb.past_availability())
        # print(avb.future_availability())

if __name__=="__main__":
    df=ComputeDataframe()
    df.main()


