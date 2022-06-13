from configparser import NoOptionError
import read_db
import time
import pgeocode
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

class ComputeDataframe:

    def __init__(self):
        self.extract = read_db.ExtractData()
        # self.df=self.extract.get_timeslots_info()
        # self.dict=self.df.set_index('Id').to_dict(orient='index')

    class Distance:

        def __init__(self,outer_class):
            self.outer_class=outer_class

        def get_distance(self,df):
            dist = pgeocode.GeoDistance('NL')
            distance=dist.query_postal_code(df['ZipCode_x'].apply(lambda x: "".join(x.split(" "))[:4]).to_list(),
                                            df['ZipCode_y'].apply(lambda x: "".join(x.split(" "))[:4]).to_list())
            return pd.DataFrame(distance).set_index(df.index)

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

        def main(self):
            temp = {}
            for k,v in tqdm(self.outer_class.dict.items(),total=len(self.outer_class.dict)):
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
                                        "DogAllergyMismatch": v["DogAllergyMismatch"],
                                        "CatAllergyMismatch": v["CatAllergyMismatch"],
                                        "OtherPetsAllergyMismatch": v["OtherPetsAllergyMismatch"],
                                        "SmokeAllergyMismatch": v["SmokeAllergyMismatch"], 
                                        "HoursLeftInMonth": temp[key[0]]["HoursLeftInMonth"],
                                        "HoursLeftInWeek": temp[key[0]]["HoursLeftInWeek"],
                                        "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract,
                                        "DaysSinceLastVisit": DaysSinceLastVisit,
                                        "NumberOfPreviousVisits": temp[key]["NumberOfPreviousVisits"]}
            return pd.DataFrame(self.out).T

    class Availability:

        def __init__(self,outer_class):
            self.outer_class=outer_class

        def past_availability(self):
            df=self.outer_class.extract.get_data("EmployeeId,RelationId,UntilUtc","TimeSlots","where TimeSlotType=1 or ")
            return df
        
        



    def main(self):
        _self=ComputeDataframe()
        # dist=_self.Distance(self)
        # dist_plot=dist.get_distance_timeslots()
        # y=dist_plot.hist(bins=100,range=[0, 20])
        # plt.show()
        # tsd=_self.TimeSeriesDetails(self).main()
        avb=_self.Availability(self)
        print(avb.past_availability())

if __name__=="__main__":
    df=ComputeDataframe()
    df.main()


