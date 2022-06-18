import read_db
import pandas as pd
import time
from tqdm import tqdm

class functions():

    def __init__(self):
        self.df=None
        self.dict={}

    def set_dataframe(self):
        extract = read_db.ExtractData()
        self.df = extract.get_timeslots_info()

    def main(self):
        self.set_dataframe()
        temp = {}
        for i,row in tqdm(self.df.iterrows()):
            key = row["EmployeeID"], row["RelationID"]

            ## Calculate the remaining availability in each calendar month and week
            appointmentLength = (row["UntilUtc"] - row["FromUtc"]).total_seconds()/3600
            if key[0] in temp:
                if temp[key[0]]["DateLastWorked"].year == row["FromUtc"].year:
                    if temp[key[0]]["DateLastWorked"].month == row["FromUtc"].month:
                        temp[key[0]]["HoursLeftInMonth"] = temp[key[0]]["HoursLeftInMonth"] - appointmentLength
                    else: 
                        temp[key[0]]["HoursLeftInMonth"] = row["AverageNumberOfHoursPerMonth"] - appointmentLength
                    if temp[key[0]]["DateLastWorked"].isocalendar().week == row["FromUtc"].isocalendar().week:
                        temp[key[0]]["HoursLeftInWeek"] = temp[key[0]]["HoursLeftInWeek"] - appointmentLength
                    else: 
                        temp[key[0]]["HoursLeftInWeek"] = row["NumberOfHoursPerWeek"] - appointmentLength
                else: 
                    temp[key[0]]["HoursLeftInMonth"] = row["AverageNumberOfHoursPerMonth"] - appointmentLength
                    temp[key[0]]["HoursLeftInWeek"] = row["NumberOfHoursPerWeek"] - appointmentLength
                temp[key[0]]["DateLastWorked"] = row["UntilUtc"]
            else:
                temp[key[0]] = {"HoursLeftInMonth": row["AverageNumberOfHoursPerMonth"] - appointmentLength, 
                                "HoursLeftInWeek": row["NumberOfHoursPerWeek"] - appointmentLength,
                                "DateLastWorked": row["FromUtc"]}

            ## Calculate the days since last visit and the total number of visits
            if key in temp:
                DaysSinceLastVisit = (row["UntilUtc"] - temp[key]["DateOfLastVisit"]).days
                temp[key]["DateOfLastVisit"] = row["UntilUtc"] 
                temp[key]["NumberOfPreviousVisits"] += 1
            else:
                DaysSinceLastVisit = 0
                temp[key] = {"DateOfLastVisit": row["UntilUtc"], "NumberOfPreviousVisits": 0}

            ## Calculate the number of months left in the contract
            NumberOfMonthsLeftInContract = (row["ContractUntil"] - row["UntilUtc"]).days/30

            ## Save all calculated variables in self.dict
            self.dict[row["Id"]] = {"ClientMismatch": row["ClientMismatch"], 
                                    "DogAllergyMismatch": row["DogAllergyMismatch"],
                                    "CatAllergyMismatch": row["CatAllergyMismatch"],
                                    "OtherPetsAllergyMismatch": row["OtherPetsAllergyMismatch"],
                                    "SmokeAllergyMismatch": row["SmokeAllergyMismatch"], 
                                    "HoursLeftInMonth": temp[key[0]]["HoursLeftInMonth"],
                                    "HoursLeftInWeek": temp[key[0]]["HoursLeftInWeek"],
                                    "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract,
                                    "DaysSinceLastVisit": DaysSinceLastVisit,
                                    "NumberOfPreviousVisits": temp[key]["NumberOfPreviousVisits"]}
            print(self.dict[row["Id"]])
            
            

if __name__ == "__main__":
    t1 = time.time()
    functions().main()
    print(time.time() - t1)