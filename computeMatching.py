import dataloading.read_db
import numpy as np
import pandas as pd
import pgeocode
import datetime as dt
from tqdm import tqdm
from time import time
import json
import dateutil
from ast import literal_eval
import os
import csv


CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

class computeMatching:
    def __init__(self,source):
        self.extract = dataloading.read_db.ExtractData(source)
        self.dist = pgeocode.GeoDistance('NL')

    def computeDistanceDict(self):
        dict = {}
        ZipCodes = self.extract.retrieve_zipcodes()
        distance = self.dist.query_postal_code(ZipCodes["EmployeeZipCode"].apply(lambda e: str(e)).to_list(), 
                                            ZipCodes["RelationZipCode"].apply(lambda e: str(e)).to_list())
        ZipCodes["Distances"] = distance
        ZipCodes.dropna(inplace = True)
        for k, v in ZipCodes.to_dict(orient = "index").items():
            dict[v["EmployeeZipCode"], v["RelationZipCode"]] = v["Distances"]
        return dict

    def computeMatchProbability(self):
        return None

    def read_jsons(self):
        VisitsJson = open("dataloading\TimeSlotDetails-2022-05-01.json", "r")
        AvailabilityJson = open("dataloading\Availabilities-2022-05-01.json", "r")
        EmployeeHoursJson = open("dataloading\EmployeeDetails-2022-05-01.json", "r")
        VisitsDict = {}
        AvailabilityDict = json.load(AvailabilityJson)
        for k, v in json.load(VisitsJson).items():
            k_0,k_1=literal_eval(k)
            VisitsDict[k_0, k_1] = v
            VisitsDict[k_0,k_1]["DateOfLastVisit"] = dateutil.parser.parse(VisitsDict[k_0,k_1]["DateOfLastVisit"])

        for k, v in json.load(EmployeeHoursJson).items():
            VisitsDict[int(k)] = v
            VisitsDict[int(k)]["DateOfLastVisit"] = dateutil.parser.parse(VisitsDict[int(k)]["DateOfLastVisit"])
        return VisitsDict, AvailabilityDict

    def get_lat_long_dict(self):
        with open(CURRENT_DIR+"/dataloading/NL.csv", mode='r') as inp:
            reader = csv.reader(inp)
            dict_from_csv = {rows[1]:{"latitude":rows[10],"longitude":rows[11]} for rows in reader}
        return dict_from_csv

    def main(self): 
        # print(CURRENT_DIR+"/dataloading/NL.txt")
        dict_from_csv = {}
        lat_long_dict=self.get_lat_long_dict()
        dict = {}
        EmployeeData = self.extract.retrieve_employee_data()
        EmployeeData = EmployeeData.to_dict(orient="index")
        TimeSlotData = self.extract.retrieve_timeslot_data()
        TimeSlotData = TimeSlotData.set_index("Id").to_dict(orient="index")

        PreviousMatches, Availability = self.read_jsons()
        PreviousMatches = {}
        Availability = {}

        for k, v in EmployeeData.items():
            if v["EmployeeId"] not in Availability:
                Availability[v["EmployeeId"]] = {"01":1,"02":1,"11":1,"12":1,"21":1,"22":1,"31":1,"32":1,"41":1,"42":1,'51':1,'52':1,'61':1,'62':1}
        date = next(iter(TimeSlotData.items()))[1]["UntilUtc"].date()

        ClientMismatches = {}
        ClientMismatchExtract = self.extract.retrieve_client_mismatches()
        for k, v in ClientMismatchExtract.to_dict(orient = "index").items():
            ClientMismatches[v["EmployeeId"], v["RelationId"]] = v["CreatedOnUTC"]
        distance_dict = self.computeDistanceDict()

        counter = 0

        for TS_k, TS_v in tqdm(TimeSlotData.items(), total = len(TimeSlotData)):
            counter += 1
            for E_k, E_v in EmployeeData.items(): 
                if date != TS_v["UntilUtc"].date():
                    for key in Availability:
                        Availability[key][str(TS_v['UntilUtc'].weekday()) + "1"] *= 0.7
                        Availability[key][str(TS_v['UntilUtc'].weekday()) + "2"] *= 0.7
                    date = TS_v["UntilUtc"].date()

                if TS_v["EmployeeID"] == E_v["EmployeeId"]:
                    if TS_v['FromUtc'].time()<dt.time(12,00) and TS_v['UntilUtc'].time()>dt.time(12,00): 
                        Availability[E_v['EmployeeId']][str(TS_v['UntilUtc'].weekday())+"1"] = 1
                        Availability[E_v['EmployeeId']][str(TS_v['UntilUtc'].weekday())+"2"] = 1
                    elif TS_v['FromUtc'].time()<dt.time(12,00):
                        Availability[E_v['EmployeeId']][str(TS_v['UntilUtc'].weekday())+"1"] = 1
                    else:
                        Availability[E_v['EmployeeId']][str(TS_v['UntilUtc'].weekday())+"2"] = 1

                if TS_v['FromUtc'].time()<dt.time(12,00):
                    AvailabilityOutput = Availability[E_v["EmployeeId"]][str(TS_v["UntilUtc"].weekday()) + "1"]
                else:
                    AvailabilityOutput = Availability[E_v["EmployeeId"]][str(TS_v["UntilUtc"].weekday()) + "2"]

                if ((TS_v["UntilUtc"] >= E_v["ContractFrom"]) and ((TS_v["UntilUtc"] <= E_v["ContractUntil"]) or (E_v["ContractUntil"] is None))):
                    if (TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]) in distance_dict:
                        Distances = distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]]
                    else: 
                        Distances = self.dist.query_postal_code(str(TS_v["ZipCodeNumberPart"]), str(E_v["EmployeeZipCode"]))
                        distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]] = Distances
                    
                    if (((E_v["EmployeeId"], TS_v["RelationID"]) in ClientMismatches) and (ClientMismatches[E_v["EmployeeId"], TS_v["RelationID"]] <= TS_v["UntilUtc"])):
                        ClientMismatch = 1
                    else: 
                        ClientMismatch = 0

                    if E_v["EmployeeId"] == TS_v["EmployeeID"]:
                        if (E_v["EmployeeId"], TS_v["RelationID"]) in PreviousMatches:
                            PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["NumberOfPreviousVisits"] += 1
                            DaysSinceLastVisit = (TS_v["UntilUtc"] - PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateOfLastVisit"]).days
                            PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateOfLastVisit"] = TS_v["UntilUtc"]
                        else: 
                            PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]] = {"NumberOfPreviousVisits": 0, "DateOfLastVisit": TS_v["UntilUtc"]}
                            DaysSinceLastVisit = 0
                        NumberOfMonthsLeftInContract = (E_v["ContractUntil"] - TS_v["UntilUtc"]).days / 30
                        NumberOfPreviousVisits = PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["NumberOfPreviousVisits"]
                    else: 
                        if (E_v["EmployeeId"], TS_v["RelationID"]) in PreviousMatches:
                            NumberOfPreviousVisits = PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["NumberOfPreviousVisits"] + 1
                            DaysSinceLastVisit = (TS_v["UntilUtc"] - PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateOfLastVisit"]).days
                            PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateOfLastVisit"] = TS_v["UntilUtc"]
                        else: 
                            NumberOfPreviousVisits = 0
                            DaysSinceLastVisit = 0
                        NumberOfMonthsLeftInContract = (E_v["ContractUntil"] - TS_v["UntilUtc"]).days / 30

                    if E_v["EmployeeId"] in PreviousMatches:
                        if E_v["EmployeeId"] == TS_v["EmployeeID"]:
                            if TS_v["UntilUtc"].year == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].year:
                                if TS_v["UntilUtc"].month == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].month:
                                    PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] -= TS_v["TimeSlotLength"]/60
                                    HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"]
                                else:
                                    PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                                    HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"]
                                if TS_v["UntilUtc"].isocalendar().week == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].isocalendar().week:
                                    PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] -= TS_v["TimeSlotLength"]/60
                                    HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"]
                                else: 
                                    PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                                    HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"]
                            else:
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                                HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"]
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                                HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"]
                        else:
                            if TS_v["UntilUtc"].year == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].year:
                                if TS_v["UntilUtc"].month == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].month:
                                    HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] - TS_v["TimeSlotLength"]/60
                                else:
                                    HoursLeftInMonth = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                                if TS_v["UntilUtc"].isocalendar().week == PreviousMatches[E_v["EmployeeId"]]["DateOfLastVisit"].isocalendar().week:
                                    HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] - TS_v["TimeSlotLength"]/60
                                else: 
                                    HoursLeftInWeek = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                            else:
                                HoursLeftInMonth = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                                HoursLeftInWeek = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                    else: 
                        if E_v["EmployeeId"] == TS_v["EmployeeID"]:
                            PreviousMatches[E_v["EmployeeId"]] = {"DateOfLastVisit": TS_v["UntilUtc"], "HoursLeftInMonth": E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60, "HoursLeftInWeek": E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60}
                            HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"]
                            HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"]
                        else: 
                            HoursLeftInMonth = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                            HoursLeftInWeek = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                    
                    DogAllergyMismatch = min(TS_v["HasDog"], E_v["HasDogAllergy"])
                    CatAllergyMismatch = min(TS_v["HasCat"], E_v["HasCatAllergy"])
                    OtherPetsAllergyMismatch = min(TS_v["HasOtherPets"], E_v["HasOtherPetsAllergy"])
                    SmokeAllergyMismatch = min(TS_v["Smokes"], E_v["HasSmokeAllergy"])
                    AllergyMismatch = max(DogAllergyMismatch, CatAllergyMismatch, OtherPetsAllergyMismatch, SmokeAllergyMismatch)

                    if TS_v["EmployeeID"] == E_v["EmployeeId"]:
                        Label = 1
                    else: 
                        Label = 0

                    dict[TS_k, E_v["EmployeeId"]] = {"ClientMismatch": ClientMismatch, "HoursLeftInMonth": HoursLeftInMonth, "HoursLeftInWeek": HoursLeftInWeek, "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract, "DaysSinceLastVisit": DaysSinceLastVisit, "NumberOfPreviousVisits": NumberOfPreviousVisits, "AllergyMismatch": AllergyMismatch, "Distances": Distances, "Availability": AvailabilityOutput, "Label": Label}
                else: 
                    continue
            if counter >= 1000:
                break
        return pd.DataFrame(dict).T

if __name__=="__main__":
    # extract=dataloading.read_db.ExtractData(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
    # for i in tqdm(range(100)):
    #     extract.get_data('Id','Timeslots TS')
    Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
    # Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod-2022-6-17-15-25-ANONYMOUS")
    df = Matching.main()
    # df.to_csv("C:/Users/niels/Desktop/Niels/Colleges/'21-'22 BA/Project Business Case/Business Case/test.csv")