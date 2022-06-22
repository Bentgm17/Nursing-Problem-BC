import dataloading.read_db
import numpy as np
import pandas as pd
import pgeocode
import datetime as dt
from tqdm import tqdm
from time import time

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

    def main(self): 
        dict = {}
        EmployeeData = self.extract.retrieve_timeslot_data()
        EmployeeData = EmployeeData.to_dict(orient="index")
        TimeSlotData = self.extract.retrieve_historical_timeslot_data()
        TimeSlotData = TimeSlotData.set_index("Id").to_dict(orient="index")
        PreviousMatches = {}
        Availability = {}
        for k, v in EmployeeData.items():
            Availability[v["EmployeeId"]] = {"01":1,"02":1,"11":1,"12":1,"21":1,"22":1,"31":1,"32":1,"41":1,"42":1,'51':1,'52':1,'61':1,'62':1}
        date = 0

        ClientMismatches = {}
        ClientMismatchExtract = self.extract.retrieve_client_mismatches()
        for k, v in ClientMismatchExtract.to_dict(orient = "index").items():
            ClientMismatches[v["EmployeeId"], v["RelationId"]] = v["CreatedOnUTC"]
        distance_dict = self.computeDistanceDict()
        for TS_k, TS_v in tqdm(TimeSlotData.items(), total = len(TimeSlotData)):
            for E_k, E_v in EmployeeData.items(): 
                if date != TS_v["UntilUtc"].date():
                    for key in Availability:
                        Availability[key][str(TS_v['UntilUtc'].weekday()) + "1"] *= 0.7
                        Availability[key][str(TS_v['UntilUtc'].weekday()) + "2"] *= 0.7
                    date = TS_v["UntilUtc"].date()

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

                    if (E_v["EmployeeId"], TS_v["RelationID"]) in PreviousMatches:
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["Count"] += 1
                        DaysSinceLastVisit = (TS_v["UntilUtc"] - PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateLastVisited"]).days
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateLastVisited"] = TS_v["UntilUtc"]
                    else: 
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]] = {"Count": 0, "DateLastVisited": TS_v["UntilUtc"]}
                        DaysSinceLastVisit = 0

                    NumberOfMonthsLeftInContract = (E_v["ContractUntil"] - TS_v["UntilUtc"]).days / 30
                    NumberOfPreviousVisits = PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["Count"]

                    """
                    Dit gedeelte hieronder werkt nog niet helemaal goed. Als we dit verder gaan gebruiken moet dit nog aangepast worden.
                    """
                    if E_v["EmployeeId"] in PreviousMatches:
                        if TS_v["UntilUtc"].year == PreviousMatches[E_v["EmployeeId"]]["DateLastVisited"].year:
                            if TS_v["UntilUtc"].month == PreviousMatches[E_v["EmployeeId"]]["DateLastVisited"].month:
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] -= TS_v["TimeSlotLength"]/60
                            else:
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                            if TS_v["UntilUtc"].isocalendar().week == PreviousMatches[E_v["EmployeeId"]]["DateLastVisited"].isocalendar().week:
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] -= TS_v["TimeSlotLength"]/60
                            else: 
                                PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                        else:
                            PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"] = E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60
                            PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"] = E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60
                    else: 
                        PreviousMatches[E_v["EmployeeId"]] = {"DateLastVisited": TS_v["UntilUtc"], "HoursLeftInMonth": E_v["AverageNumberOfHoursPerMonth"] - TS_v["TimeSlotLength"]/60, "HoursLeftInWeek": E_v["NumberOfHoursPerWeek"] - TS_v["TimeSlotLength"]/60}
                    
                    HoursLeftInMonth = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInMonth"]
                    HoursLeftInWeek = PreviousMatches[E_v["EmployeeId"]]["HoursLeftInWeek"]
                    
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
        return pd.DataFrame(dict).T

if __name__=="__main__":
    # Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
    Matching=computeMatching(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod-2022-6-17-15-25-ANONYMOUS")
    df = Matching.main()
    df.to_csv("C:/Users/niels/Desktop/Niels/Colleges/'21-'22 BA/Project Business Case/Business Case/test.csv")