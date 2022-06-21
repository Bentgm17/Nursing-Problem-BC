import read_db
import numpy as np
import pandas as pd
import pgeocode
from tqdm import tqdm
from time import time

class computeMatching:
    def __init__(self):
        self.extract = read_db.ExtractData()
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
        EmployeeData = self.extract.retrieve_employee_data()
        EmployeeData = EmployeeData.to_dict(orient="index")
        TimeSlotData = self.extract.retrieve_historical_timeslot_data()
        TimeSlotData = TimeSlotData.set_index("Id").to_dict(orient="index")
        PreviousMatches = {}

        ClientMismatches = {}
        ClientMismatchExtract = self.extract.retrieve_client_mismatches()
        for k, v in ClientMismatchExtract.to_dict(orient = "index").items():
            ClientMismatches[v["EmployeeId"], v["RelationId"]] = v["CreatedOnUTC"]
        distance_dict = self.computeDistanceDict()

        for TS_k, TS_v in tqdm(TimeSlotData.items(), total = len(TimeSlotData)):
            for E_k, E_v in EmployeeData.items():
                if ((TS_v["UntilUtc"] >= E_v["ContractFrom"]) and (TS_v["UntilUtc"] <= E_v["ContractUntil"])):
                    if (TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]) in distance_dict:
                        Distances = distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]]
                    else: 
                        Distances = self.dist.query_postal_code(str(TS_v["ZipCodeNumberPart"]), str(E_v["EmployeeZipCode"]))
                        distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]] = Distances
                    
                    if (E_v["EmployeeId"], TS_v["RelationID"]) in ClientMismatches:
                        if ClientMismatches[E_v["EmployeeId"], TS_v["RelationID"]] <= TS_v["UntilUtc"]:
                            ClientMismatch = 0
                        else:
                            ClientMismatch = 1
                    else: 
                        ClientMismatch = 1

                    if (E_v["EmployeeId"], TS_v["RelationID"]) in PreviousMatches:
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["Count"] += 1
                        DaysSinceLastVisit = (TS_v["UntilUtc"] - PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateLastVisited"]).days
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["DateLastVisited"] = TS_v["UntilUtc"]
                    else: 
                        PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]] = {"Count": 0, "DateLastVisited": TS_v["UntilUtc"]}
                        DaysSinceLastVisit = 0

                    NumberOfMonthsLeftInContract = (E_v["ContractUntil"] - TS_v["UntilUtc"]).days / 30
                    NumberOfPreviousVisits = PreviousMatches[E_v["EmployeeId"], TS_v["RelationID"]]["Count"]
                    
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
                    
                    dict[TS_k, E_v["EmployeeId"]] = {"ClientMismatch": ClientMismatch, "HoursLeftInMonth": HoursLeftInMonth, "HoursLeftInWeek": HoursLeftInWeek, "NumberOfMonthsLeftInContract": NumberOfMonthsLeftInContract, "DaysSinceLastVisit": DaysSinceLastVisit, "NumberOfPreviousVisits": NumberOfPreviousVisits, "Distances": Distances, "DogAllergyMismatch": DogAllergyMismatch, "CatAllergyMismatch": CatAllergyMismatch, "OtherPetsAllergyMismatch": OtherPetsAllergyMismatch, "SmokeAllergyMismatch": SmokeAllergyMismatch}
                else: 
                    continue
        return dict

if __name__=="__main__":
    Matching=computeMatching()
    print(Matching.main())