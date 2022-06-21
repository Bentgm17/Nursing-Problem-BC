import read_db
import numpy as np
import pandas as pd
import pgeocode
from tqdm import tqdm

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
        ClientMismatches = {}
        ClientMismatchExtract = self.extract.retrieve_client_mismatches()
        for k, v in ClientMismatchExtract.to_dict(orient = "index").items():
            ClientMismatches[v["EmployeeId"], v["RelationId"]] = v["CreatedOnUTC"]
        distance_dict = self.computeDistanceDict()

        for TS_k, TS_v in tqdm(TimeSlotData.items(), total = len(TimeSlotData)):
            for E_k, E_v in EmployeeData.values():
                if ((TS_v["UntilUtc"] >= E_v["ContractFrom"]) and (TS_v["UntilUtc"] <= E_v["ContractUntil"])):
                    if (TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]) in distance_dict:
                        Distance = distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]]
                    else: 
                        Distance = self.dist.query_postal_code(str(TS_v["ZipCodeNumberPart"]), str(E_v["EmployeeZipCode"]))
                        distance_dict[TS_v["ZipCodeNumberPart"], E_v["EmployeeZipCode"]] = Distance
                    
                    if (E_v["EmployeeId"], TS_v["RelationID"]) in ClientMismatches:
                        if ClientMismatches[E_v["EmployeeId"], TS_v["RelationID"]] <= TS_v["UntilUtc"]:
                            ClientMismatch = 0
                        else:
                            ClientMismatch = 1
                    else: 
                        ClientMismatch = 1

                    NumberOfMonthsLeftInContract = (E_v["ContractUntil"] - TS_v["UntilUtc"]).days / 30
                    previous_visits = [element_v["UntilUtc"] for element_k, element_v in TimeSlotData.items() if ((TS_v["UntilUtc"] > element_v["UntilUtc"]) and (TS_v["RelationID"] == element_v["RelationID"]) and (TS_v["EmployeeID"] == element_v["EmployeeID"]))]
                    NumberOfPreviousVisits = len(previous_visits)
                    if (NumberOfPreviousVisits == 0):
                        DaysSinceLastVisit = 0
                    else:
                        DaysSinceLastVisit = (TS_v["UntilUtc"] - max(previous_visits)).days
                    DogAllergyMismatch = min(TS_v["HasDog"], E_v["HasDogAllergy"])
                    CatAllergyMismatch = min(TS_v["HasCat"], E_v["HasCatAllergy"])
                    OtherPetsAllergyMismatch = min(TS_v["HasOtherPets"], E_v["HasOtherPetsAllergy"])
                    SmokeAllergyMismatch = min(TS_v["Smokes"], E_v["HasSmokeAllergy"])
                    dict[TS_k, E_v["EmployeeId"]] = {"Distance": Distance, "DogAllergyMismatch": DogAllergyMismatch, "CatAllergyMismatch": CatAllergyMismatch, "OtherPetsAllergyMismatch": OtherPetsAllergyMismatch, "SmokeAllergyMismatch": SmokeAllergyMismatch}
                else: 
                    continue
        return dict

if __name__=="__main__":
    Matching=computeMatching()
    print(Matching.main())