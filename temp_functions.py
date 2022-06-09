import read_db
import pandas as pd
pd.options.mode.chained_assignment = None       # Used to suppress unnecessary warnings
import time
from tqdm import tqdm

class functions():

    def __init__(self):
        self.df=None
        self.dict={}

    def set_dataframe(self):
        extract = read_db.ExtractData()
        self.df = extract.get_timeslots_info()

    def time_since_last_visit(employee_id, relation_id):
        """
        Calculate the time since the last visit
        """
        extract = read_db.ExtractData()
        df = extract.get_data("TimeSlotType,EmployeeID,RelationID,UntilUtc", "Timeslots")
        df = df[df["TimeSlotType"] == 0]
        df = df[df["EmployeeID"] == employee_id]
        df = df[df["RelationID"] == relation_id]
        df.sort_values(by="UntilUtc", inplace=True)
        df["DaysSinceLastVisit"] = [0] + [(df["UntilUtc"].iloc[i]-df["UntilUtc"].iloc[i-1]).days for i in range(1, len(df))]
        df["NumberOfPreviousVisits"] = [i for i in range(len(df))]
        df.sort_index(inplace=True)
        print(df)
        return 0

    def remaining_availability(timeslot_id):
        """
        Calculate the remaining availability
        """
        extract = read_db.ExtractData()
        TimeSlots = extract.get_data("id,TimeSlotType,EmployeeID,RelationID,FromUtc,UntilUtc", "Timeslots")
        Employments = extract.get_data("Id,EmployeeID","Employments")
        EmployeeContracts = extract.get_data("EmploymentId,NumberOfHoursPerWeek,AverageNumberOfHoursPerMonth,FromUtc,UntilUtc", "EmployeeContracts")

        TimeSlots = TimeSlots[TimeSlots["TimeSlotType"] == 0]
        TimeSlots.drop_duplicates(subset = ["EmployeeID","RelationID","FromUtc","UntilUtc"], inplace=True)
        timeslot = TimeSlots[TimeSlots["id"] == timeslot_id].iloc[0]
        TimeSlots = TimeSlots[TimeSlots["EmployeeID"] == timeslot["EmployeeID"]]

        WeeklyTimeSlots = TimeSlots[(pd.to_datetime(TimeSlots["UntilUtc"]).dt.isocalendar().week == timeslot["UntilUtc"].isocalendar().week) & (pd.to_datetime(TimeSlots["UntilUtc"]).dt.year == timeslot["UntilUtc"].year)]
        WeeklyTimeSlots["AppointmentDuration"] = (WeeklyTimeSlots["UntilUtc"] - WeeklyTimeSlots["FromUtc"]).dt.total_seconds()/3600
        WeeklyTimeSlots.sort_values(by="FromUtc", inplace=True)
        WeeklyTimeSlots["HoursWorkedThisWeek"] = [WeeklyTimeSlots['AppointmentDuration'].iloc[:i].sum() for i in range(1,len(WeeklyTimeSlots)+1)]
        HoursWorkedThisWeek = WeeklyTimeSlots[WeeklyTimeSlots["id"] == timeslot_id]["HoursWorkedThisWeek"]

        MonthlyTimeSlots = TimeSlots[(pd.to_datetime(TimeSlots["UntilUtc"]).dt.month == timeslot["UntilUtc"].month) & (pd.to_datetime(TimeSlots["UntilUtc"]).dt.year == timeslot["UntilUtc"].year)]
        MonthlyTimeSlots["AppointmentDuration"] = (MonthlyTimeSlots["UntilUtc"] - MonthlyTimeSlots["FromUtc"]).dt.total_seconds()/3600
        MonthlyTimeSlots.sort_values(by="FromUtc", inplace=True)
        MonthlyTimeSlots["HoursWorkedThisMonth"] = [MonthlyTimeSlots['AppointmentDuration'].iloc[:i].sum() for i in range(1,len(MonthlyTimeSlots)+1)]
        TimeSlots = MonthlyTimeSlots[MonthlyTimeSlots["id"] == timeslot_id]
        TimeSlots["HoursWorkedThisWeek"] = HoursWorkedThisWeek

        df = pd.merge(TimeSlots, Employments, on="EmployeeID")
        df = pd.merge(df, EmployeeContracts, left_on="Id", right_on="EmploymentId")
        df = df[df["FromUtc_x"] >= df["FromUtc_y"]]
        df = df[(df["FromUtc_x"] <= df["UntilUtc_y"]) | (df["UntilUtc_y"].isnull())]
        df["NumberOfMonthsLeftInContract"] = (df["UntilUtc_y"] - df["FromUtc_x"]).dt.days/30
        df["NumberOfHoursLeftInMonth"] = df["AverageNumberOfHoursPerMonth"] - df["HoursWorkedThisMonth"]
        df["NumberOfHoursLeftInWeek"] = df["NumberOfHoursPerWeek"] - df["HoursWorkedThisWeek"]
        df.drop(["TimeSlotType", "Id", "EmploymentId", "AppointmentDuration", "FromUtc_x","UntilUtc_x","FromUtc_y","UntilUtc_y","AverageNumberOfHoursPerMonth","NumberOfHoursPerWeek"], axis=1, inplace=True)
        print(df)
        return 0

    def main(self):
        self.set_dataframe()
        temp = {}
        for i,row in tqdm(self.df.iterrows()):
            key = row["EmployeeID"], row["RelationID"]
            if key in temp:
                DaysSinceLastVisit = (row["UntilUtc"] - temp[key]["DateOfLastVisit"]).days
                NumberOfVisits = temp[key]["NumberOfVisits"] + 1
                temp[key] = {"DateOfLastVisit": row["UntilUtc"], 
                             "NumberOfVisits": NumberOfVisits,}
            else:
                DateOfLastVisit = row["UntilUtc"]
                NumberOfVisits = 0
                temp[key] = {"DateOfLastVisit": DateOfLastVisit, "NumberOfVisits": NumberOfVisits}

            NumberOfMonthsLeftInContract = (row["ContractUntil"] - row["UntilUtc"]).days/30

            """
            hours_left=self.hours_left
            self.dict[timeslotId]={hours left:hours_left,contact:....}
            of
            self.new_df.append([hours_left,contractdetails,......])
            if date day of the month>contract:
                self.hours_left=employee_contract_hours
            else:
                self.hours_left-=appointment_hours
            """
        print(temp[7373, 19924])
            
            

if __name__ == "__main__":
    #remaining_availability(189)
    #functions.remaining_availability(315436)
    functions().main()