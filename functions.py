import read_db
import time

def distance_customer:
    """
    Calculate the distance between two points
    """
    distances = []
    extract = read_db.ExctractData()
    df = extract.getData("EmployeeID,RelationID", "Timeslots")
    for i, row in df.iterrows():
        distances.append()

if __name__ = "__main__":
