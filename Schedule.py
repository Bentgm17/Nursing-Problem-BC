class Schedule():
        """
        A class to print details of the schedule.
        ...

        Attributes
        ----------
        _from: Datetime
            Datetime object of the start of the schedule
        _to: Datetime
            Datetime object of the end of the schedule

        Methods
        -------
        __init__(_from, _to)->None
            Initializes the class
        init_employees() -> None
            Initializes the employees
        init_appointments() -> None
            Initializes the appointments
        initialize() -> None
            Initializes the schedule
        get_appointments() -> list[appointments]
            Returns the appointments
        """
        def init_employees(self):
            """
            Initializes the employees
            """
            pass

        def init_appointments(self):
            """
            Initializes the appointments
            """
            pass
    
        def initialize(self,_from:Datetime, _to:Datetime):
            """
            Initializes the schedule
            """
            self._from=_from
            self._to=_to            
    
        def get_appointments(self):
            """
            Returns the appointments
            """
            appointments = []
            return appointments
        