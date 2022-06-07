class RecurringRelations():
        """
        A class to print details of the recurring relations.
        ...

        Attributes
        ----------
        _Client: obj
            Client object
        _Employee: obj
            Employee object

        Methods
        -------
        __init__(_Client, _Employee)->None
            Initializes the class
        get_client() -> obj
            Returns the client
        get_employee() -> obj
            Returns the employee
        get_duration() -> int
            Returns the duration        # in weeks/months?
        get_last_visit() -> Datetime
            Returns the date of last visit
        """
        def __init__(self,_Client:object, _Employee:object)->None:
            """
            Initializes the class
            Parameters
            ----------
            _Client: obj
                Client object
            _Employee: obj
                Employee object
            """
            self._Client=_Client
            self._Employee=_Employee

        def get_client(self)->object:
            """
            Returns the client
            """
            client = self._Client
            return client
        
        def get_employee(self)->object:
            """
            Returns the employee
            """
            employee = self._Employee
            return employee

        def get_duration(self)->int:
            """
            Returns the duration
            """
            duration = 0
            return duration
        
        def get_last_visit(self)->Datetime:
            """
            Returns the date of last visit
            """
            last_visit = 0
            return last_visit
        


