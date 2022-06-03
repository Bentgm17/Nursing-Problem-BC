class Employee():
        """
        A class to print employee details.
        ...

        Attributes
        ----------
        _Employee_id: str
            Client_id string
        _Address: str
            Address string
        _Contract_hours: int
            Hours in the contract int
        _Hours_left_contract: float
            Hours left in the contract float
        _Salary: float
            Salary float
        _Contract_till: Datetime
            Ending Datetime of the contract

        Methods
        -------
        __init__(_Employee_id, _Address,_Contract_hours,_Hours_left_contract,_Salary,_Contract_till)->None
            Initializes the class
        get_employee_id() -> str
            Returns the employee id
        get_address() -> str
            Returns the address
        get_contract_hours() -> int
            Returns the contract hours
        get_salary() -> float
            Returns the salary
        get_months_contract_till() -> int
            Returns the remaining number of months in the contract
        """
        def __init__(self,_Employee_id:str, _Address:str, _Contract_hours:int, _Hours_left_contract:float, _Salary:float, _Contract_till)->None:
            """
            Initializes the class
            Parameters
            ----------
            _Employee_id: str
                Employee_id string
            _Address: str
                Address string
            _Contract_hours: int
                Hours in the contract int
            _Hours_left_contract: float
                Hours left in the contract float
            _Salary: float
                Salary float
            _Contract_till: Datetime
                Ending Datetime of the contract
        
            """
            self._Employee_id=_Employee_id
            self._Address=_Address
            self._Contract_hours=_Contract_hours
            self._Hours_left_contract=_Hours_left_contract
            self._Salary=_Salary
            self._Contract_till=_Contract_till

        def get_employee_id(self)->str:
            """
            Returns the employee id
            """
            employee_id = self._Employee.get_id()
            return employee_id

        def get_address(self)->str:
            """
            Returns the address
            """
            address = self._Employee.get_address()
            return address

        def get_contract_hours(self)->int:
            """
            Returns the contract hours
            """
            contract_hours = self._Employee.get_contract_hours()
            return contract_hours
        
        def get_salary(self)->float:
            """
            Returns the salary
            """
            salary = self._Employee.get_salary()
            return salary
        
        def get_months_contract_till(self)->int:
            """
            Returns the remaining number of months in the contract
            """
            months_contract_till = self._Employee.get_months_contract_till()
            return months_contract_till