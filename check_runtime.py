import read_db
import time

extract = read_db.ExtractData()
t1=time.time()
print(extract.get_contract_information_on_id('7636'))
print(time.time()-t1)
extract.close_conn()
t1=time.time()
print(extract.get_contract_information_on_id_2('7636'))
print(time.time()-t1)