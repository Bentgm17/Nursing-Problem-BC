import read_db
import time

extract = read_db.ExtractData()
t1=time.time()
extract.get_contract_information_on_id('6721')
print(time.time()-t1)