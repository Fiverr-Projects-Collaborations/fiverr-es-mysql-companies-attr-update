from sqlalchemy import create_engine
import requests
import json
import pandas as pd
from datetime import datetime

print("Started process: ", datetime.now())
db_connection_str = 'mysql+pymysql://XXXX:XXXX@XXXX/XXXX'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('SELECT id, listing_order FROM nummerbran.companies', con=db_connection)
print("Finished getting data from database: ", datetime.now())
finalList = []
updateStr = []
count = 0
rowCount = 0
print("Started building json: ", datetime.now())
for index, row in df.iterrows():
    count = count + 1
    rowCount = rowCount + 1
    if count % 5000 == 0 or rowCount == df['id'].size:
        count = 0
        finalList.append(updateStr)
        updateStr = []
    updateStr.append(json.dumps({"update": {"_id": int(row['id']), "_index": "sqldata_t"}}))
    updateStr.append(json.dumps({"doc": {"listing_order": int(row['listing_order'])}}))
finalList.append(updateStr)
print("Finished building json: ", datetime.now())

for list in finalList:
    data = "\n".join(list) + "\n"
    r = requests.post(url="http://XXXX:9200/_bulk", data=data, headers={"content-type": "application/json"})
print("Finished pushing data to es: ", datetime.now())
print("Finished process: ", datetime.now())
