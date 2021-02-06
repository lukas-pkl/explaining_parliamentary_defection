# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects the parliamentary sittings' data from LRS website
"""

import requests
import time
from tqdm import tqdm
import json
from bs4 import BeautifulSoup
from utils import db_conn



#Read parameters from `CONSTANTS.jsos` file 
with open("CONSTANTS.json" , "r") as file :
    constants = json.loads( file.read() )

#Initialise/Connect to DB
cnxn, cursor=db_conn( constants["DB_name"])

# Get seesion IDs from DB
query = "SELECT session_id FROM sessions"
cursor.execute(query)
session_ids = [row[0] for row in cursor.fetchall()]

#Call API; Get sittings for each session
xmls = []
sittings_url = "http://apps.lrs.lt/sip/p2b.ad_seimo_posedziai?sesijos_id="
for ids in tqdm(session_ids):
    result = requests.get(sittings_url + ids)
    xmls.append(result.text)

    time.sleep(0.2)

#Parse XMLs; Transfrom data to a list of dicts
data = []
for index, item in enumerate(xmls):
    session_id = session_ids[index]
    soup = BeautifulSoup(item, "html.parser")
    sittings = soup.find_all("seimoposėdis")
    for sitting in sittings:
        sitting_id = sitting["posėdžio_id"]
        sitting_number = sitting["numeris"]
        sitting_start_time = sitting["pradžia"]
        sitting_end_time = sitting["pabaiga"]
        sitting_type = sitting["tipas"]
        
        d ={"sitting_id" : sitting_id,
            "sitting_number" : sitting_number,
            "sitting_start_time" : sitting_start_time,
            "sitting_end_time" : sitting_end_time,
            "sitting_type" : sitting_type,
            "session_id" : session_id}
        data.append(d)

#Create blank sittings table
query1 = "DROP TABLE IF EXISTS sittings"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE sittings (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
sitting_id TEXT NULL,
sitting_number TEXT NULL,
sitting_start_time TEXT NULL,
sitting_end_time TEXT NULL,
sitting_type TEXT NULL,
session_id TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert Data 
query="""INSERT INTO sittings (
                    sitting_id ,
                    sitting_number ,
                    sitting_start_time ,
                    sitting_end_time ,
                    sitting_type,
                    session_id 
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['sitting_id'], 
       item['sitting_number'], 
       item['sitting_start_time'],
       item['sitting_end_time'],
       item['sitting_type'],
       item['session_id'] )

    cursor.execute(query, t)
cnxn.commit()

#Close DB connection
cnxn.close()

