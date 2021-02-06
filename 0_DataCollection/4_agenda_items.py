# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects agenda items for each parliamentary sitting from LRS website
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


# Get sitting IDs from DB
query = "SELECT sitting_id FROM sittings"
cursor.execute(query)
sitting_ids = [str(row[0]) for row in cursor.fetchall()]

#Call API; Get agenda items for each sitting
xmls = []
sittings_url = "https://apps.lrs.lt/sip/p2b.ad_seimo_pos_darb?posedzio_id="
for ids in tqdm(sitting_ids):
    result = requests.get(sittings_url + ids)
    xmls.append(result.text)

    time.sleep(0.2)

#Parse XML; transfrom to list of dicts
data = []
for index, item in enumerate(xmls):
    sitting_id = sitting_ids[index]
    soup = BeautifulSoup(item, "html.parser")
    issues = soup.find_all("darbotvarkėsklausimas")
    for issue in issues:
        issue_number = issue["numeris"]
        issue_name = issue["pavadinimas"]
        issue_stages = issue.find_all("klausimostadija")
        for stage in issue_stages:
            stage_id = stage["darbotvarkės_klausimo_id"]
            stage_name = stage["pavadinimas"]

            d ={"stage_id" : stage_id,
                "stage_name" : stage_name,
                "issue_name" : issue_name,
                "issue_number" : issue_number,
                "sitting_id" : sitting_id}
            data.append(d)

#Create a blank Agenda items table
query1 = "DROP TABLE IF EXISTS agenda_items"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE agenda_items (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
stage_id TEXT NULL,
stage_name TEXT NULL,
issue_name TEXT NULL,
issue_number TEXT NULL,
sitting_id TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert data
query="""INSERT INTO agenda_items (
                    stage_id ,
                    stage_name ,
                    issue_name ,
                    issue_number ,
                    sitting_id 
                    ) 
                    VALUES (?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['stage_id'], 
       item['stage_name'], 
       item['issue_name'],
       item['issue_number'],
       item['sitting_id'])

    cursor.execute(query, t)
cnxn.commit()

#Close DB connection
cnxn.close()