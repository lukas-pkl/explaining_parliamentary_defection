# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects votting records for each parliamentary agenda item from LRS website
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

#Select agenda items' IDs from the DB 
query = "SELECT stage_id FROM agenda_items"
cursor.execute(query)
agenda_items = [row[0] for row in cursor.fetchall()]

#query = "SELECT stage_id FROM votings"
#cursor.execute(query)
#agenda_item_ids_in_db = [row[0] for row in cursor.fetchall()]

#agenda_items_to_check = [i for i in agenda_item_ids if i not in agenda_item_ids_in_db]


# Call API for each agenda item;

xmls = []
event_url = "http://apps.lrs.lt/sip/p2b.ad_sp_klausimo_svarstymo_eiga?darbotvarkes_klausimo_id="
for index, ids in enumerate(tqdm(agenda_items)):  
    result = requests.get(event_url + ids)
    xmls.append(result.text)

    time.sleep(0.2)


# Check if they contain a vote;
# If so, get vote data;

data = []
for index, item in enumerate(tqdm(xmls)):
    agenda_item_id = agenda_items[index]
    soup = BeautifulSoup(item, "html.parser")
    events = soup.find_all("svarstymoeigosįvykis")
    registration_id = ""
    for event in events:
        if "registracijos_id" in event.attrs:
                registration_id = event["registracijos_id"]
        if "balsavimo_id" in event.attrs:
            voting_id = event["balsavimo_id"]
            time_stamp = event["laikas_nuo"]
            
            d = {"stage_id" : agenda_item_id,
                 "registration_id" : registration_id , 
                 "voting_id" : voting_id ,
                 "time" : time_stamp}
            data.append(d)

#Create a blank table for vottings
query1 = "DROP TABLE IF EXISTS votings"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE votings (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
stage_id TEXT NULL,
registration_id TEXT NULL,
voting_id TEXT NULL , 
timestamp TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert data
query="""INSERT INTO votings (
                    stage_id ,
                    registration_id ,
                    voting_id , 
                    timestamp
                    ) 
                    VALUES (? , ? , ? , ?)"""
                    
for item in tqdm(data):
    t=(item['stage_id'], 
       item['registration_id'] ,
       item['voting_id'], 
       item["time"])

    cursor.execute(query, t)
cnxn.commit()

#Close connection 

cnxn.close()

