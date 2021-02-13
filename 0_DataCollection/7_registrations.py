# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects votting registrations for each parliamentary vote from LRS website
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

#Create an empty table
query1 = "DROP TABLE IF EXISTS registrations"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE registrations (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
registration_id TEXT NULL ,
timestamp TEXT NULL,
mp_id TEXT NULL ,
mp_name TEXT NULL ,
mp_surname TEXT NULL ,
has_registered TEXT NULL 

);
"""

cursor.execute(query)
cnxn.commit()

#Select registration ids from votings table
query = "SELECT DISTINCT registration_id FROM votings"
cursor.execute(query)
registration_ids = [row[0] for row in cursor.fetchall()]

#Call API; get data
xmls = []
event_url = "http://apps.lrs.lt/sip/p2b.ad_sp_registracijos_rezultatai?registracijos_id="
for ids in tqdm(registration_ids):
    result = requests.get(event_url + ids)
    xmls.append(result.text)
    time.sleep(0.2)

#Parse XML; Get list of dicts
data = []
for index, item in enumerate(tqdm(xmls)):
    registration_id = registration_ids[index]
    soup = BeautifulSoup(item, "html.parser")
    ts = soup.find("seimonariųregistracija")
    timestamp = ""
    if ts != None:
        if "registracijos_laikas" in ts.attrs:
            timestamp = ts["registracijos_laikas"]
    registrations = soup.find_all("individualusregistracijosrezultatas")
    for registration in registrations :
        mp_id = registration["asmens_id"]
        mp_name = registration["vardas"]
        mp_surname = registration["pavardė"]
        has_registered = registration["ar_registravosi"]

        
        d = {"registration_id" : registration_id ,
             "timestamp" : timestamp , 
             "mp_id" : mp_id,
             "mp_name" : mp_name,
             "mp_surname" : mp_surname,
             "has_registered" : has_registered }

        data.append(d)
        
#Insert data into DB
query="""INSERT INTO registrations (
                    registration_id , 
                    timestamp , 
                    mp_id ,
                    mp_name ,
                    mp_surname ,
                    has_registered
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['registration_id'] ,
       item['timestamp'] ,
       item['mp_id'] , 
       item['mp_name'] , 
       item['mp_surname'] ,
       item['has_registered'] )

    cursor.execute(query, t)
cnxn.commit()

#Close db connection
cnxn.close()
