# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects votting results for each parliamentary vote from LRS website
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

#Create an empty table for votting
query1 = "DROP TABLE IF EXISTS voting_results"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE voting_results (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
mp_id TEXT NULL ,
mp_name TEXT NULL ,
mp_surname TEXT NULL ,
mp_ppg TEXT NULL ,
mp_vote TEXT NULL,
voting_id TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Select voting IDS from the DB
query = "SELECT DISTINCT voting_id FROM votings"
cursor.execute(query)
voting_ids = [row[0] for row in cursor.fetchall()]


#Call API; Get results for each vote
xmls = []
event_url = " http://apps.lrs.lt/sip/p2b.ad_sp_balsavimo_rezultatai?balsavimo_id="
for ids in tqdm(voting_ids):
    result = requests.get(event_url + ids)
    xmls.append(result.text)

    time.sleep(0.2)

#Parse XMLs; convert to list of dicts
data = []
for index, item in enumerate(tqdm(xmls)):
    voting_id = voting_ids[index]
    soup = BeautifulSoup(item, "html.parser")
    votes = soup.find_all("individualusbalsavimorezultatas")
    for vote in votes :
        mp_id = vote["asmens_id"]
        mp_name = vote["vardas"]
        mp_surname = vote["pavardė"]
        mp_ppg = vote["frakcija"]
        mp_vote = vote["kaip_balsavo"]
        
        d = {"mp_id" : mp_id,
             "mp_name" : mp_name,
             "mp_surname" : mp_surname,
             "mp_ppg" : mp_ppg, 
             "mp_vote" : mp_vote,
             "voting_id" : voting_id}
        data.append(d)
        
#Insert into DB
query="""INSERT INTO voting_results (
                    mp_id ,
                    mp_name ,
                    mp_surname ,
                    mp_ppg ,
                    mp_vote,
                    voting_id 
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['mp_id'], 
       item['mp_name'], 
       item['mp_surname'],
       item['mp_ppg'],
       item['mp_vote'] , 
       item['voting_id'])

    cursor.execute(query, t)
cnxn.commit()

#Close connection to DB;
cnxn.close()
