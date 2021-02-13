# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects MP data from LRS website
"""

import requests
import time
from tqdm import tqdm
import json
from utils import db_conn

#Read parameters from `CONSTANTS.jsos` file 
with open("CONSTANTS.json" , "r") as file :
    constants = json.loads( file.read() )

#Initialise/Connect to DB
cnxn, cursor=db_conn( constants["DB_name"])

# Get Parliamentary Sitting IDs from the DB
query = "SELECT term_id FROM terms"
cursor.execute(query)
sitting_ids = [row[0] for row in cursor.fetchall()]

#Call API; Get MP data for each term
xmls = []
mps_url = "http://apps.lrs.lt/sip/p2b.ad_seimo_nariai?kadencijos_id="
for ids in tqdm(sitting_ids):
    result = requests.get(mps_url + ids)
    xmls.append(result.text)

    time.sleep(0.5)

# Parse XML; Convert to a list of dictionaries
data =[]

for index, xml in enumerate(xmls):
    term_no = str(index + 1)
    parts1 = xml.split(">")
    parts2 = [i for i in parts1 if "SeimoNarys" in i]
    parts3 = []
    for part in parts2:
        plh = []
        parts = part.split('="')
        for p in parts:
            pp = p.split('"')
            plh += pp
        parts3.append(plh)
    items  = [part[1::2] for part in parts3]
    for i in items:
        if i != []:
            web_bio = ""
            if len(i) == 10:
                web_bio = i[9]
            d = {"mp_id" : i[0],
                 "mp_name" : i[1],
                 "mp_surname" : i[2], 
                 "mp_sex" : i[3],
                 "mp_start_date" : i[4], 
                 "mp_end_date" : i[5],
                 "mp_party" : i[6], 
                 "mp_district" : i[7], 
                 "mp_term" : i[8], 
                 "mp_web_bio" : web_bio,
                 "term_id" : term_no}
            data.append(d)

#Create a blank sittings table in the DB
query1 = "DROP TABLE IF EXISTS mps"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE mps (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
mp_id TEXT NULL,
mp_name TEXT NULL,
mp_surname TEXT NULL,
mp_sex TEXT NULL,
mp_start_date TEXT NULL,
mp_end_date TEXT NULL,
mp_party TEXT NULL,
mp_district TEXT NULL,
mp_term_count TEXT NULL,
mp_web_bio TEXT NULL,
term_id TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert data into the table
query="""INSERT INTO mps (
                    mp_id,
                    mp_name,
                    mp_surname,
                    mp_sex,
                    mp_start_date,
                    mp_end_date,
                    mp_party,
                    mp_district,
                    mp_term_count,
                    mp_web_bio,
                    term_id
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['mp_id'], 
       item['mp_name'], 
       item['mp_surname'],
       item['mp_sex'],
       item['mp_start_date'],
       item['mp_end_date'],
       item['mp_party'],
       item['mp_district'],
       item['mp_surname'],
       item['mp_web_bio'],
       item["term_id"])
    cursor.execute(query, t)
cnxn.commit()

#Test
print("Test: Get all MPs with the name `Andrius`")
query = 'SELECT * FROM mps WHERE mp_name = "Andrius"'
cursor.execute(query)
rows = cursor.fetchall()

for row in rows :
    print(row[1] , row[2] , row[3])

#Close Connection to DB
cnxn.close()
 