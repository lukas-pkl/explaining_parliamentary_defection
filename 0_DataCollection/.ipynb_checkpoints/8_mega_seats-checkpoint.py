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
from bs4 import BeautifulSoup
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


mega_jobs = ["Komiteto pirmininkė", "Komiteto pirmininkas",  "Seimo Pirmininko pavaduotoja", "Seimo Pirmininko pavaduotojas", 
            "Seimo Pirmininkas", "Seimo Pirmininkė", "Frakcijos seniūnė", "Frakcijos seniūnas", ]
mega_groups = ["Seniūnų sueiga", ]

data =[]

for index, xml in enumerate(xmls):
    term_no = sitting_ids[index]

    soup = BeautifulSoup(xml, "html.parser")
    mps = soup.find_all("seimonarys")
    for mp in mps :
        mp_id = mp["asmens_id"]
        jobs = mp.find_all("pareigos")
        for job in jobs :
            to_add = False
            if job.get("pareigos", "") != "" :
                if job["pareigos"] in mega_jobs :
                    to_add = True
            if job.get("parlamentinės_grupės_pavadinimas", "") != "" :
                if job["parlamentinės_grupės_pavadinimas"] in mega_groups :
                    to_add = True
            if to_add == True:

                plh = {"mp_id" : mp_id, 
                        "term_id" : term_no, 
                        "location" : job.get("parlamentinės_grupės_pavadinimas", "") , 
                        "position" : job.get("pareigos", ""), 
                        "date_from" : job["data_nuo"], 
                        "date_to" : job["data_iki"]
                        }
                data.append(plh)

#Create a blank sittings table in the DB
query1 = "DROP TABLE IF EXISTS mega_seats"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE mega_seats (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
mp_id TEXT NULL,
term_id TEXT NULL, 
location TEXT NULL, 
position TEXT NULL, 
date_from TEXT NULL, 
date_to TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert data into the table
query="""INSERT INTO mega_seats (
                    mp_id,
                    term_id,
                    location,
                    position,
                    date_from,
                    date_to
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['mp_id'], 
       item['term_id'], 
       item['location'],
       item['position'],
       item['date_from'],
       item['date_to'])
    cursor.execute(query, t)
cnxn.commit()


#Close Connection to DB
cnxn.close()