# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects parliamentary term data from LRS website
"""

import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
import json
from utils import db_conn

#Read parameters from `CONSTANTS.jsos` file 
with open("CONSTANTS.json" , "r") as file :
    constants = json.loads( file.read() )

#Initialise/Connect to DB
cnxn, cursor=db_conn( constants["DB_name"])

#Get parliamentary sittings data from the API
sittings_url = "http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos"
result = requests.get(sittings_url)
soup = BeautifulSoup(result.text, "lxml")

# Parse XML; Convert to a list of dictionaries
sittings = soup.find_all("seimokadencija")

data = []
for s in sittings:
    term_name = ""
    try:
        term_name = s["pavadinimas"]
    except Exception as e:
        print(e)
    term_id = ""
    try:
        term_id = s["kadencijos_id"]
    except Exception as e:
        print(e)
    term_start_date = ""
    try:
        term_start_date = s["data_nuo"]
    except Exception as e:
        print(e)
    term_end_date = ""
    try:
        term_end_date = s["data_iki"]
    except Exception as e:
        print(e)
    d = {"term_name" : term_name,
         "term_id" : term_id,
         "term_start_date" : term_start_date,
         "term_end_date" : term_end_date}
    data.append(d)


#Create a blank parliametary terms table in the DB
query1 = "DROP TABLE IF EXISTS terms"
cursor.execute(query1)
cnxn.commit()

query = """
CREATE TABLE terms (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
term_name TEXT NULL,
term_id TEXT NULL,
term_start_date TEXT NULL,
term_end_date TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert data into table
query="INSERT INTO terms (term_name, term_id, term_start_date, term_end_date) VALUES (?, ?, ?, ?)"
for item in tqdm(data):
    t=(item['term_name'], item['term_id'], item['term_start_date'], item['term_end_date'])
    cursor.execute(query, t)
cnxn.commit()

#Close the connection 
cnxn.close()