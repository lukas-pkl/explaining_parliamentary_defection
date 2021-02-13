# -*- coding: utf-8 -*-
"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Script collects Parliamentary sessions' data from LRS website
"""

import requests
from bs4 import BeautifulSoup
import time
import json
from tqdm import tqdm
from utils import db_conn

#Read parameters from `CONSTANTS.jsos` file 
with open("CONSTANTS.json" , "r") as file :
    constants = json.loads( file.read() )

#Initialise/Connect to DB
cnxn, cursor=db_conn( constants["DB_name"])

#Call API; Get all sessions
session_url = "http://apps.lrs.lt/sip/p2b.ad_seimo_sesijos?ar_visos=T"
result = requests.get(session_url)
xml = result.text
soup = BeautifulSoup(xml, "lxml")

#Parse XMLs get session data
terms = soup.find_all("seimokadencija")
data = []
for term in terms:
    term_id = term["kadencijos_id"]
    sessions = term.find_all("seimosesija")
    for session in sessions:
        session_id = session["sesijos_id"]
        session_number = session["numeris"]
        session_name = session["pavadinimas"]
        session_start_date = session["data_nuo"]
        session_end_date = session["data_iki"]
        
        d = {"session_id" : session_id,
             "session_number" : session_number,
             "session_name" : session_name,
             "session_start_date" : session_start_date,
             "session_end_date" : session_end_date,
             "term_id" : term_id}
        data.append(d)

#Create a blank session table
query1 = "DROP TABLE IF EXISTS sessions"
cursor.execute(query1)
cnxn.commit()

query="""
CREATE TABLE sessions (
id INTEGER PRIMARY KEY AUTOINCREMENT ,
session_id TEXT NULL,
session_number TEXT NULL,
session_name TEXT NULL,
session_start_date TEXT NULL,
session_end_date TEXT NULL,
term_id TEXT NULL
);
"""

cursor.execute(query)
cnxn.commit()

#Insert session data 
query="""INSERT INTO sessions (
                    session_id ,
                    session_number ,
                    session_name ,
                    session_start_date ,
                    session_end_date ,
                    term_id 
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
                    
for item in tqdm(data):
    t=(item['session_id'], 
       item['session_number'], 
       item['session_name'],
       item['session_start_date'],
       item['session_end_date'],
       item['term_id'] )

    cursor.execute(query, t)
cnxn.commit()

#Close connection 
cnxn.close()