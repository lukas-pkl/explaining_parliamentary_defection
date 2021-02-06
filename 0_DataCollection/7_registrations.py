# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 15:08:34 2019

@author: l.pukelis
"""


import os
os.chdir("C:\\Users\\l.pukelis\\LRS_KTU\\2_attempt\\")

import requests
from bs4 import BeautifulSoup

import time
from tqdm import tqdm

import sqlite3

#%%
def db_conn():
    cnxn = sqlite3.connect('LRS_data.db')
    print(sqlite3.version)
    cursor=cnxn.cursor()
    return cnxn, cursor
cnxn, cursor=db_conn()
#%%
'''
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
#'''



#%%
query = "SELECT DISTINCT registration_id FROM votings"
cursor.execute(query)
registration_ids = [row[0] for row in cursor.fetchall()]

query = "SELECT DISTINCT registration_id FROM registrations"
cursor.execute(query)
registration_ids_in_db = [row[0] for row in cursor.fetchall()]

registration_ids_to_check = [i for i in registration_ids if i not in registration_ids_in_db]
#%%
xmls = []
event_url = "http://apps.lrs.lt/sip/p2b.ad_sp_registracijos_rezultatai?registracijos_id="
for ids in tqdm(registration_ids_to_check):
    result = requests.get(event_url + ids)
    xmls.append(result.text)

    #time.sleep(0.2)
#%%
data = []
for index, item in enumerate(tqdm(xmls)):
    registration_id = registration_ids_to_check[index]
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
        
#%%
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


