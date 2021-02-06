# -*- coding: utf-8 -*-

"""
Created: 19 06 2019 
Edited: 06 02 2021

Author: LukasP

Utility functions for parliamentary defections project data colection
"""
import sqlite3

def db_conn( db_name ):
    """
    Connects to SQLite DB;
    Creates DB if it does not exist
    """
    cnxn = sqlite3.connect( db_name ) 
    print(sqlite3.version)
    cursor=cnxn.cursor()
    return cnxn, cursor