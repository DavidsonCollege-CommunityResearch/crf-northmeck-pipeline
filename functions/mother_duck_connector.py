# ==============================================================================
# Database Configuration Module
# Author: Paul Park, Gemini Code
# Objective: Centralize MotherDuck connection logic and credentials
# ==============================================================================

import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def get_md_connection():
    """
    Sets the MotherDuck token and returns an active database connection.
    """
    print("☁️ Connecting to MotherDuck Cloud (nmidw)...")
    con = duckdb.connect("md:")
    con.execute("CREATE DATABASE IF NOT EXISTS nmidw;")
    con.execute("USE nmidw;") 
    
    return con


