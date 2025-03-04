#importing libraries 
import sqlite3 as sq
import pandas as pd 
import pandas.io.sql as pds
'''Our first step will be to create a connection to our SQL database.
   A few common SQL databases used with Python include:

    Microsoft SQL Server
    Postgres
    MySQL
    AWS Redshift
    AWS Aurora
    Oracle DB
    Terradata
    Db2 Family
    Many, many others
    Each of these databases will require a slightly different setup, 
    and may require credentials (username & password), tokens,
    or other access requirements.We'll be using sqlite3 to connect to our database,
    but other connection packages include:

    SQLAlchemy (most common)
    psycopg2
    MySQLdb

'''
# let's download our file 
import requests
import os 
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-ML0232EN-SkillsNetwork/asset/classic_rock.db"
destination_folder = "data" #folder name 
os.makedirs(destination_folder,exist_ok=True) # performs the function similar to
# makedir
file_path = os.path.join(destination_folder,"classic_rock.db")
response = requests.get(url) # sends an http get request 
with open(file_path,"wb") as file:
    file.write(response.content)
print(f"Download complete saved at {file_path}")