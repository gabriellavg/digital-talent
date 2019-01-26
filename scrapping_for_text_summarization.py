# Program untuk melakukan scrapping berdasarkan URL yang diberikan
# Pada halaman web yang bersesuaian dengan URL tersebut akan dilakukan text summarization 
# dan hasilnya disimpan ke local DB

import pandas as pd
from sqlalchemy import create_engine
import urllib.request
import http.client
from newspaper import Article
from newspaper.article import ArticleException
import re

# Bagian koneksi DB
host = 'localhost'
port = '3306'
username = 'root'
password = ''
database = 'gdelt'

engine = create_engine('mysql+pymysql://' + username + ':' + password + '@' 
                       + host + ':' + port + '/' + database)

def run(sql):
    df = pd.read_sql_query(sql, engine)
    return df

# Bagian scrapping
count = 0
rows = engine.execute("SELECT id, url FROM `post20jan`").fetchall()
for row in rows :
    
    count += 1
    evtId = row[0]
    url = row[1]
    print("-----" + str(count) + "-----")
    print(url)
    
    try:
        urllib.request.urlopen(url)
        
        # Text summarization menggunakan library newspaper
        article = Article(url)
        article.download()
        article.parse() 
        article.nlp()
        
        summary = article.summary
        summary = re.sub("[^a-z]+", " ", summary.lower())
        print("SUMMARY:\n", summary)        
        
        # Simpan summary ke local DB       
        engine.execute("UPDATE `post20jan` SET summary = '" 
                       + summary + "' WHERE id = '" + evtId + "'")
        print("success")
        
    except (urllib.error.HTTPError, ValueError, urllib.error.URLError, 
            ArticleException, http.client.HTTPException, TimeoutError, 
            ConnectionResetError) as e :
        print(e)