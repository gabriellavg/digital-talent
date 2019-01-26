# Program untuk mengambil data dari tabel BigQuery dan memasukkannya ke local DB

from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("./bq-client/usgkg-3-459893c3bd79.json")
project_id = "usgkg-3"
client =  bigquery.Client(credentials = credentials, project = project_id)


print("Do query job...")
query_job = client.query("""
    SELECT 
        GKGRECORDID, DATE, DocumentIdentifier, V2Tone
    FROM 
        `retrieve-7-new.gkg.tab`
    """)
query_job.allowLargeResults = True
results = query_job.result()
print("Result received.")

# Koneksi ke MariaDB
print("Connecting to DB...")
host = "127.0.0.1"
port = "3306"
user = "root"
password = ""
database = "bigdata_project"
conn = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + database)
print("Connected.")

# Insert hasil query ke MariaDB
print("Inserting result to DB...")
i = 1
for r in results :
    conn.execute("INSERT INTO us_gkg (id, date, url, tone) VALUES (%s, %s, %s, %s)", (r.GKGRECORDID, r.DATE, r.DocumentIdentifier, r.V2Tone))
    print("Insert success: ", i)
    i += 1
print("Finished.")
