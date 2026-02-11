import sqlite3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

DB = "local.db"

queries = {
    "4a": """
        SELECT sources, COUNT(*) AS document_count
        FROM CANDIDATE_SK_DOCS_MASTER
        GROUP BY sources
    """,

    "4b": """
        SELECT substr(lastmod,1,7) AS month, COUNT(*) AS count
        FROM CANDIDATE_SK_DOCS_MASTER
        WHERE lastmod IS NOT NULL
        GROUP BY month;
    """,

    "4c": """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status_code=200 THEN 1 ELSE 0 END) AS success
        FROM CANDIDATE_SK_DOCUMENT_CONTENT;
    """,

    "4d": """
        SELECT
            '/' || substr(path,1,instr(path,'/')-1) AS segment,
            COUNT(*) AS frequency
        FROM (
            SELECT substr(url,length('https://docs.snowflake.com/')+1) AS path
            FROM CANDIDATE_SK_DOCS_MASTER
        )
        WHERE instr(path,'/')>0
        GROUP BY segment
        ORDER BY frequency DESC
        LIMIT 10;
    """,

    "4e": """
        SELECT COUNT(*) AS stale_docs
        FROM CANDIDATE_SK_DOCS_MASTER
        WHERE lastmod IS NOT NULL
        AND date(lastmod) < date('now','-180 days');
    """
}

# --- Google Auth ---
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open("Task4_Analytics")

conn = sqlite3.connect(DB)

for tab, query in queries.items():

    df = pd.read_sql(query, conn)

    try:
        ws = sheet.worksheet(tab)
        ws.clear()
    except:
        ws = sheet.add_worksheet(tab, rows=100, cols=20)

    ws.update([df.columns.values.tolist()] + df.values.tolist())

conn.close()

print("Export complete!")
