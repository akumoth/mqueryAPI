import pandas as pd
import sqlite3
from sqlite3 import Error
import peewee as pw

db = pw.SqliteDatabase("../api/sql.db")
my_df = pd.read_parquet('../data/processed/api_db.parquet')

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

create_connection("../api/sql.db")

class BaseModel(pw.Model):
    class Meta:
        database = db

class Movies(BaseModel):
    id = pw.CharField()
    title = pw.CharField()
    date_added = pw.DateTimeField()
    release_year = pw.DateTimeField()
    duration_int = pw.IntegerField()
    duration_type = pw.CharField()
    rating = pw.CharField()
    score = pw.IntegerField()

db.connect()
db.create_tables([Movies])
with db.atomic():
    for idx in range(0, len(my_df), 100):
        Movies.insert_many(my_df.to_dict(orient="records")[idx:idx+100]).execute()