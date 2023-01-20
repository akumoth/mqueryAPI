import sys

from fastapi import FastAPI, HTTPException, Response
import pandas as pd
import json

version = f"{sys.version_info.major}.{sys.version_info.minor}"

app = FastAPI()

df = pd.read_parquet("api_db.parquet")
df.duration_int = df.duration_int.astype(int)
df.release_year = df.release_year.astype(int)
df.score = df.score.astype(int)

@app.get("/")
async def read_root():
    message = f"Hello world! Welcome to the first version of MQueryAPI! From FastAPI & Uvicorn, with love. Using Python {version}"
    return {"message": message}

@app.get("/mqueryAPI_v1/get_word_count/")
async def get_word_count(platform, keyword):
    wordcount = (
        df.where(
            (df.id.str.startswith(platform[0])) &
            (df.title.str.contains(keyword))
        )
        .dropna()
        .count()[1]
    )
    return {'platform' : platform, 'count': float(wordcount)}

@app.get("/mqueryAPI_v1/get_score_count/")
async def get_score_count(platform, score, year):
    scorecount = (
        df.where(
            (df.id.str.startswith(platform[0])) & 
            (df.score == int(score)) & 
            (df.release_year == int(year))
        )
        .dropna()
        .count()[1]
    )
    return {'platform' : platform, 'count': float(scorecount)}

@app.get("/mqueryAPI_v1/get_second_score/")
async def get_second_score(platform):
    secondscore = (
        df.where(
            (df.id.str.startswith(platform[0])) & 
            (df.duration_type.str.contains('min'))
        )
        .dropna()
        .sort_values(
            by=['score','title'],
            ascending=[False,True]
        )
        .reset_index(drop=True)
    )
    return {'title' : secondscore.title[1], 'score' : float(secondscore.score[1])}

@app.get("/mqueryAPI_v1/get_longest/")
async def get_longest(platform,duration_type,year):
    longest = (
        df.where(
            (df.id.str.startswith(platform[0])) & 
            (df.duration_type.str.contains(duration_type)) & 
            (df.release_year == int(year)) 
        )
        .dropna()
        .sort_values(
            by='duration_int',
            ascending=False
        )
        .reset_index(drop=True)
    )
    return {'title' : longest.title[0], 'duration' : float(longest.duration_int[0]), 'duration_type' : duration_type}

@app.get("/mqueryAPI_v1/get_rating_count/")
async def get_rating_count(rating):
    ratingcount = (
        df.where(
            (df.rating.str.contains(rating))
        )
        .dropna()
        .count()[1]
    )
    return {'rating': rating, 'count' : float(ratingcount)}
