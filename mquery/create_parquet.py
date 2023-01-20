import pandas as pd
months = {
    'January': '1',
    'February': '2',
    'March': '3',
    'April': '4',
    'May': '5',
    'June': '6',
    'July': '7',
    'August': '8',
    'September': '9',
    'October': '10',
    'November': '11',
    'December': '12'
}
def textDateToNumerical(x):
    if (type(x)) == str:
        x.replace(',','')
        a = x.split()
        a[0] = months[a[0]]
        a[0] = a[0].rjust(2,'0')
        a[1] = a[1].rjust(2,'0')
        return '-'.join([a[2],a[0],a[2]])
    else:
        return x

# Inicialización de los dataset

amazon_prime_df = pd.read_csv('../data/raw/amazon_prime_titles-score.csv')
disney_plus_df = pd.read_csv('../data/raw/disney_plus_titles-score.csv')
hulu_df = pd.read_csv('../data/raw/hulu_titles-score.csv')
netflix_df = pd.read_csv('../data/raw/netflix_titles-score.csv')
target_df = pd.DataFrame(columns=['id','title','date_added','release_year','duration_int','duration_type','rating','score'])

# Eliminación de valores duplicados

amazon_prime_df.drop_duplicates(inplace=True)
disney_plus_df.drop_duplicates(inplace=True)
hulu_df.drop_duplicates(inplace=True)
netflix_df.drop_duplicates(inplace=True)

# Campo id

amazon_prime_df.show_id = 'a' + amazon_prime_df.show_id.astype(str)
disney_plus_df.show_id = 'd' + disney_plus_df.show_id.astype(str)
hulu_df.show_id = 'h' + hulu_df.show_id.astype(str)
netflix_df.show_id = 'n' + netflix_df.show_id.astype(str)
target_df.id = pd.concat([amazon_prime_df.show_id,disney_plus_df.show_id,hulu_df.show_id,netflix_df.show_id],axis=0)
target_df = target_df.reset_index(drop=True)

# Campo title

amazon_prime_df.title = amazon_prime_df.title.map(str.lower)
disney_plus_df.title = disney_plus_df.title.map(str.lower)
hulu_df.title = hulu_df.title.map(str.lower)
netflix_df.title = netflix_df.title.map(str.lower)
target_df.title = pd.concat([amazon_prime_df.title,disney_plus_df.title,hulu_df.title,netflix_df.title],axis=0).reset_index(drop=True)

# Campo date_added

amazon_prime_df.date_added = amazon_prime_df.date_added.map(textDateToNumerical)
disney_plus_df.date_added = disney_plus_df.date_added.map(textDateToNumerical)
hulu_df.date_added = hulu_df.date_added.map(textDateToNumerical)
netflix_df.date_added = netflix_df.date_added.map(textDateToNumerical)
target_df.date_added = pd.concat([amazon_prime_df.date_added,disney_plus_df.date_added,hulu_df.date_added,netflix_df.date_added],axis=0).reset_index(drop=True)
target_df.date_added.fillna('00-00-0000',inplace=True)

# Campo release_year

target_df.release_year = pd.concat([amazon_prime_df.release_year,disney_plus_df.release_year,hulu_df.release_year,netflix_df.release_year],axis=0).reset_index(drop=True)

# Campo rating

# Amazon

amazon_prime_df.rating = amazon_prime_df.rating.dropna().apply(lambda x: '18+' if '18' in x else x)

amazon_prime_df.rating = amazon_prime_df.rating.dropna().apply(lambda x: '13+' if '13' in x else x)
amazon_prime_df.rating = amazon_prime_df.rating.str.replace('13+', 'PG-13', regex=False)

amazon_prime_df.rating = amazon_prime_df.rating.dropna().apply(lambda x: '16+' if '16' in x else x)
amazon_prime_df.rating = amazon_prime_df.rating.str.replace('ALL_AGES', 'G')

amazon_prime_df.rating = amazon_prime_df.rating.str.replace('ALL', 'G')
amazon_prime_df.rating = amazon_prime_df.rating.str.replace('NC-17', '18+')
amazon_prime_df.rating = amazon_prime_df.rating.dropna().apply(lambda x: 'NOT RATED' if 'NR' in x else x)
amazon_prime_df.rating = amazon_prime_df.rating.str.replace('NOT RATED', 'NOT_RATE')

amazon_prime_df.rating.fillna('G',inplace=True)

# Disney Plus

disney_plus_df.rating = disney_plus_df.rating.dropna().apply(lambda x: '13+' if '13' in x else x)
disney_plus_df.rating = disney_plus_df.rating.str.replace('13+', 'PG-13', regex=False)
disney_plus_df.rating.fillna('G',inplace=True)

# Hulu

hulu_df = hulu_df.assign(duration=hulu_df.rating.where(hulu_df.rating.str.contains('min'),hulu_df.duration))
hulu_df = hulu_df.assign(duration=hulu_df.rating.where(hulu_df.rating.str.contains('Seasons'),hulu_df.duration))
hulu_df.rating = hulu_df.rating.dropna().apply(lambda x: pd.np.nan if 'min' in x else x)
hulu_df.rating = hulu_df.rating.dropna().apply(lambda x: pd.np.nan if 'Season' in x else x)
hulu_df.rating = hulu_df.rating.dropna().apply(lambda x: 'NOT RATED' if 'NR' in x else x)
hulu_df.rating.fillna('G',inplace=True)

# Netflix

netflix_df = netflix_df.assign(duration=netflix_df.rating.where(netflix_df.rating.str.contains('min'),netflix_df.duration))
netflix_df.rating = netflix_df.rating.dropna().apply(lambda x: pd.np.nan if 'min' in x else x)

netflix_df.rating = netflix_df.rating.dropna().apply(lambda x: 'NOT RATED' if 'NR' in x else x)
netflix_df.rating = netflix_df.rating.dropna().apply(lambda x: 'NOT RATED' if 'UR' in x else x)

netflix_df.rating = netflix_df.rating.str.replace('NC-17', '18+')
netflix_df.rating.fillna('G',inplace=True)

target_df.rating = pd.concat([amazon_prime_df.rating,disney_plus_df.rating,hulu_df.rating,netflix_df.rating],axis=0).reset_index(drop=True)

# Columna duration_int y duration_type

amazon_prime_df = amazon_prime_df.assign(duration_int=amazon_prime_df.duration.str.split().apply(lambda x: x[0]))
disney_plus_df = disney_plus_df.assign(duration_int=disney_plus_df.duration.str.split().apply(lambda x: x[0]))
hulu_df = hulu_df.assign(duration_int=hulu_df.duration.dropna().str.split().apply(lambda x: x[0]))
netflix_df = netflix_df.assign(duration_int=netflix_df.duration.str.split().apply(lambda x: x[0]))

amazon_prime_df = amazon_prime_df.assign(duration_type=amazon_prime_df.duration.str.split().apply(lambda x: x[1]))
amazon_prime_df.duration_type = amazon_prime_df.duration_type.map(str.lower)
amazon_prime_df.duration_type = amazon_prime_df.duration_type.str.replace('seasons', 'season')

disney_plus_df = disney_plus_df.assign(duration_type=disney_plus_df.duration.str.split().apply(lambda x: x[1]))
disney_plus_df.duration_type = disney_plus_df.duration_type.map(str.lower)
disney_plus_df.duration_type = disney_plus_df.duration_type.str.replace('seasons', 'season')

hulu_df = hulu_df.assign(duration_type=hulu_df.duration.dropna().str.split().apply(lambda x: x[1]))
hulu_df.duration_type = hulu_df.duration_type.dropna().map(str.lower)
hulu_df.duration_type = hulu_df.duration_type.str.replace('seasons', 'season')

netflix_df = netflix_df.assign(duration_type=netflix_df.duration.str.split().apply(lambda x: x[1]))
netflix_df.duration_type = netflix_df.duration_type.map(str.lower)
netflix_df.duration_type = netflix_df.duration_type.str.replace('seasons', 'season')

target_df.duration_int = pd.concat([amazon_prime_df.duration_int,disney_plus_df.duration_int,hulu_df.duration_int,netflix_df.duration_int],axis=0).reset_index(drop=True)
target_df.duration_int.fillna(0,inplace=True)
target_df.duration_int = target_df.duration_int.astype(int)

target_df.duration_type = pd.concat([amazon_prime_df.duration_type,disney_plus_df.duration_type,hulu_df.duration_type,netflix_df.duration_type],axis=0).reset_index(drop=True)

# Columna score

target_df.score = pd.concat([amazon_prime_df.score,disney_plus_df.score,hulu_df.score,netflix_df.score],axis=0).reset_index(drop=True)
target_df.score = target_df.score.astype(int)

# Exportación a parquet

target_df = target_df.astype(str)
target_df.to_parquet('../data/processed/api_db.parquet')