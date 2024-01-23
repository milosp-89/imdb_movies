#### IMDB movies

# modules and libraries:
import os, requests as r, pandas as pd, gzip, shutil, math, warnings, time, requests, base64
from concurrent.futures import ThreadPoolExecutor as tpe
from concurrent import futures

# exclude messages and warnings:
warnings.filterwarnings("ignore")

# start time
start = time.time()

# function for url setup:
def url_setup():
    main_link = 'https://datasets.imdbws.com/'
    ext = ['name.basics.tsv.gz',
           'title.basics.tsv.gz',
           'title.principals.tsv.gz',
           'title.ratings.tsv.gz']
    full_link = [main_link + x for x in ext] # [0]
    fpaths = [x.split('/')[-1] for x in full_link] #[1]
    new_f_exts = [y.replace('.gz', '') for y in fpaths] #[2]
    return full_link, fpaths, new_f_exts

# function to check and to delete all files:
def delete_files():
    try:
        for x, y in zip(url_setup()[1], url_setup()[2]):
            os.remove(x)
            os.remove(y)
    except:
        pass

# calling function delete_files():
delete_files()

# function to download url:
def download(url):
    res = r.get(url)
    fname = url.split('/')[-1]
    with open(fname, 'wb') as f:
        f.write(res.content)

# function to apply multithreading to download all urls in parallel:
def concurrent():
    with tpe(max_workers = 6) as executor:
        executor.map(download, url_setup()[0])

# calling function concurrent():
concurrent()

# function to extract downloaded .gz files into .tsv:
def extract_gz(args):
    fpath, new_f_ext = args
    with gzip.open(fpath, 'rb') as f_in, open(new_f_ext, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# function to apply multithreading on each file:
def extract_concurrent(fpaths, opaths):
    with tpe(6) as executor:
        executor.map(extract_gz, zip(fpaths, opaths))

# calling function extract_concurrent():
extract_concurrent(url_setup()[1], url_setup()[2])

# function to load/read data into dataframes:
def load_df(fpath):
    try: return pd.read_csv(fpath, sep = '\t',
                            engine = 'pyarrow',
                            low_memory = False)
    except: return pd.read_csv(fpath, sep = '\t', low_memory = False)

# calling function load_df():
with tpe(max_workers = 6) as executor:
    futures = {executor.submit(load_df, fpath):
               fpath for fpath in [x for x in url_setup()[2]]}
    for future in futures:
        future.result()
df_names, df_titles, df_principals, df_ratings = (
    [future.result() for future in futures])

# function to transform runtime column:
def ttime(row):
    if int(row) == 60:
        return '1h:0m'
    elif int(row) < 60:
        return f'0h:{row}m'
    else:
        full = int(row) / 60
        h = (str(full))[0]
        m = math.ceil((full - int(full)) * 60)
        return f'{h}h:{m}m'
    
# function to prepare df_titles dataframe:
def titles():
    dtitles = df_titles
    dtitles = (dtitles[(dtitles['isAdult'] == '0') &
                       (dtitles['titleType'] == 'movie')])
    dtitles = dtitles.drop(dtitles.columns[[2, 4]],
                           axis = 1)
    dtitles.columns = ['Id',
                       'Type',
                       'Title',
                       'Year',
                       'Status',
                       'Runtime',
                       'Genres']
    dtitles['Runtime'] = (
        dtitles['Runtime'].replace(r'\N', '0'))
    dtitles['FullRuntime'] = (
        dtitles['Runtime'].apply(ttime))
    columns = dtitles.columns.tolist()
    columns.insert(columns.index('Runtime') + 1,
                   columns.pop(columns.index('FullRuntime')))
    dtitles = dtitles[columns]
    dtitles['Runtime'] = (
        dtitles['Runtime'].astype(dtype = 'int64'))
    dtitles = dtitles[dtitles['Runtime'] != 0]
    return dtitles

# function to split titles to movies, series and tv shows:
def split_titles():
    main = titles()
    df_movies = main.reset_index(drop = True)
    df_movies = (
        df_movies.drop(df_movies.columns[[1, 4]],
                       axis = 1))
    df_movies[['Year', 'Genres']] = (
        df_movies[['Year', 'Genres']]
        .apply(lambda x: x.replace(r'\N', 'Unknown')))
    return df_movies

# function to transform ratings:
def trating(row):
    if row <= 5.0:
        return 'Bad'
    elif row > 5.0 and row <= 6.5:
        return 'Ok'
    elif row > 6.5 and row <= 7.5:
        return 'Good'
    elif row > 7.5:
        return 'Very good'
    
# function to transform votes:
def tvotes(row):
    if row <= 5000:
        return '<=5000'
    elif row > 5000 and row <= 10000:
        return '> 5000 and <= 10,000'
    elif row > 10000 and row <= 50000:
        return '> 10,000 and <= 50,000'
    elif row > 50000 and row <= 100000:
        return '> 50,000 and <= 100,000'
    elif row > 100000 and row <= 500000:
        return '> 100,000 and <= 500,000'
    elif row > 500000 and row <= 1000000:
        return '> 500,000 and row <= 1,000,000'
    elif row > 1000000:
        return '> 1,000,000'
    
# function to merge titles and ratings:
def movies():
    dmovies = split_titles()
    movies = (dmovies.merge(right = df_ratings,
                           how = 'left',
                           left_on = 'Id',
                           right_on = 'tconst')
                               .drop(columns = 'tconst',
                                     axis = 1))
    movies.columns = ['Id','Title','Year','Runtime','FullRuntime',
                      'Genres','Rating','NumofVotes']
    movies['NumofVotes'] = (
        movies['NumofVotes'].fillna(0).astype('int64'))
    movies['Rating'] = (
        movies['Rating'].fillna(0))
    movies = (
        movies[(movies['Rating'] != 0.0) 
               & (movies['NumofVotes'] != 0)])
    movies['CategoryRating'] = (
        movies['Rating'].apply(trating))
    movies['CategoryVotes'] = (
        movies['NumofVotes'].apply(tvotes))
    movies['Hyperlink'] = (
        'https://www.imdb.com/title/' + movies['Id'].astype(dtype = str) + '/')
    return movies

# function for preparing names:
def names():
    df_principals.set_index('nconst', inplace = True)
    df_names.set_index('nconst', inplace = True)
    full_mm = (
        df_principals.join(other = df_names,
                           how = 'left',
                           on = ['nconst']))
    fnames = (full_mm.drop(columns = ['ordering',
                                      'job',
                                      'characters',
                                      'primaryProfession',
                                      'knownForTitles'],
                                          axis = 1))
    fnames.reset_index(inplace = True)
    fnames['birthYear'] = (
        fnames['birthYear']
            .apply(lambda x: 'Unknown' if x == r'\N' else x))
    fnames['deathYear'] = (
        fnames['deathYear']
        .apply(lambda x: 'Unknown' if x == r'\N' else x))
    lnames = ['actor','actress','director','producer','writer',
              'editor', 'cinematographer','composer']
    new_names = (
        fnames[fnames['category'].isin(lnames)])
    new_names = (
        new_names.drop(columns = ['nconst',
                                  'birthYear',
                                  'deathYear'],
                                      axis = 1))
    final_table_names = pd.pivot_table(data = new_names,
                                       index = 'tconst',
                                       columns = 'category',
                                       values = 'primaryName',
                                       aggfunc = 'first',
                                       fill_value = 'Unknown').reset_index()
    final_table_names = (
        final_table_names.rename_axis(columns = None).reset_index())
    final_table_names = (
        final_table_names.drop(columns = 'index', axis = 1))
    return final_table_names

# function to transform runtime column:
def truntime(row):
    if row <= 75:
        return '<=1h:15m'
    elif row > 75 and row <= 105:
        return '>1h:15m and <=1h:45'
    elif row > 105 and row <= 120:
        return '>1h:45m and <=2h:0m'
    elif row > 120 and row <= 150:
        return '>2h:0m and <=2h:30m'
    elif row > 150:
        return '>2h:30m'
    
# function for final movies preparation:
def final_movies():
    final_table_names = names()
    final_table_names = (
        final_table_names.rename(columns = {'tconst': 'Id'}))
    dmovies = movies()
    dmovies.set_index('Id', inplace = True)
    final_table_names.set_index('Id', inplace = True)
    fmt = (
        dmovies.join(other = final_table_names,
                     how = 'left',
                     on = ['Id']))
    fmt.reset_index(inplace = True)
    fmt = fmt.sort_values(by = 'Id')
    fmt['RuntimeCategory'] = (
        fmt['Runtime'].apply(truntime))
    fmt[['Genre 1','Genre 2', 'Genre 3']] = (
        fmt['Genres'].str.split(',', expand = True))
    cols = ['Id','Title','Year','Runtime','FullRuntime','RuntimeCategory','Rating','CategoryRating',
           'NumofVotes','CategoryVotes','Genres','Genre 1','Genre 2','Genre 3','actor','actress',
           'writer','director','producer','editor','composer','cinematographer','Hyperlink']
    fmt = fmt.reindex(columns = cols)
    fcols = ['Genre 1', 'Genre 2', 'Genre 3']
    for c in fcols:
        fmt[c] = fmt[c].fillna('Unknown')
    return fmt

# function to save files into parts:
def save(part):
    main = final_movies()
    columnss = ['Id','Title','Year','Runtime','FullRuntime','RuntimeCategory','Rating',
               'CategoryRating','NumofVotes','CategoryVotes','Genres','Genre 1','Genre 2',
               'Genre 3','Actor','Actress','Writer','Director','Producer','Editor','Composer',
               'Cinematographer','Hyperlink']
    main.columns = columnss
    col = columnss[1:]

    num_parts = part
    part_size = len(main) // num_parts

    for i in range(num_parts):
        start_index = i * part_size
        end_index = (i + 1) * part_size if i < num_parts - 1 else len(main)
        part = main.iloc[start_index:end_index]
        part.to_csv(f'movies_part{i + 1}.csv',
                    sep=';',
                    index=False,
                    decimal = ',',
                    columns = col)
        
# calling save() function: 
save(5)
        
# function to push files into the github:
def git_push(fpath):
    with open(fpath, encoding='utf8', mode='r') as f:
        file = f.read()
    encoded = base64.b64encode(file.encode()).decode()

    repo_owner = 'milosp-89'
    repo_name = 'movie-finder_app'
    token = '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'
    filename = fpath.split('\\')[-1]
    url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{filename}'

    headers = {
    'Authorization': f'Token {token}',
    'Content-Type': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    latest_commit_sha = response.json().get('sha', None)
    
    data = {
    'message': 'Update CSV file',
    'content': encoded,
    'sha': latest_commit_sha
    }
    
    response = requests.put(url, headers=headers, json=data)

# calling function git_push():
files = ["G:\\IMDB\\main_python_script\\movies_part1.csv",
"G:\\IMDB\main_python_script\\movies_part2.csv",
"G:\\IMDB\main_python_script\\movies_part3.csv",
"G:\\IMDB\main_python_script\\movies_part4.csv",
"G:\\IMDB\main_python_script\\movies_part5.csv"]

for f in files:
    git_push(f)
    time.sleep(5)

print('imdb_movies.py script has been executed and all files sent to github repo!')
print(f'Total time execution is: {round((time.time() - start) / 60, 2)} minutes')