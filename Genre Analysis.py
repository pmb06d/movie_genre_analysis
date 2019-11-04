# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 19:34:46 2018

@author: pbonnin
"""

import pandas as pd
import requests
import time
import datetime
import ast
import numpy as np
from tqdm import tqdm

# bring in the file with the api key
import config

#start_time = time.time()

pd.options.display.max_columns = 20

# a function to get the number of lines to skip from the IBOPE csv's
def skipper(file):
    with open(file) as f:
        lines = f.readlines()
        #get list of all possible lins starting by beerId
        num = [i for i, l in enumerate(lines) if l.startswith('"')]
        #if not found value return 0 else get first value of list subtracted by 1
        num = 0 if len(num) == 0 else num[0]
        return(num)


#%% Bringing my dataset of movies aired on HBO in Chile
directory = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/For upload'

filenames = ['Argentina','Brazil','Chile','Colombia','Mexico','Peru']

#file = directory + '/'+ filenames[0] + '.csv'
#test = pd.read_csv(file,delimiter=';',skiprows=skipper(file),encoding='latin1').drop([' '], axis=1)

dflist = []

for name in filenames:
    file = directory + '/'+ name + '.csv'
    df = pd.read_csv(file,delimiter=';',skiprows=skipper(file),encoding='latin1')
    df.drop([' '], axis=1, inplace=True)
    df['Region'] = name
    dflist.append(df)

raw_stack = pd.concat(dflist, ignore_index=True,sort=False)
    

#%% Normalizing some values

targets = list(raw_stack['Target'].unique())
targets_nm = ['Pay_Universe',
             'P18-49',
             'P25-49_HM',
             'Pay_Universe',
             'P18-49',
             'P25-49_HM',
             'Pay_Universe',
             'P25-49_HM',
             'Pay_Universe',
             'P18-49',
             'P25-49_HM',
             'P18-49',
             'P25-49_HM',
             'Pay_Universe',
             'P18-49']

targets = dict(zip(targets,targets_nm))

types = list(raw_stack['Desc2'].unique())

approved_types = ['FILM', 'ANIMATION', 'DOCUMENTARY']

movies = raw_stack.loc[raw_stack['Desc2'].isin(approved_types),:]

movie_list = list(movies['Description'].unique())

#%% useful functions to get information from the API


api_key = config.api_key

# a function to check what matches in the API per title
def print_matches(title):
    response = requests.get('https://api.themoviedb.org/3/search/movie?api_key=' +  api_key + '&language=en-US&query=' + title + '&include_adult=false)')
    data = response.json()
    results = []
    for result in data['results']:
        results.append(result['title']+' ('+result['release_date'][:4]+')')
    print(len(results),"matches:")
    for item in results:
        print(item)
        
# Fields available on the API response
def available_fields():
    response = requests.get('https://api.themoviedb.org/3/search/movie?api_key=' +  api_key + '&language=en-US&query=' + 'Hunger Games' + '&include_adult=false)')
    data = response.json()
    result_fields = []
    for i in data['results'][0].keys():
        result_fields.append(i)
    print('There are '+str(len(result_fields))+' fields available:','\n')
    for field in result_fields:
        print(field)

# funcions to look up genre IDs
def genre_lookup(genreid):
    return(genre_dict[genreid])

def list_genre_lookup(idlist):
    sample_gnames = []
    for id in idlist:
        sample_gnames.append(genre_lookup(id))
    return(sample_gnames)
    
# function to find the 'age' of the movies using today's year
def how_many_years(year):
    this_year = datetime.datetime.now().year
    return(this_year - year)


# a function to loop over a list and bring back the information (includes a tqdm progress bar)
def get_movie_info(movielist,result=0, api_key=api_key, wait=.15, print_rejected=False):
    tqdm.pandas()
    
    # a set of lists for the available fields
    original_title = []
    vote_count = []
    vote_average = []
    popularity = []
    title = []
    genre_ids = []
    release_date = []
    
    # Requests that don't match will show 'N/A' on the requested fields and will be added to a 'rejected_movies' dataframe
    for movie in tqdm(movielist):
        time.sleep(wait)
        response = requests.get('https://api.themoviedb.org/3/search/movie?api_key=' +  api_key + '&language=en-US&query=' + movie + '&include_adult=false)')
        info = response.json()
        try:
            #Response fields will be filled out with the first match from the API, if you believe this was not the correct match you can use
            #the print_matches function to check if there are other matches that are better
            first_match = info['results'][result]
            original_title.append(movie)
            vote_count.append(first_match['vote_count'])
            vote_average.append(first_match['vote_average'])
            popularity.append(first_match['popularity'])
            title.append(first_match['title'])
            genre_ids.append(list_genre_lookup(first_match['genre_ids']))
            release_date.append(first_match['release_date'])
        except:
            original_title.append(movie)
            vote_count.append('N\A')
            vote_average.append('N\A')
            popularity.append('N\A')
            title.append('N\A')
            genre_ids.append('N\A')
            release_date.append('N\A')
    
    # Dataframes with the new fields
    movie_df = pd.DataFrame(list(zip(original_title,vote_count,vote_average,popularity,title,genre_ids,release_date)),columns= ['Original_title','Vote_Count','Vote_Avg.','Popularity','TMDB_Title','Genres','Release_date'])
    rejected_movies = movie_df.loc[movie_df.TMDB_Title == 'N\A','Original_title']
    
    # Print the percentage match and the movies that did not match
    print('Sent '+str(len(original_title))+' title(s). Matching '+str(round(((len(original_title)-len(rejected_movies))/len(original_title))*100,0))+'% ('+str(len(rejected_movies))+' title(s) rejected)','\n')
    
    if print_rejected == True:
        print('Rejected titles:')
        for movie in rejected_movies:
            print(movie)
        print('\n')
        
    return(movie_df, rejected_movies)



#%% A dictionary of Genre IDs and Genres: genre_dict
    
# Genre ID into a dictionary
genre_response = requests.get('https://api.themoviedb.org/3/genre/movie/list?api_key='+api_key+'&language=en-US')
genres = genre_response.json()
genres = genres['genres']

keys = []
values = []
for dic in genres:
    keys.append(dic['id'])
    values.append(dic['name'])
genre_dict = dict(zip(keys, values))


#%% Get the genre info

acc, rej = get_movie_info(movie_list, print_rejected=True)
acc = acc.loc[acc.TMDB_Title != 'N\A']


#%% Cleaning the data frame

movie_clean = acc.copy()

empty_dates = movie_clean[movie_clean['Release_date'].isna()].index


# Dropping the dates with their specific index
movie_clean = movie_clean.drop(list(empty_dates), axis = 0)

# Feature extraction: Getting a release year and month for easier binning

# Converting the 'release date' field to date time (it comes in as strings) and extracting the year and the month
movie_clean['Release_year'] = pd.to_datetime(movie_clean['Release_date']).dt.year
movie_clean['Release_month'] = pd.to_datetime(movie_clean['Release_date']).dt.month

# Using the 'how_many_years' function to get the 'age' of the movie
movie_clean['Movie_age'] = movie_clean['Release_year'].apply(how_many_years)


# Print a file to check the final data set
output = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/Q1_MovieGenres_Cinemax.csv'
movie_clean.to_csv(path_or_buf=output, sep=',')



#%% A csv read of the movie_clean file to avoid running the API loop again

output = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/Q1_MovieGenres_Cinemax.csv'
movie_clean2 = pd.read_csv(output,sep=',')

#movie_clean2[movie_clean2['Release_year']==2019]

movie_clean2.drop('Unnamed: 0',axis=1, inplace=True)

#%% Wrap the loop from above in a function for the other bad matches

def replace_bad(title,data,search_title=None,result=0):
    
    if search_title == None:
        search_title = title
        
    # use the print_matches() function to find the result_number
    column_list = ['Original_title',
                   'Vote_Count',
                   'Vote_Avg.',
                   'Popularity',
                   'TMDB_Title',
                   'Genres',
                   'Release_date',
                    'Release_year',
                    'Release_month',
                    'Movie_age']
    
    movieinsertions4 = data
    
    movieinsertions4_pt1 = movieinsertions4.loc[movieinsertions4['TMDB_Title'] != title,:] 
    movieinsertions4_pt2 = movieinsertions4.loc[movieinsertions4['TMDB_Title'] == title,:]
    
    movieinsertions4_pt2_Ibope = movieinsertions4_pt2.drop(column_list,axis=1).reset_index(drop=True)
    
    movieinsertions4_pt2_acc = movieinsertions4_pt2.loc[:,column_list].reset_index()
    movieinsertions4_pt2_acc.drop(['Release_year','Release_month', 'Movie_age'],axis=1, inplace=True)
    
    acc, rej = get_movie_info([search_title,'$$$'],result=result)
    
    cols = list(acc)
    new_subset = [list(acc.iloc[0,:]) for i in range(len(movieinsertions4_pt2))]
    new_subset = pd.DataFrame(new_subset, columns=cols)
    
    movieinsertions4_pt2_new = pd.concat([movieinsertions4_pt2_Ibope, new_subset], axis=1, sort=False)
    movieinsertions4_pt2_new['Release_year'] = pd.to_datetime(movieinsertions4_pt2_new['Release_date']).dt.year
    movieinsertions4_pt2_new['Release_month'] = pd.to_datetime(movieinsertions4_pt2_new['Release_date']).dt.month
    movieinsertions4_pt2_new['Movie_age'] = movieinsertions4_pt2_new['Release_year'].apply(how_many_years)
    
    movieinsertions5 = movieinsertions4_pt1.append(movieinsertions4_pt2_new,sort=False)
    
    print(str(len(movieinsertions4_pt2))+' rows were added, replacing '+title+' with '+ acc.iloc[0,0])
    return(movieinsertions5)


#%%

print_matches('Cats and Dogs')   
movieinsertions4 = replace_bad('Cats',movie_clean2,search_title='Cats and Dogs',result=0)

print_matches('Rocketman')
movieinsertions4 = replace_bad('Rocketman',movieinsertions4,search_title='Rocketman',result=1)

print_matches('The Outsider')
movieinsertions4 = replace_bad('The Outsider',movieinsertions4,search_title='The Outsider',result=1)


output = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/Q1_MovieGenres_Cinemax.csv'
movieinsertions4.to_csv(path_or_buf=output, sep=',')


#%% Join with the rating portion

output = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/Q1_MovieGenres_Cinemax.csv'
movieinsertions4 = pd.read_csv(output,sep=',')
movieinsertions4.drop('Unnamed: 0',axis=1, inplace=True)


movieinsertions = raw_stack.merge(movieinsertions4, how='left', left_on='Description', right_on='Original_title')
movieinsertions = movieinsertions.loc[movieinsertions['TMDB_Title'].notna()]

genre_rating = movieinsertions.copy()

# Turning the genre lists back into lists, eliminating movies that don't have a genre assigned
genre_rating['Count'] = [len(ast.literal_eval(i)) for i in genre_rating['Genres']]
genre_rating = genre_rating.loc[genre_rating['Count']>0,:]
genre_rating['Genres2'] = [ast.literal_eval(i) for i in genre_rating['Genres']]

# Some code to unstack the lists
# Source: https://stackoverflow.com/questions/42012152/unstack-a-pandas-column-containing-lists-into-multiple-rows
lst_col = 'Genres2'
genre_rating2 = pd.DataFrame({
        col:np.repeat(genre_rating[col].values, genre_rating[lst_col].str.len())
        for col in genre_rating.columns.difference([lst_col])
        }).assign(**{lst_col:np.concatenate(genre_rating[lst_col].values)})[genre_rating.columns.tolist()]


genre_rating2.replace({"Target": targets}, inplace=True)

output = 'C:/Users/pbonnin/Desktop/Cinemax Research Workflows/Q1_MovieGenreRating_Cinemax.csv'
genre_rating2.to_csv(path_or_buf=output, sep=',')

#
#output_genre[output_genre['TMDB_Title']=='Furious 10']
#output_genre[output_genre['TMDB_Title']=='The Lego Movie 2: The Second Part']

