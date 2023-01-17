import requests,csv
import json
import os
import logging

def generate_new_dataset(movie_data,genres_list):
    try:
        with open('TrendingMovies.csv', 'w', newline='') as file:
          writer = csv.writer(file)
          # csv titles create
          writer.writerow(["Movie_id","Language","Movie_Name","Genres","Year of Release","Popularity","Vote_Average","Vote_Count","Adult"])
          for movie in range(1,movie_data['total_pages']):
              movie_collection = requests.get(f"https://api.themoviedb.org/3/trending/movie/week?api_key={api_key}&page={movie}")
              collected_info = json.loads(movie_collection.content)
              for result in collected_info['results']:
                  movie_id = result['id']
                  language = result['original_language']
                  original_title = result['original_title']
                  popularity = result['popularity']
                  release_date = result['release_date']
                  vote_average = result['vote_average']
                  vote_count = result['vote_count']
                  adult = result['adult']
                  genre_ids = result['genre_ids']
                  genres_string = " ".join([item['name'] for item in genres_list['genres'] if item['id'] in genre_ids])
                  # writing items to csv file
                  writer.writerow([movie_id,language,original_title,genres_string,release_date,popularity,vote_average,vote_count,adult])
    except Exception as e:
        print(e)    


try:
  api_key = os.environ.get('API_KEY')
  movie_url = f"https://api.themoviedb.org/3/trending/movie/week?api_key={api_key}"
  collected_data = requests.get(movie_url)
  collected_data.raise_for_status()
  movie_data = json.loads(collected_data.content)
  genres = requests.get(f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}")
  genres_list = json.loads(genres.content)
  generate_new_dataset(movie_data=movie_data,genres_list=genres_list)
  logging.debug("completed")
except Exception as e:
  print(e)