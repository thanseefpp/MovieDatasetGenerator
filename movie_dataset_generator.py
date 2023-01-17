#################################### IMPORTING LIBRARIES #####################################
import requests
import csv
import json
import os
import boto3
from botocore.exceptions import NoCredentialsError
# import logging
from dotenv import load_dotenv
load_dotenv()

#################################### CONFIGURATIONS ##########################################

# logging.basicConfig(level=logging.DEBUG)
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
API_KEY = os.environ.get('API_KEY')
UPLOAD_FOLDER = "movie_dataset/"
bucket_name = os.environ.get('BUCKET_NAME')
FILE_PATH = "data/"
FILE_NAME = "TrendingMovies.csv"

#################################### GENERATING DATABASE #####################################


def generate_new_dataset(movie_data, genres_list):
    with open(FILE_PATH+FILE_NAME, 'w', newline='') as file:
        writer = csv.writer(file)
        # csv titles create
        writer.writerow(["Movie_id", "Language", "Movie_Name", "Genres",
                        "Year of Release", "Popularity", "Vote_Average", "Vote_Count", "Adult"])
        for movie in range(1, movie_data['total_pages']):
            movie_collection = requests.get(
                f"https://api.themoviedb.org/3/trending/movie/week?api_key={api_key}&page={movie}")
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
                genres_string = " ".join(
                    [item['name'] for item in genres_list['genres'] if item['id'] in genre_ids])
                # writing items to csv file
                writer.writerow([movie_id, language, original_title, genres_string,
                                release_date, popularity, vote_average, vote_count, adult])
    return 'Completed'

#################################### UPLOADING TO S3 BUCKET ####################################


def upload_generated_csv_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        return True
    except FileNotFoundError:
        return False
    except NoCredentialsError:
        return False


#################################### MAIN FUNCTION ####################################

try:
    # logging.debug('Project Started')
    api_key = API_KEY
    if api_key is None:
        raise Exception("API KEY is not getting")
    movie_url = f"https://api.themoviedb.org/3/trending/movie/week?api_key={api_key}"
    collected_data = requests.get(movie_url)
    collected_data.raise_for_status()
    movie_data = json.loads(collected_data.content)
    genres = requests.get(
        f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}")
    genres_list = json.loads(genres.content)
    generate = generate_new_dataset(
        movie_data=movie_data, genres_list=genres_list)
    if generate == 'Completed':
        # print("success")
        uploaded = upload_generated_csv_s3(local_file=os.path.join(FILE_PATH, FILE_NAME),
                                           bucket=bucket_name,
                                           s3_file=f"{UPLOAD_FOLDER}{FILE_NAME}")
        if uploaded == True:
            print('Uploaded to S3 bucket')
    else:
        raise Exception("Generating Database Failed")
    # logging.debug("completed")
except Exception as e:
    print(e)