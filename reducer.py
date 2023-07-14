#!/usr/bin/env python3
from operator import itemgetter
import sys

current_movie = None
current_rating_sum = 0
current_rating_count = 0

for line in sys.stdin:
  line = line.strip()
  movie, rating = line.split("\t", 1)
  
  try:
    rating = float(rating)
  except ValueError:
    continue

  if current_movie == movie:
    current_rating_sum += rating
    current_rating_count += 1
  else:
    if current_movie:
      rating_average = current_rating_sum / current_rating_countS
      print ("%s\t%s" % (current_movie, rating_average))
    current_movie = movie
    current_rating_sum = rating
    current_rating_count = 1

if current_movie == movie:
  rating_average = current_rating_sum / current_rating_count
  print ("%s\t%s" % (current_movie, rating_average))



    
mapred streaming -files mapper.py,reducer.py -input movies_dataset.csv -output avgrating -mapper "python3 mapper.py" -reducer "python3 reducer.py"

hadoop jar /home/hadoop/hadoop-3.2.2/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar -input movies_dataset.csv -output avgrating2 -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py -local 
    
    
    cat movies_dataset.csv | head -n 5 | python3 mapper.py
    
mapred job -status job_1689147258845_0001 | grep "CPU time"
    
hadoop dfs -rmr avgrating

CREATE TABLE IF NOT EXISTS movies (UserId int, MovieName string, rating float, Genre string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


LOAD DATA LOCAL INPATH '/home/ubuntu/movies_new.csv' INTO TABLE movies;
select MovieName, rating from Movies where group by MovieName, rating;

movies = LOAD '/home/ubuntu/movies_new.csv' USING PigStorage(',') as (Userid, MovieName, rating, Genre);

group_data = FOREACH movies GENERATE MovieName, rating;

group_data = group movies by (MovieName, rating);
average = FOREACH group_data GENERATE group, AVG(movies.rating);

cd ~/IST3134/pig-tutorial-master

hadoop -cat reviews.txt | python3 /home/hadoop/workspace/python/mapper.py | sort - 

from mrjob.job import MRJob

class MRAverageRating(MRJob):
    def mapper(self, _, line):
        for oneMovie in sys.stdin:
          oneMovie = oneMovie.strip()
          ratingInfo = oneMovie.split(",")
          Movie_Name = ratingInfo[2]
          Rating = ratingInfo[3]
          yield Movie_Name, float(Rating)

    def reducer(self, movie_id, ratings):
        ratings = list(ratings)
        avg_rating = sum(ratings) / len(ratings)
        yield movie_id, avg_rating

if __name__ == '__main__':
    MRAverageRating.run()

