#!/usr/bin/env python3
import sys
for oneMovie in sys.stdin:
  oneMovie = oneMovie.strip()
  ratingInfo = oneMovie.split(",")
  Movie_Name = ratingInfo[2]
  Rating = ratingInfo[3]
  print ('%s\t%s' % (Movie_Name, Rating))