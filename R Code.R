library(dplyr)


#read and convert dataset to dataframe
DT <- read.csv('D:/Sunway/y3/big data/assignment/archive (2)/movies_dataset.csv')
DT=data.frame(DT)

#Keep 2nd column and onward
DT2=DT[c(2:5)]

#shows first 6 lines of the dataframe created
head(DT2)

#shows number of rows
nrow(DT)

#Statistic Sum of dataframe
summary(DT2)




