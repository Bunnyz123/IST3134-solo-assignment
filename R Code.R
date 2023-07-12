library(dplyr)


#read and convert dataset to dataframe
DT <- read.csv("D:/Sunway/y3/big data/assignment/movies_dataset.csv")
DT=data.frame(DT)

#Keep 2nd column and 
DT2=DT[c(2:5)]


#shows first 6 lines of the dataframe created
head(DT2)

#shows number of rows
nrow(DT)

#Statistic Sum of dataframe
summary(DT2)

#export new csv
write.table(DT2, "D:/Sunway/y3/big data/assignment/movies_new.csv",sep=",", col.names=F, row.names=F)



