#STA 242 Homework 5
#Yichuan Wang


#obtaining one fifth of the total observations in each year without replacement
#implementing the explicit parallelization mechanism
#the exact script will be running on each node but using node ID to identify
#which csv file should be read

#find csv file paths
load("~/hw4/csvpath.rda")

#obtain the specific csv file to be operated on
csvid = as.numeric(Sys.getenv('SGE_TASK_ID'))
csvfile = csvpath[csvid]

#read all lines of the csv file, omitting first line which is the column names
csvcon = file(csvfile, open = "rt" )
csvlines = readLines(csvfile)[-1]

#sample the indeces of lines to be selected
#about one fifth of total observations will be selected
sampleind = sample(1:length(csvlines), size = floor(length(csvlines)/5))

#grab the sampled lines
csvsample = csvlines[sampleind]

#close connection
close(csvcon)

#open connection to write the lines to one file in an appending manner
writecon = file("~/hw4/airlinesample", open = "at")
writeLines(text = csvsample, con = writecon)
close(writecon)















