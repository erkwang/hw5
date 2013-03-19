#read individual rds files containing data frames from each year
rdspath = list.files("~/Downloads/airlinerds", pattern = ".rds$", full.names = TRUE)
samplelist = lapply(rdspath, readRDS)

#add column for whether the flight is bound to of from a major airport
#and if it is operated by a major airline company
majorap = c("ATL", "ORD", "LAX", "DFW", "DEN", "JFK", "SFO", "LAS", "PHX", 
            "IAH", "CLT", "MIA", "MCO", "EWR", "SEA", "MSP", "DTW", "PHL", 
            "BOS", "LGA", "FLL", "BWI", "IAD", "SLC", "MDW", "DCA", "HNL", 
            "SAN", "TPA")
majorcarrier = c("FL", "AS", "AA", "MQ", "EV", "OH", "CO", "DL", "F9", "HA", 
                "B6", "YV", "OO", "WN", "NK", "UA", "US", "WO")

checkmajor = function(df, ap = majorap, carrier = majorcarrier){
  df$MainAP = (df$Origin %in% ap) | (df$Dest %in% ap)
  df$MainCarrier = df$UniqueCarrier %in% carrier
  df
}

samplelist = lapply(samplelist, function(x)checkmajor(df = x))
sampledf = do.call("rbind", samplelist)

#generate randomForest in a parallelized way
library(doSNOW)

cl = makeCluster(3)
clusterSetupSPRNG(cl)

#function to ask each worker generate one tree at a time
rfworker = function(cl, df, samsize, treenum){
  clusterEvalQ(cl, library(randomForest))
  n = nrow(df)
  indmat = matrix(sample(1:n, size = samsize*treenum, replace = TRUE), nrow = samsize, ncol = treenum)
  res = parApply(cl, indmat, 2, function(ind){randomForest(ArrDelay~., data = df[ind,], 
                                                          ntree = 1, na.action = na.omit)})
  class(res) = c("ClusterRF", "list")
  res
}

rf = rfworker(cl, sampledf, 10000, 500)

#function to create an RF with half randomly picked variable at the beginning of fitting each tree
rfworker.randvar = function(cl, df, samsize, treenum){
  clusterEvalQ(cl, library(randomForest))
  n = nrow(df)
  indmat = matrix(sample(1:n, size = samsize*treenum, replace = TRUE), nrow = samsize, ncol = treenum)
  varmat = replicate(treenum, sample(c(1:10, 12:15), size = 7, replace = FALSE))
  res = parLapply(cl, 1:treenum, function(i){randomForest(ArrDelay~., data = df[indmat[,i],c(varmat[,i],11)], 
                                                          ntree = 1, na.action = na.omit)})
  class(res) = c("ClusterRF", "list")
  res
}

rf.randvar = rfworker.randvar(cl, sampledf, 10000, 500)

stopCluster(cl)

#prediction method for ClusterRF objects
predict.ClusterRF = function(object, newdata, cl, ...){
  clusterEvalQ(cl, library(randomForest))
  pred = parLapply(cl, object, predict, newdata = newdata)
  pred = do.call("cbind", pred)
  pred = rowMeans(pred, na.rm = TRUE)
  pred
}

#test the two randomForests
newdf = sampledf[sample(1:nrow(sampledf), size = 10000, replace = FALSE),]

cl = makeCluster(3)
clusterSetupSPRNG(cl)
pred.rf = predict(object = rf, newdata = newdf, cl)
pred.randvar = predict(object = rf.randvar, newdata = newdf, cl)
stopCluster(cl)

#obtain MSPE
mspe.rf = sum((pred.rf - newdf[,"ArrDelay"])^2, na.rm = TRUE)/sum(!is.na(pred.rf))
mspe.randvar = sum((pred.randvar - newdf[,"ArrDelay"])^2, na.rm = TRUE)/sum(!is.na(pred.randvar))
