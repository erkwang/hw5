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





foo = do.call("rbind", bob[1:2])
foo$Cancelled = as.logical(foo$Cancelled)
bar = foo[sample(1:nrow(foo), size = 10000, replace = TRUE),]

library(randomForest)
library(parallel)

cl = makeCluster(3, type = "FORK")
clusterSetRNGStream(cl, 2)
tmp = clusterCall(cl, randomForest, 
                  formula = ArrDelay~., data = bar, ntree = 10, na.action = na.omit)
tmp2 = parLapply(cl, 1:2, function(i)predict(tmp[[i]], newdata = foo[1:100,], na.action = na.omit))
stopCluster(cl)







