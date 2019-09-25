library(ggplot2)
library(feather)

setwd("~/code/notebooks/20190919file_benchmarks/")

results <- read.csv("all_results.csv")

results

ggplot(results, aes(fill=output_type, y=time, x=expr)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") + 
  coord_flip()