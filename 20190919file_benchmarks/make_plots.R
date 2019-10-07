library(ggplot2)

# setwd("~/code/notebooks/20190919file_benchmarks/")

results <- read.csv("all_results.csv")

ggplot(results, aes(fill=output_type, y=time, x=expr)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()


ggsave("plot.png", width=10, height=4)