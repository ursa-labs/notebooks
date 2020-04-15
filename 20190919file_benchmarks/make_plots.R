library(ggplot2)

setwd("~/code/notebooks/20190919file_benchmarks/")

results <- read.csv("all_results.csv")
file_sizes <- read.csv("file_sizes.csv")

ggplot(results, aes(fill=factor(nthreads), y=time, x=expr)) +
  facet_grid(rows=vars(output_type), col=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggplot(results[results$language == "R",], aes(fill=factor(nthreads), y=time, x=expr)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggplot(results[results$language == "Python",], aes(fill=factor(nthreads), y=time, x=expr)) +
  facet_grid(cols=vars(dataset), rows=vars(output_type)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

results

ggsave("results_20200412.png", width=10, height=6)

ggplot(file_sizes, aes(fill=file_type, y=size, x=dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggsave("file_sizes_20200412.png", width=10, height=3)