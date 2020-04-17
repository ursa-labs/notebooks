library(ggplot2)

setwd("~/code/notebooks/20190919file_benchmarks/")

read_results <- read.csv("all_read_results.csv")
write_results <- read.csv("all_write_results.csv")
file_sizes <- read.csv("file_sizes.csv")

ggplot(read_results, aes(fill=factor(nthreads), y=time, x=expr)) +
  facet_grid(rows=vars(output_type), col=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip() +
  ggtitle("Read speeds")

ggsave("20200414_read_results.png", width=10, height=6)

ggplot(write_results, aes(fill=factor(nthreads), y=time, x=expr)) +
  facet_grid(rows=vars(output_type), col=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip() +
  ggtitle("Write speeds")

ggsave("20200414_write_results.png", width=10, height=6)

ggplot(file_sizes, aes(fill=file_type, y=size, x=dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip() +
  ggtitle("File sizes")

ggsave("20200414_file_sizes.png", width=10, height=3)