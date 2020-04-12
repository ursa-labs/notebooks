library(ggplot2)

# install.packages("stringi")

setwd("~/code/notebooks/20190919file_benchmarks/")

reads <- read.csv("ipc_read_parallel.csv")
writes <- read.csv("ipc_write_parallel.csv")

writes

# file size
ggplot(writes, aes(fill=factor(chunksize), y=file_size, x=codec)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggsave("ipc_file_size.png", width=10, height=4)

# write time
ggplot(writes, aes(fill=factor(chunksize), y=write_time, x=codec)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggsave("ipc_write_time.png", width=10, height=4)

# read time
ggplot(reads, aes(fill=factor(chunksize), y=read_time, x=codec)) +
  facet_grid(rows=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip()

ggsave("ipc_read_time.png", width=10, height=4)