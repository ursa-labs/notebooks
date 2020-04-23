library(ggplot2)
library(dplyr)


read_results <- read.csv("all_read_results.csv")
write_results <- read.csv("all_write_results.csv")
# Add a row for the Fannie Mae CSV file size, as reported in the previous post
file_sizes <- rbind(
  read.csv("file_sizes.csv", stringsAsFactors = FALSE),
  data.frame(
    dataset = "fanniemae",
    file_type = "CSV",
    size = 1.52*1024,
    stringsAsFactors = FALSE
  )
)

# We did an extra run of the fanniemae data with more reps, so let's use that
fm_read <- read.csv("20200421_fanniemae_results/all_read_results.csv")
fm_read_stats <- fm_read %>%
  group_by(expr, output_type, nthreads) %>%
  summarize(min = min(time), median = median(time), max=max(time), dataset = "fanniemae")
read_results <- merge(read_results, fm_read_stats, all.x = TRUE)
# The python numbers are close enough; the R numbers are errors. Let's not use them

fm_write <- read.csv("20200421_fanniemae_results/all_write_results.csv")
fm_write_stats <- fm_write %>%
  group_by(expr, output_type, nthreads) %>%
  summarize(min = min(time), median = median(time), max=max(time), dataset = "fanniemae")
write_results <- merge(write_results, fm_write_stats, all.x = TRUE)
# These look more sensible. Update the time we'll use accordingly
write_results$time[!is.na(write_results$median)] <- write_results$median[!is.na(write_results$median)]

# Color mapping
cols <- c(
  "feather V1" = "steelblue",
  "feather V2 (UNC)" = "steelblue",
  "feather V2 (LZ4)" = "steelblue",
  "feather V2 (ZSTD)" = "steelblue",
  "parquet (SNAPPY)" = "steelblue1",
  "parquet (UNC)" = "steelblue1",
  "fst (C=50)" = "wheat4",
  "fst (UNC)" = "wheat4",
  "RDS (C)" = "gray",
  "RDS (UNC)" = "gray",
  "csv_fread" = "wheat3",
  "CSV" = "wheat3"
)

# This is ugly but it makes the graph labels prettier
munge_labels <- function (x) {
  sub("csv_fread", "CSV (data.table::fread)",
    sub("UNC", "Uncompressed",
      sub("feather", "Feather",
        sub("parquet", "Parquet",
          sub("[Cc]=", "ZSTD, ",
            sub("ZSTD", "ZSTD, 1",
              sub("\\(C\\)", "(GZIP)",
                sub("V1", "V1 (Uncompressed)",
                  x))))))))
}
names(cols) <- munge_labels(names(cols))

fix_formats <- function(x) {
  # This applies the pretty names and reorders the factor levels so that
  # they print in the order we want
  levels(x) <- munge_labels(levels(x))
  factor(x, levels = rev(c(
      "Feather V1 (Uncompressed)",
      "Feather V2 (Uncompressed)",
      "Feather V2 (LZ4)",
      "Feather V2 (ZSTD, 1)",
      "Parquet (Uncompressed)",
      "Parquet (SNAPPY)",
      "RDS (Uncompressed)",
      "RDS (GZIP)",
      "CSV",
      "CSV (data.table::fread)",
      "fst (Uncompressed)",
      "fst (ZSTD, 50)"
    ))
  )
}

benchmark_plot <- function(data) {
  # Since we do the same thing for most of the graphs, collect plotting logic here
  ggplot(data, aes(y=time, fill=expr, x=expr)) +
    facet_wrap(vars(output_type), ncol=1) +
    geom_col(position="dodge") +
    theme_minimal() +
    scale_fill_manual(values = cols) +
    coord_flip() +
    theme(
      legend.position = "none",
      panel.grid.major.y = element_blank()
    )
}


### Reading
read_results$expr <- fix_formats(read_results$expr)
read_results$Threads <- factor(read_results$nthreads)
# All
ggplot(read_results, aes(fill=Threads, y=time, x=expr)) +
  facet_grid(rows=vars(output_type), col=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip() +
  theme_minimal() +
  theme(legend.position = "right") +
  labs(x = "Format", y = "Time to read (s)", title = "")
ggsave("20200414_read_full.png", width=10, height=6)

# Python and Arrow only
read_results %>%
  filter(nthreads == 4 & dataset == "fanniemae" & language == "Python") %>%
  benchmark_plot() +
  scale_y_continuous(breaks = seq(0, 10, 2), limits = c(0, 10)) +
  labs(x = "", y = "Time to read (s)", title = "")
ggsave("20200414_read_py.png", width=10, height=3)

# R (and drop RDS because it is out of range)
read_results %>%
  filter(nthreads == 4 & dataset == "fanniemae" & language == "R" & !grepl("^RDS", expr)) %>%
  benchmark_plot() +
  scale_y_continuous(breaks = seq(0, 10, 2), limits = c(0, 10)) +
  labs(x = "", y = "Time to read (s)", title = "")
ggsave("20200414_read_r.png", width=10, height=3)

### Writing
write_results$expr <- fix_formats(write_results$expr)
write_results$Threads <- factor(write_results$nthreads)
# All
ggplot(write_results, aes(fill=Threads, y=time, x=expr)) +
  facet_grid(rows=vars(output_type), col=vars(dataset)) +
  geom_bar(position="dodge", stat="identity") +
  coord_flip() +
  labs(x = "Format", y = "Time (s)", title = "Write speeds")
ggsave("20200414_write_full.png", width=10, height=6)

# Python and Arrow only
write_results %>%
  filter(nthreads == 4 & dataset == "fanniemae" & language == "Python") %>%
  benchmark_plot() +
  scale_y_continuous(breaks = seq(0, 12, 2), limits = c(0, 13)) +
  labs(x = "", y = "Time to write (s)", title = "")
ggsave("20200414_write_py.png", width=10, height=3)

# R
write_results %>%
  filter(nthreads == 4 & dataset == "fanniemae" & language == "R" & !grepl("^RDS", expr)) %>%
  benchmark_plot() +
  scale_y_continuous(breaks = seq(0, 12, 2), limits = c(0, 13)) +
  labs(x = "", y = "Time to write (s)", title = "")
ggsave("20200414_write_r.png", width=10, height=3)

### File sizes
file_sizes$file_type <- fix_formats(as.factor(file_sizes$file_type))
ggplot(file_sizes[file_sizes$dataset == "fanniemae",], aes(y=size/1024, file_type, fill = file_type)) +
  geom_col(position="dodge") +
  theme_minimal() +
  scale_fill_manual(values = cols) +
  coord_flip() +
  theme(
    legend.position = "none",
    panel.grid.major.y = element_blank()
  ) +
  labs(y = "File size (GB)", x = "", title = "")
ggsave("20200414_file_sizes.png", width=10, height=3)
