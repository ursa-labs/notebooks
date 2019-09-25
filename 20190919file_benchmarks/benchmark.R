library(fst)
library(microbenchmark)
library(data.table)
library(arrow)
library(feather)
library(stringr)
library(dplyr)

files <- c("2016Q4", "yellow_tripdata_2010-01")
names <- c("fanniemae", "nyctaxi")

create_fst_file <- function(base) {
  df <- arrow::read_parquet(str_c(base, ".parquet"))
  fst::write_fst(df, str_c(base, ".fst"))
}

do_benchmark <- function(index) {
  # csv_path <- str_c(base, ".txt")
  # csv_data_table=data.table::fread(csv_path, sep="|", header=FALSE),

  base <- files[index]

  feather_path <- str_c(base, ".feather")
  fst_path <- str_c(base, ".fst")
  parquet_path <- str_c(base, ".parquet")

  mbm <- microbenchmark(
     feather_old=feather::read_feather(feather_path),
     fst=fst::read_fst(fst_path),
     feather_arrow=arrow::read_feather(feather_path),
     parquet=arrow::read_parquet(parquet_path),
     times=5
  )
  mbm <- data.frame(mbm) %>% dplyr::group_by(expr) %>% dplyr::summarize(time=mean(time))
  mbm$dataset <- names[index]
  mbm
}

generate_files <- function() {
  for (base in files) {
    create_fst_file(base)
  }
}

results <- dplyr::bind_rows(do_benchmark(1), do_benchmark(2))

write.csv(results, "r_results.csv")