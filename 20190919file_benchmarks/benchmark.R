library(fst)
library(microbenchmark)
library(data.table)
library(arrow)
library(feather)
library(stringr)
library(dplyr)

files <- c("2016Q4", "yellow_tripdata_2010-01")
names <- c("fanniemae", "nyctaxi")
seps <- c("|", ",")

create_fst_files <- function(base) {
  df <- arrow::read_parquet(str_c(base, "_snappy.parquet"))
  fst::write_fst(df, str_c(base, "_0.fst"), compress=0)
  fst::write_fst(df, str_c(base, "_50.fst"), compress=50)
}

do_benchmark <- function(index) {
  base <- files[index]
  sep <- seps[index]

  csv_path <- str_c("data/", base, ".csv")
  feather_unc_path <- str_c(base, "_uncompressed.feather")
  feather_lz4_path <- str_c(base, "_lz4.feather")
  feather_zstd_path <- str_c(base, "_zstd.feather")
  fst_0_path <- str_c(base, "_0.fst")
  fst_50_path <- str_c(base, "_50.fst")
  parquet_unc_path <- str_c(base, "_uncompressed.parquet")
  parquet_snappy_path <- str_c(base, "_snappy.parquet")

  mbm <- microbenchmark(
     csv_fread=data.table::fread(csv_path, sep=sep, header=FALSE),
     fst_unc=fst::read_fst(fst_0_path),
     fst_50=fst::read_fst(fst_50_path),
     feather_unc=arrow::read_feather(feather_unc_path),
     feather_lz4=arrow::read_feather(feather_lz4_path),
     feather_zstd=arrow::read_feather(feather_zstd_path),
     parquet_unc=arrow::read_parquet(parquet_unc_path),
     parquet_snappy=arrow::read_parquet(parquet_snappy_path),
     times=5
  )
  mbm <- data.frame(mbm) %>% dplyr::group_by(expr) %>% dplyr::summarize(time=mean(time))
  mbm$dataset <- names[index]
  mbm
}

do_write_benchmark <- function(index) {
  base <- files[index]
  sep <- seps[index]

  df <- arrow::read_parquet(str_c(base, "_snappy.parquet"))

  mbm <- microbenchmark(
     fst_unc=fst::write_fst(df, str_c(base, "_0.fst"), compress=0),
     fst_50=fst::write_fst(df, str_c(base, "_50.fst"), compress=50),
     parquet_unc=arrow::write_parquet(df, str_c(base, "_unc_r.parquet"),
          compression="uncompressed"),
     parquet_snappy=arrow::write_parquet(df, str_c(base, "_snappy_r.parquet"),
          compression="snappy"),
     times=1
  )
  mbm <- data.frame(mbm) %>% dplyr::group_by(expr) %>% dplyr::summarize(time=mean(time))
  mbm$dataset <- names[index]
  mbm
}

generate_files <- function() {
  for (base in files) {
    create_fst_files(base)
  }
}

# generate_files()

print(str_c("Using ", arrow::cpu_count(), " threads"))

results <- dplyr::bind_rows(do_benchmark(1), do_benchmark(2))
print(results)
write.csv(results, str_c("r_results_", arrow::cpu_count(), ".csv"))

write_results <- dplyr::bind_rows(do_write_benchmark(1), do_write_benchmark(2))
print(write_results)
write.csv(write_results, str_c("r_write_results_", arrow::cpu_count(), ".csv"))
