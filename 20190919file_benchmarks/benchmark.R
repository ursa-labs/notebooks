library(fst)
library(microbenchmark)
library(data.table)
library(arrow)
library(feather)

csv_path <- "2016Q4.txt"
feather_path <- "2016Q4.feather"
fst_path <- "2016Q4.fst"
parquet_path <- "2016Q4.parquet"

create_fst_file <- function() {
  df <- data.table::fread(csv_path, sep="|", header=FALSE)
  fst::write_fst(df, fst_path)
}

do_benchmark <- function() {
  mbm <- microbenchmark(
     csv_data_table=data.table::fread(csv_path, sep="|", header=FALSE),
     feather_old=feather::read_feather(feather_path),
     fst=fst::read_fst(fst_path),
     feather_arrow=arrow::read_feather(feather_path),
     parquet=arrow::read_parquet(parquet_path),
     times=5
  )
  mbm
}

# create_fst_file()

mbm <- do_benchmark()
write.csv(mbm, "r_results.csv")