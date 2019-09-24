library(fst)
library(microbenchmark)
library(data.table)
library(arrow)
library(feather)

csv_path <- "/home/wesm/Downloads/2016Q4.txt"
feather_path <- "2016Q4.feather"
fst_path <- "2016Q4.fst"
parquet_path <- "2016Q4.parquet"

create_fst_file <- function() {
  df <- data.table::fread(csv_path, sep="|", header=FALSE)
  fst::write_fst(df, fst_path)
}

mbm <- microbenchmark(
   csv_data_table=data.table::fread(csv_path, sep="|", header=FALSE),
   feather_old=feather::read_feather(feather_path),
   fst=fst::read_fst(fst_path),
   feather_arrow=arrow::read_feather(feather_path),
   parquet=arrow::read_parquet(parquet_path),
   times=5
)

mbm