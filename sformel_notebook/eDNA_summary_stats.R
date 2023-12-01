library(dplyr)
library(arrow)
library(robis)

arrow::open_dataset(sources = "data/obis_20230726.parquet")$schema$names %>%  sort()

# partition dataset
df <- arrow::open_dataset(sources = "data/obis_20230726.parquet") %>% 
  write_dataset(path = "data/partitioned/", format = "parquet", partitioning = c(""))

#count total eDNA datasets, ASVs, and reads
df <- arrow::open_dataset(sources = "data/obis_20230726.parquet")

df <- arrow::open_dataset(sources = "data/obis_20230726.parquet") %>% 
  select(dataset_id, organismQuantity, organismQuantityType) %>% 
  filter(stringr::str_detect(organismQuantityType, pattern ="DNA")) %>% 
  group_by(dataset_id) %>% 
  summarise(ASV_count = n(), read_total = sum(organismQuantity %>% as.numeric())) %>% 
  head() %>% 
  collect()

# How to get all DNA datasets
DNads <- robis::dataset(hasextensions = "DNADerivedData")

# But here I get 40 datasets
https://obis.org/datasets

# It's not clear how "Sequence Records" can be more than "occurrence records" in 
# https://obis.org/dataset/74d67d71-2d25-4fa1-9a1e-df71c6af891e

DNA_dataset_count <- unique(df$dataset_id)
occurrence_count <- nrow(df)
read_total <- sum(df$organismQuantity %>% as.numeric())


# What if I try to download from the API all the eDNA datasets?

#So it looks like you can't call the DNA derived fields directly
DNA <- "DNADerivedData"
api_download <- robis::occurrence(datasetid = "74d67d71-2d25-4fa1-9a1e-df71c6af891e", scientificname = "Chrysoculter rhomboideus", hasextensions = DNA, extensions = DNA, fields = c("occurrenceID", "organismQuantity", "target_gene", "DNA_sequence"))

api_download <- robis::occurrence(datasetid = "74d67d71-2d25-4fa1-9a1e-df71c6af891e", scientificname = "Chrysoculter rhomboideus", hasextensions = DNA, extensions = DNA)

tictoc::tic()
api_download <- robis::occurrence(hasextensions = DNA, extensions = DNA)
tictoc::toc()

# It's possible to query a taxon and get the DNA extension
api_download <- robis::occurrence(scientificname = "Chrysoculter rhomboideus", hasextensions = DNA, extensions = DNA)

# It's possible to query a dataset and get the DNA extension
api_download <- robis::occurrence(datasetid = "c82dd852-a454-4a5b-8515-ece289f382d3", extensions = "DNADerivedData")

# It's possible to query a dataset and get the DNA extension
api_download2 <- robis::occurrence(datasetid = "c82dd852-a454-4a5b-8515-ece289f382d3", hasextensions = "DNADerivedData", extensions = "DNADerivedData")

# Check if clash is true for MOF too
robis::occurrence(datasetid = "5e3015f8-a398-46b5-adb1-518a8247aeb9", hasextensions = "MeasurementOrFact", extensions = "MeasurementOrFact") %>% 
  head()

robis::occurrence(datasetid = "5e3015f8-a398-46b5-adb1-518a8247aeb9", extensions = "MeasurementOrFact") %>% 
  head()


# Download jut occurrencID and DNA
eDNA_select <- robis::occurrence(datasetid = "c82dd852-a454-4a5b-8515-ece289f382d3", 
                                 extensions = "DNADerivedData", 
                                 fields = c("occurrenceID", "organismQuantity", "dna"))


# Try all DNA datasets for bug
DNA_ds_list <- robis::dataset(hasextensions = "DNADerivedData")

test_DNA_returns <- lapply(DNA_ds_list$id[15:25], function(x) {
  robis::occurrence(
    datasetid = x,
    verbose = TRUE,
    hasextensions = "DNADerivedData",
    extensions = "DNADerivedData"
  )
})

lapply(test_DNA_returns, length)


#Test total time needed to download all seqs - 10.1 hours

tictoc::tic()
api_download <- robis::occurrence(hasextensions = "DNADerivedData", 
                                  extensions = "DNADerivedData")
tictoc::toc()

saveRDS(api_download, file = "OBIS_dna_Seqs_20231105.rds")

# Time needed to download same ones without DNA extension - 12.6 hours

tictoc::tic()
api_download_noext <- robis::occurrence(hasextensions = "DNADerivedData")
tictoc::toc()

#download parquet - 30 min

tictoc::tic()
fileMetadata <- httr::GET("https://api.obis.org/export?complete=true") %>% 
  httr::content()  %>% 
  .[["results"]] %>% 
  purrr::keep(~.x[['type']] == 'parquet') %>% 
  .[[1]]

if (!dir.exists("data")){
  dir.create("data")
}

sink(paste0("data/OBIS_download_", Sys.Date(), ".txt")) 
print(fileMetadata) 
sink()

fname <- fileMetadata %>% .[["s3path"]]
root <- "https://obis-datasets.ams3.digitaloceanspaces.com/"
destination_path <- paste0("data/", basename(fname))

if (!file.exists(destination_path)){
  
  options(timeout = 1e5)
  download.file(url = paste0(root, fname), 
                destfile = paste0("data/", 
                                  basename(fname)), 
                mode = "wb")
}
tictoc::toc()