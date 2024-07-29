library(duckdb)
# validate data structure of each zipped csv
library(archive)
library(readr)
library(dplyr)
library(dbplyr)
library(stringr)
library(janitor)
library(fs)
library(lubridate)
library(purrr)
library(cli)

check_duckdb_tables <- function(){
  # Establish the connection to the DuckDB database
  quack_conn <- dbConnect(duckdb(), "denue_historical.duckdb")
  
  # Ensure the connection is closed after execution, regardless of success or failure
  on.exit({
    if (dbIsValid(quack_conn)) {
      dbDisconnect(quack_conn)
    }
  })
  
  # Get the list of tables in the database
  tables <- dbListTables(quack_conn)
  
  # Inform the user about the tables in the database
  cli::cli_inform(
    paste0("Tables in denue_historical.duckdb: ", tables)
  )
}

ingest_denue_data_2_duckdb <- function(denue_df) {
  # Define a tryCatch block to handle errors
  tryCatch({
    # Establish the connection to the DuckDB database
    quack_conn <- dbConnect(duckdb(), "denue_historical.duckdb")
    
    # Ensure the connection is closed after execution, regardless of success or failure
    on.exit({
      if (dbIsValid(quack_conn)) {
        dbDisconnect(quack_conn)
      }
    })
    
    # Get the table name from the dataframe
    table_name <- denue_df |> 
      distinct(tbl_name) |>
      pull(tbl_name)
    
    print(table_name)
    
    clean_df <- denue_df |> 
      select(!tbl_name)
    
    # Write the table to the DuckDB database
    dbWriteTable(
      quack_conn,
      table_name,
      clean_df,
      overwrite = FALSE,
      append = TRUE
    )
    
    # Inform the user about successful ingestion
    cli::cli_inform(
      paste0("Table ", table_name, " has been ingested into denue_historical.duckdb")
    )
    
  }, error = function(e) {
    # Print an error message if something goes wrong
    cli::cli_alert_danger(
      paste0("Failed to ingest table into DuckDB: ", e$message)
    )
    
    # # compare denue df columns to the duck dk tbale columns
    # denue_df_col_names <- colnames(denue_df)
    # 
    # # Establish the connection to the DuckDB database
    # quack_conn <- dbConnect(duckdb(), "denue_historical.duckdb")
    # 
    # # Ensure the connection is closed after execution, regardless of success or failure
    # on.exit({
    #   if (dbIsValid(quack_conn)) {
    #     dbDisconnect(quack_conn)
    #   }
    # })
    # 
    # # Get the table name from the dataframe
    # table_name <- denue_df |> 
    #   distinct(tbl_name) |>
    #   pull(tbl_name)
    # 
    # quack_table_col_names <- tbl(quack_conn,
    #                              table_name) |> 
    #   colnames()
    # 
    # # Inform the user about the columns in the dataframe and the table
    # compared_columns <- setdiff(denue_df_col_names, quack_table_col_names)
    # 
    # cli::cli_alert_danger(
    #   paste0("Columns in dataframe but not in DuckDB table: ", compared_columns)
    # )
    # 
    
    
   # print(glimpse(denue_df))
  })
}


# Read Zipped Csvs --------------------------------------------------------


# need function that reads in an archive and reads the csv with denue inegi at the start of its name

# file in 2015 read in directly

# if not then read in metadata to check which snapshot it is  

identify_processing_type <- function(path_2_file_zip){
  
  # detect if file name base name has 2015 for example denue_31-33_25022015_csv.zip
  
  # detect if the  4 digits before _csv are 2015
  
  denue_file_categorized <- tibble(
    file_full_path = path_2_file_zip,
    processing_type = if_else(
      str_detect(basename(path_2_file_zip), "2015"),
      "direct",
      "indirect"
    )
  )
  
  return(denue_file_categorized)
  
}

read_denue_data <- function(path_2_file_zip){
  
  processing_type <- identify_processing_type(path_2_file_zip) |> 
    pull(processing_type)
  
  print(processing_type)
  
  if(processing_type == "direct"){
    denue_data <- direct_read_denue_data(path_2_file_zip)
  } else {
    denue_data <- indirect_read_denue_data(path_2_file_zip)
  }
  
  # Define the column names as a character vector
  column_names <- c(
    "id",
    "nom_estab",
    "raz_social",
    "codigo_act",
    "nombre_act",
    "per_ocu",
    "tipo_vial",
    "nom_vial",
    "tipo_v_e_1",
    "nom_v_e_1",
    "tipo_v_e_2",
    "nom_v_e_2",
    "tipo_v_e_3",
    "nom_v_e_3",
    "numero_ext",
    "letra_ext",
    "edificio",
    "edificio_e",
    "numero_int",
    "letra_int",
    "tipo_asent",
    "nomb_asent",
    "tipoCenCom",
    "nom_CenCom",
    "num_local",
    "cod_postal",
    "cve_ent",
    "entidad",
    "cve_mun",
    "municipio",
    "cve_loc",
    "localidad",
    "ageb",
    "manzana",
    "telefono",
    "correoelec",
    "www",
    "tipoUniEco",
    "latitud",
    "longitud",
    "fecha_alta",
    "clee",
    "tbl_name"
  )
  
  clean_denue_data <- denue_data |> 
    select(any_of(column_names)) |> 
    mutate(across(everything(), as.character)) |>
    mutate(
      across(
        where(is.character),
        ~iconv(.x, from = "latin1", to = "UTF-8")
      )
    )
  
  # if column clee missing add it with defailt value "missing"
  if(!"clee" %in% colnames(clean_denue_data)){
    clean_denue_data <- clean_denue_data |> 
      mutate(clee = "missing")
  }
  
  
  return(clean_denue_data)
}

direct_read_denue_data <- function(path_2_file_zip) {
  tryCatch({
    # Read the CSV file from the ZIP archive
    denue_data <- archive_read(path_2_file_zip) |>
      read_csv() |>
      clean_names() |>
      mutate(
        snapshot_period = "2015",
        tbl_name = "denue_2015"
      )
    
    # Return the processed data
    return(denue_data)
    
  }, error = function(e) {
    # Print an error message if something goes wrong
    message("Failed to read or process the data: ", e$message)
    
    # Return NULL or an empty data frame if an error occurs
    return(NULL)
  })
}

indirect_read_denue_data <- function(path_2_file_zip) {
  tryCatch({
    # Identify the metadata file
    diccionario_de_datos_file <- identify_metadata_file(path_2_file_zip)
    
    # Read the metadata file to extract the snapshot period
    snapshot_period <- archive_read(path_2_file_zip, file = diccionario_de_datos_file) |>
      readLines(n = 1) |>  
      str_remove("Identifier: ") |> 
      str_replace_all("\\.", "_") |>  # Escape period for str_replace_all
      str_replace_all("-", "_") |> 
      str_to_lower()
    
    # Identify the DENUE CSV file
    denue_csv_file <- identify_denue_csv_file(path_2_file_zip)
    
    # Read the DENUE CSV file
    denue_data <- archive_read(path_2_file_zip, file = denue_csv_file) |>
      read_csv() |>
      clean_names() |>
      mutate(
        snapshot_period = snapshot_period,
        tbl_name = paste0("denue_", snapshot_period)
      )
    
    # Return the processed data
    return(denue_data)
    
  }, error = function(e) {
    # Print an error message if something goes wrong
    message("Failed to read or process the data: ", e$message)
    
    # Return NULL or an empty data frame if an error occurs
    return(NULL)
  })
}


# utilities ---------------------------------------------------------------

identify_metadata_file <- function(path_2_file_zip){
  files_in_zip <- archive(path_2_file_zip)
  
  # metadata file
  diccionario_de_datos_file <- files_in_zip |> 
    filter(str_detect(path, "metadatos_denue.txt")) |> 
    pull(path)
  
  return(diccionario_de_datos_file)
}

identify_denue_csv_file <- function(path_2_file_zip){
  files_in_zip <- archive(path_2_file_zip)
  
  # denue csv file
  denue_csv_file <- files_in_zip |> 
    #detect files ending with .csv at the end of the file name and not the metadata file
    filter(str_detect(path, ".csv$") & !str_detect(path, "diccionario_de_datos.csv")) |>
    pull(path)
  
  return(denue_csv_file)
}

# Ingest Denue Data -------------------------------------------------------
ingest_denue_data_into_duckdb <- function(denue_zip){
  gc()
  cli::cli_inform(
    paste0("Ingesting ", denue_zip)
  )
  denue_data <- read_denue_data(denue_zip)
  
  cli::cli_inform(
    paste0("Ingesting ", denue_zip, " into duckdb")
  )
  
  ingest_denue_data_2_duckdb(denue_data)
  
  gc()
  
}

denue_zips <- dir_ls("denue zips")

safe_ingest_denue_data_into_duckdb <- safely(ingest_denue_data_into_duckdb)

ingestion_results <- denue_zips |>
  walk(~ safe_ingest_denue_data_into_duckdb(.x))


# checar que haya 17 tablas porque son 17 snapshots del denue en geenral basado enlos datos estatales desde 2015 en adelante