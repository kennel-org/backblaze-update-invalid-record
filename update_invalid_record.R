# Update Backblaze drive_stats invalid record
# 2024/12/03 kennel.org

library("RMariaDB")
library("tictoc")
library("yaml")

get_script_directory <- function() {
  # Check for command line argument for --file= (Rscript execution)
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    # Extract the script path from the --file= argument
    script_path <- normalizePath(sub("^--file=", "", file_arg))
    # Return the directory containing the script
    return(dirname(script_path))
  }
  
  # Check if running in RStudio and get the active document's path
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      rstudioapi::isAvailable()) {
    script_path <- rstudioapi::getActiveDocumentContext()$path
    if (nzchar(script_path)) {
      # Return the directory containing the active script
      return(dirname(normalizePath(script_path)))
    }
  }
  
  # Return NA if the script directory could not be determined
  return(NA)
}

# Set working directory to the script's directory
script_directory <- get_script_directory()
print(script_directory)
setwd(script_directory)

outfile_path <- "outfile/"
infile_path <- "infile/"
tmp_path <- "tmp/"

# List of directories to check and create if missing
dirs <- c(outfile_path, infile_path, tmp_path)

# Loop through each directory and create it if it doesn't exist
for (dir in dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir)
    cat(sprintf("Directory '%s' created.\n", dir))
  } else {
    cat(sprintf("Directory '%s' already exists.\n", dir))
  }
}

# Load the configuration file (config.yml)
config <- read_yaml("config.yml")
db_config <- config$db_connection

# Connect to the database with a hard-coded timeout value (Inf)
con <- dbConnect(
  RMariaDB::MariaDB(),
  username = db_config$username,
  password = db_config$password,
  host     = db_config$host,
  port     = db_config$port,
  dbname   = db_config$dbname,
  timeout  = Inf  # Timeout is hardcoded as Inf, not read from the YAML file
)

tic()
date_str <- strftime(
  dbGetQuery(
    con,
    "SELECT date FROM drive_stats WHERE ID = (SELECT MAX(ID) FROM drive_stats);"
  )[1, 1],
  format = "%y%m%d"
)
toc()

# -------------------------------------------------------------------------------------------------

infile_path <-
  paste0("outfile/", date_str, "-infile_mariadb_initial.csv")

# Save initial status

if (file.exists(infile_path)) {
  df <- read.csv(infile_path)
} else {
  sql <-
    'SELECT vendor, model, serial_number, capacity_bytes, modified, count(*)
  FROM drive_stats
  GROUP BY vendor, model, modified, capacity_bytes;'
  
  tic()
  df <- dbGetQuery(con, sql)
  toc()
  
  write.csv(df, infile_path, na = "", row.names = FALSE)
}

# UPDATE drive_stats
#
sql <-
  'START TRANSACTION;'
dbSendQuery(con, sql)


# -------------------------------------------------------------------------------------------------

infile_path <-
  paste0("outfile/", date_str, "-count_mariadb_initial.csv")

# Save initial status

if (file.exists(infile_path)) {
  df <- read.csv(infile_path)
} else {
  sql <-
    'SELECT model, modified, count(*)
  FROM drive_stats
  GROUP BY model, modified;'
  
  tic()
  df <- dbGetQuery(con, sql)
  toc()
  
  write.csv(df, infile_path, na = "", row.names = FALSE)
}

# -------------------------------------------------------------------------------------------------
sql <- "
UPDATE drive_stats
  SET vendor = 'WDC', model = 'WUH721816ALE6L4', modified = 1
  WHERE vendor = 'WUH721816ALE6L4';
"
tic()
dbSendQuery(con, sql)
toc()

sql <- "
UPDATE drive_stats
  SET capacity_bytes = 4000787030016, modified = 1
  WHERE model = 'HMS5C4040ALE640' AND capacity_bytes <> 4000787030016;
"
tic()
dbSendQuery(con, sql)
toc()

sql <- "
UPDATE drive_stats
  SET capacity_bytes = 6001175126016, modified = 1
  WHERE model = 'ST6000DX000' AND capacity_bytes <> 6001175126016;
"
tic()
dbSendQuery(con, sql)
toc()

sql <- "
UPDATE drive_stats
  SET capacity_bytes = 4000787030016, modified = 1
  WHERE
  model = 'ST4000DM000' AND (
  capacity_bytes = 600332565813390500 OR
  capacity_bytes = 137438952960);

"
tic()
dbSendQuery(con, sql)
toc()

# -------------------------------------------------------------------------------------------------

infile_path <-
  paste0("outfile/", date_str, "-infile_mariadb_initial2.csv")

# Save initial 2 status

if (file.exists(infile_path)) {
  df <- read.csv(infile_path)
} else {
  sql <-
    'SELECT vendor, model, serial_number, capacity_bytes, modified, count(*)
  FROM drive_stats
  GROUP BY vendor, model, modified, capacity_bytes;'
  
  tic()
  df <- dbGetQuery(con, sql)
  toc()
  
  write.csv(df, infile_path, na = "", row.names = FALSE)
}

# -------------------------------------------------------------------------------------------------

invalid_capacity <-
  df[df$capacity_bytes < 0, ]

valid_capacity <- df[df$capacity_bytes > 0, ]

x <-
  merge(invalid_capacity,
        valid_capacity,
        by = 'model',
        all = FALSE)

x <- x[!duplicated(x$model), ]

write.csv(
  x,
  paste0("outfile/", date_str, "-invalid_capacity.csv"),
  na = "",
  row.names = FALSE
)

# -------------------------------------------------------------------------------------------------

for (i in 1:nrow(x)) {
  model <- x$model[i]
  incorrect_capacity <- as.numeric(x$capacity_bytes.x[i])
  correct_capacity <- as.numeric(x$capacity_bytes.y[i])
  
  sql <- sprintf(
    "UPDATE drive_stats
     SET
       capacity_bytes = %.0f,
       modified = 1
     WHERE
       model = '%s' AND capacity_bytes = %.0f;",
    correct_capacity,
    model,
    incorrect_capacity
  )
  
  tic()
  dbExecute(con, sql)
  toc()
}

# -------------------------------------------------------------------------------------------------
# Save final status

infile_path <-
  paste0("outfile/", date_str, "-infile_mariadb_final.csv")

if (file.exists(infile_path)) {
  df <- read.csv(infile_path)
} else {
  sql <-
    'SELECT vendor, model, serial_number, capacity_bytes, modified, count(*)
  FROM drive_stats
  GROUP BY vendor, model, modified, capacity_bytes;'
  
  tic()
  df <- dbGetQuery(con, sql)
  toc()
  
  write.csv(df, infile_path, na = "", row.names = FALSE)
}
