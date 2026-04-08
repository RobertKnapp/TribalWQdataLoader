

### Setup Instructions for Your Team
#       Before running this, instruct your users to set up their directory like this:
#         1. Open the R Project.
#         2. Create a folder named `ready_for_upload` in the same directory as the R script.
#         3. Ensure that the relevant Organization exists in the target database. 
#         4. If the required Projects, MonLocs, Activities are not already in the target database, 
#              place their filled `.xlsx` files into the `ready_for_upload` folder 
#              (`Activities.xlsx`, `Results.xlsx`, `Projects.xlsx`, `MonLocs.xlsx`, and `WQTSData.xlsx`).


### The Complete Master Loader Script

# ==============================================================================
# TRIBAL WATER QUALITY DATA LOADER
# Features: Continuous Workflow Menu, Zero-Prompt Connect, Safe Transactions
# ==============================================================================

library(DBI)
library(odbc)
library(tidyverse)
library(readxl)

# --- 0. HELPER FUNCTION: LOGGING ---
log_file <- paste0("Log_DataLoad_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
write_log <- function(message) { 
  timestamped_msg <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", message) 
  cat(timestamped_msg, "\n") 
  write(timestamped_msg, file = log_file, append = TRUE) 
}

# --- 1. ZERO-PROMPT CONNECTION MANAGER ---
get_db_connection <- function(target_db) {
  con <- NULL
  
  is_multi <- as.logical(Sys.getenv("MULTIPLE_ENVIRONMENTS", unset = "FALSE"))
  if (is_multi) {
    env <- select.list(c("Production", "Test"), title = paste("Select", target_db, "Environment:"))
    dsn <- ifelse(env == "Test", Sys.getenv("DB_DSN_TEST"), Sys.getenv("DB_DSN_PROD"))
  } else {
    dsn <- Sys.getenv("DB_DSN_PROD")
  }
  
  drv <- Sys.getenv("DB_DRIVER", unset = "ODBC Driver 17 for SQL Server")
  srv_int <- Sys.getenv("DB_SERVER_INT")
  srv_ext <- Sys.getenv("DB_SERVER_EXT")
  u <- Sys.getenv("DB_USER")
  p <- Sys.getenv("DB_PASSWORD")
  
  if (dsn != "") {
    write_log(paste("Attempting SSO connection to", target_db, "via DSN..."))
    try({ con <- dbConnect(odbc::odbc(), dsn, Database = target_db, Trusted_Connection = "Yes") }, silent = TRUE)
  }
  if (!is.null(con) && dbIsValid(con)) return(con)
  
  write_log("DSN skipped or failed. Attempting internal server connection...")
  try({ con <- dbConnect(odbc::odbc(), Driver = drv, Server = srv_int, Database = target_db, UID = u, PWD = p) }, silent = TRUE)
  if (!is.null(con) && dbIsValid(con)) return(con)
  
  write_log("Internal server failed. Attempting remote/VPN fallback...")
  tryCatch({ 
    con <- dbConnect(odbc::odbc(), Driver = drv, Server = srv_ext, Database = target_db, UID = u, PWD = p) 
    write_log("SUCCESS: Connected to Database.")
    return(con)
  }, error = function(e) {
    stop("FATAL ERROR: All connection attempts failed. Please check network and .Renviron variables.")
  })
}

# --- 2. MAIN INTERACTIVE WORKFLOW LOOP ---
run_data_loader <- function() {
  
  # Define relative paths
  upload_dir <- "ready_for_upload/"
  if(!dir.exists(upload_dir)) dir.create(upload_dir)
  
  while(TRUE) {
    action <- select.list(
      choices = c("Load Data to TWQD", "Load Data to WQTS", "Exit Script"),
      title = "\nMAIN MENU: What would you like to do?",
      graphics = TRUE
    )
    
    if (action == "Exit Script" || action == "") {
      write_log("Exiting Data Loader. Goodbye!")
      break
    }
    
    # =========================================================
    # WORKFLOW A: TWQD (Projects, Sites, Discrete Data)
    # =========================================================
    if (action == "Load Data to TWQD") {
      write_log("\n--- STARTING TWQD LOAD ---")
      
      file_act <- paste0(upload_dir, "Activities.xlsx")
      file_res <- paste0(upload_dir, "Results.xlsx")
      
      if(!file.exists(file_act) || !file.exists(file_res)) {
        write_log("ERROR: Activities.xlsx or Results.xlsx not found in ready_for_upload folder.")
        next
      }
      
      # Read incoming data
      new_activities <- read_excel(file_act)
      new_results <- read_excel(file_res)
      
      # --- 1. MANDATORY DATA FORMATTING ---
      # Apply formatting based on NWIFC templates to prevent SQL Type/Truncation Errors
      
      # --- A. Activities Formatting ---
      # 1. Dates: SQL expects DATE format
      new_activities$StartDate <- as.Date(new_activities$StartDate)
      if("EndDate" %in% colnames(new_activities)) {
        new_activities$EndDate <- as.Date(new_activities$EndDate)
      }
      
      # 2. Times: Safely extract ONLY the "HH:MM" using stringr
      # This strips Excel dummy dates ("1899-12-31") and prevents right truncation
      new_activities$StartTime <- str_extract(as.character(new_activities$StartTime), "\\d{2}:\\d{2}")
      
      if("EndTime" %in% colnames(new_activities)) {
        new_activities$EndTime <- str_extract(as.character(new_activities$EndTime), "\\d{2}:\\d{2}")
      }
      
      # 3. Timestamps: Use Sys.time() for DATETIME or Sys.Date() if the column is strict DATE
      new_activities$LastChangeDate <- Sys.time() 
      
      if("DeletedDate" %in% colnames(new_activities)) {
        new_activities$DeletedDate <- as.Date(new_activities$DeletedDate)
      }
      
      # --- B. Results Formatting ---
      # 1. Result Values: Varchar in SQL, converting to character prevents extra decimal digits
      new_results$ResultMeasureValue <- as.character(new_results$ResultMeasureValue) 
      
      # 2. Dates and Timestamps
      new_results$LastChangeDate <- Sys.time()
      
      if("SampleDateTime" %in% colnames(new_results)) {
        new_results$SampleDateTime <- as.Date(new_results$SampleDateTime) 
      }
      
      if("DeletedDate" %in% colnames(new_results)) {
        new_results$DeletedDate <- as.Date(new_results$DeletedDate)
      }
      
      write_log(paste("Loaded Input Files. Activities:", nrow(new_activities), "Results:", nrow(new_results)))
      
      con <- get_db_connection("TWQD")
      
      # --- PRE-FLIGHT CHECKS ---
      # A. Check Projects
      req_projects <- unique(new_activities$ProjectIdentifier)
      db_projects <- dbGetQuery(con, "SELECT ProjectIdentifier FROM Project")
      missing_projects <- setdiff(req_projects, db_projects$ProjectIdentifier)
      
      if(length(missing_projects) > 0) {
        choice <- select.list(c("Yes, load Projects.xlsx", "No, cancel load"), 
                              title = paste(length(missing_projects), "Project(s) missing. Load from template?"))
        if(choice == "Yes, load Projects.xlsx") {
          Projdata <- read_excel(paste0(upload_dir, "Projects.xlsx"))
          Projdata$LastChangeDate <- Sys.time()
          dbWriteTable(con, "Project", Projdata, append = TRUE, row.names = FALSE)
          write_log("SUCCESS: Projects loaded from template.")
        } else {
          dbDisconnect(con)
          next
        }
      }
      
      # B. Check Monitoring Locations
      req_sites <- unique(new_activities$MonitoringLocationIdentifier)
      db_sites <- dbGetQuery(con, "SELECT MonitoringLocationIdentifier FROM MonitoringLocation")
      missing_sites <- setdiff(req_sites, db_sites$MonitoringLocationIdentifier)
      
      if(length(missing_sites) > 0) {
        choice <- select.list(c("Yes, load MonLocs.xlsx", "No, cancel load"), 
                              title = paste(length(missing_sites), "Site(s) missing. Load from template?"))
        if(choice == "Yes, load MonLocs.xlsx") {
          MonLocData <- read_excel(paste0(upload_dir, "MonLocs.xlsx"))
          MonLocData$LastChangeDate <- Sys.time()
          dbWriteTable(con, "MonitoringLocation", MonLocData, append = TRUE, row.names = FALSE)
          write_log("SUCCESS: Monitoring Locations loaded from template.")
        } else {
          dbDisconnect(con)
          next
        }
      }
      
      # --- IDEMPOTENCY CHECK (PREVENT DUPLICATES) ---
      existing_ids <- dbGetQuery(con, "SELECT ActivityIdentifier FROM Activity")
      
      # Keep only Activities that do NOT exist in the database
      unique_activities <- new_activities %>% anti_join(existing_ids, by = "ActivityIdentifier")
      
      # Filter incoming Results to match ONLY the unique Activities we are keeping
      unique_results <- new_results %>% semi_join(unique_activities, by = "ActivityIdentifier")
      
      count_dropped <- nrow(new_activities) - nrow(unique_activities)
      write_log(paste("IDEMPOTENCY CHECK:", count_dropped, "Activities already exist and were dropped."))
      write_log(paste("READY TO LOAD:", nrow(unique_activities), "Activities and", nrow(unique_results), "Results."))
      
      if(nrow(unique_activities) > 0) {
        # --- TRANSACTIONAL UPLOAD (ATOMIC COMMIT) ---
        # Format necessary dates/strings
        unique_activities$StartDate <- as.Date(unique_activities$StartDate)
        unique_results$ResultMeasureValue <- as.character(unique_results$ResultMeasureValue) 
        
        dbBegin(con) # Start Transaction
        tryCatch({
          dbWriteTable(con, "Activity", unique_activities, append = TRUE, row.names = FALSE)
          write_log(paste("Staged", nrow(unique_activities), "rows into Activity Table."))
          
          dbWriteTable(con, "Result", unique_results, append = TRUE, row.names = FALSE)
          write_log(paste("Staged", nrow(unique_results), "rows into Result Table."))
          
          dbCommit(con) # Save to Database
          write_log("SUCCESS: Transaction Committed. Data is live in TWQD.")
          
        }, error = function(e) {
          dbRollback(con) # Undo everything if a failure occurs
          write_log(paste("TRANSACTION FAILED & ROLLED BACK. Error:", e$message))
        })
      }
      
      dbDisconnect(con)
      write_log("--- TWQD LOAD COMPLETE. RETURNING TO MENU ---\n")
    }
    
    # =========================================================
    # WORKFLOW B: WQTS (Continuous Data)
    # =========================================================
    if (action == "Load Data to WQTS") {
      write_log("\n--- STARTING WQTS LOAD ---")
      
      file_wqts <- paste0(upload_dir, "WQTSData.xlsx")
      if(!file.exists(file_wqts)) {
        write_log("ERROR: WQTSData.xlsx not found in ready_for_upload folder.")
        next
      }
      
      con <- get_db_connection("WQTS")
      wqts_data <- read_excel(file_wqts)
      
      tryCatch({
        dbWriteTable(con, "WQTS_Data", wqts_data, append = TRUE, row.names = FALSE)
        write_log(paste("SUCCESS: Uploaded", nrow(wqts_data), "rows to WQTS_Data."))
      }, error = function(e) {
        write_log(paste("ERROR: WQTS Upload Failed.", e$message))
      })
      
      dbDisconnect(con)
      write_log("--- WQTS LOAD COMPLETE. RETURNING TO MENU ---\n")
    }
  }
}

# --- 3. EXECUTE SCRIPT ---
run_data_loader()


