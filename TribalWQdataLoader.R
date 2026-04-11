
###############################################################################
# TRIBAL WATER QUALITY DATA LOADER
# Version: 2026-04-11 (Namespaced and Commented Edition)
# Features: Zero-Prompt SSO, Atomic Transactions, Interactive Menus
###############################################################################

### Setup Instructions for Your Team

# Before running this, instruct your users to set up their directory like this:

# 1. Open the R Project.
# 2. Create a folder named `ready_for_upload` in the same directory as the R script.
# 3. Ensure that the relevant Organization exists in the target database.
# 4. If the required Projects, MonLocs, Activities are not already in the target database,
#    place their filled `.xlsx` files into the `ready_for_upload` folder
#    (`Activities.xlsx`, `Results.xlsx`, `Projects.xlsx`, `MonLocs.xlsx`, and `WQTSData.xlsx`).

### The Complete Master Loader Script

# ==============================================================================
# TRIBAL WATER QUALITY DATA LOADER
# Features: Continuous Workflow Menu, Zero-Prompt Connect, Safe Transactions
# ==============================================================================

# Load required packages; all function calls below use package::function() notation
library(DBI)
library(odbc)
library(tidyverse)
library(readxl)

# --- 0. HELPER FUNCTION: LOGGING ---

# 1. Define the logs directory path
log_dir <- "logs/"

# 2. Check if the logs folder exists, and create it if it doesn't
if (!base::dir.exists(log_dir)) base::dir.create(log_dir)

# 3. Prepend the log directory path to the dynamically generated file name,
#    using the current timestamp to ensure unique log file names per session
log_file <- base::paste0(log_dir, "Log_DataLoad_", base::format(base::Sys.time(), "%Y%m%d_%H%M%S"), ".txt")

# write_log: appends a timestamped message to both the R console and the log file
write_log <- function(message) {
  # Prepend a formatted timestamp to the message
  timestamped_msg <- base::paste0("[", base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", message)
  
  # Print to the R console so the user sees progress in real time
  base::cat(timestamped_msg, "\n")
  
  # Append the same message to the session log file on disk
  base::write(timestamped_msg, file = log_file, append = TRUE)
}

# --- 1. ZERO-PROMPT CONNECTION MANAGER ---

# get_db_connection: attempts database connection using multiple strategies in order:
#   1) Windows SSO via DSN, 2) internal server credentials, 3) remote/VPN fallback.
# Arguments:
#   target_db - the name of the database to connect to ("TWQD" or "WQTS")
#   env       - the environment to connect to ("Production" or "Test")
get_db_connection <- function(target_db, env = "Production") {
  
  con <- NULL # initialize connection object as NULL before any attempt
  
  # Fetch the correct DSN from .Renviron based on whether we are in Test or Production
  dsn <- base::ifelse(env == "Test", base::Sys.getenv("DB_DSN_TEST"), base::Sys.getenv("DB_DSN_PROD"))
  
  # Read driver and server connection details from .Renviron; fall back to a common default driver
  drv    <- base::Sys.getenv("DB_DRIVER", unset = "ODBC Driver 17 for SQL Server")
  srv_int <- base::Sys.getenv("DB_SERVER_INT")  # internal network server address
  srv_ext <- base::Sys.getenv("DB_SERVER_EXT")  # external/VPN server address
  u      <- base::Sys.getenv("DB_USER")          # database username
  p      <- base::Sys.getenv("DB_PASSWORD")      # database password
  
  # --- Attempt 1: Windows SSO via DSN (preferred; no username/password needed) ---
  if (dsn != "") {
    write_log(base::paste("Attempting SSO connection to", target_db, env, "via DSN..."))
    base::try({
      con <- DBI::dbConnect(odbc::odbc(), dsn, Database = target_db, Trusted_Connection = "Yes")
    }, silent = TRUE)
  }
  
  # Return immediately if SSO succeeded and the connection is valid
  if (!base::is.null(con) && DBI::dbIsValid(con)) return(con)
  
  # --- Attempt 2: Internal network server with username/password credentials ---
  write_log("DSN skipped or failed. Attempting internal server connection...")
  base::try({
    con <- DBI::dbConnect(odbc::odbc(), Driver = drv, Server = srv_int, Database = target_db, UID = u, PWD = p)
  }, silent = TRUE)
  
  # Return if the internal connection succeeded
  if (!base::is.null(con) && DBI::dbIsValid(con)) return(con)
  
  # --- Attempt 3: Remote/VPN fallback server ---
  write_log("Internal server failed. Attempting remote/VPN fallback...")
  base::tryCatch({
    con <- DBI::dbConnect(odbc::odbc(), Driver = drv, Server = srv_ext, Database = target_db, UID = u, PWD = p)
    write_log("SUCCESS: Connected to Database.")
    return(con)
  }, error = function(e) {
    # All three attempts failed; halt execution with an informative error message
    base::stop("FATAL ERROR: All connection attempts failed. Please check network and .Renviron variables.")
  })
}

# --- 2. MAIN INTERACTIVE WORKFLOW LOOP ---

# run_data_loader: the top-level entry point that presents the menu and routes
#   the user to the appropriate upload workflow (TWQD or WQTS)
run_data_loader <- function() {
  
  upload_dir <- "ready_for_upload/" # folder where all input Excel files must be placed
  
  # Create the upload folder if it doesn't already exist
  if (!base::dir.exists(upload_dir)) base::dir.create(upload_dir)
  
  # 1. Check .Renviron for a flag indicating whether Test environments should be shown
  is_multi <- base::as.logical(base::Sys.getenv("MULTIPLE_ENVIRONMENTS", unset = "FALSE"))
  
  # 2. Dynamically build the menu choices based on the multi-environment flag
  if (is_multi) {
    # Show separate Production and Test options for each database
    menu_choices <- c(
      "TWQD - Production", "TWQD - Test",
      "WQTS - Production", "WQTS - Test",
      "Exit Script"
    )
  } else {
    # Show a simplified single-environment menu
    menu_choices <- c("Load Data to TWQD", "Load Data to WQTS", "Exit Script")
  }
  
  # Keep presenting the menu after each load until the user chooses Exit
  while (TRUE) {
    
    # 3. Present the popup menu and capture the user's selection
    action <- utils::select.list(
      choices = menu_choices,
      title   = "MAIN MENU: Select Database destination",
      graphics = TRUE
    )
    
    # Exit the loop if the user selects Exit or dismisses the dialog
    if (action == "Exit Script" || action == "") {
      write_log("Exiting Data Loader. Goodbye!")
      break
    }
    
    # 4. Parse the user's selection to determine target database and environment
    target_db <- base::ifelse(base::grepl("TWQD", action), "TWQD", "WQTS") # "TWQD" or "WQTS"
    env       <- base::ifelse(base::grepl("Test", action), "Test", "Production") # "Test" or "Production"
    
    # =========================================================
    # WORKFLOW A: TWQD (Projects, Sites, Discrete Data)
    # =========================================================
    if (target_db == "TWQD") {
      
      write_log(base::paste("\n--- STARTING TWQD", base::toupper(env), "LOAD ---"))
      
      # Build file paths for the two required TWQD input files
      file_act <- base::paste0(upload_dir, "Activities.xlsx")
      file_res <- base::paste0(upload_dir, "Results.xlsx")
      
      # Abort this iteration if either required file is missing
      if (!base::file.exists(file_act) || !base::file.exists(file_res)) {
        write_log("ERROR: Activities.xlsx or Results.xlsx not found in ready_for_upload folder.")
        next
      }
      
      # Read incoming Excel data into data frames
      new_activities <- readxl::read_excel(file_act)
      new_results    <- readxl::read_excel(file_res)
      
      # --- 1. MANDATORY DATA FORMATTING ---
      # Coerce date and time columns to the correct R types expected by the database
      
      new_activities$StartDate      <- base::as.Date(new_activities$StartDate)
      
      # EndDate is optional; only coerce it if the column exists
      if ("EndDate" %in% base::colnames(new_activities))
        new_activities$EndDate <- base::as.Date(new_activities$EndDate)
      
      # Extract only the HH:MM portion from the time string (strips date prefix added by Excel)
      new_activities$StartTime <- stringr::str_extract(base::as.character(new_activities$StartTime), "\\d{2}:\\d{2}")
      
      if ("EndTime" %in% base::colnames(new_activities))
        new_activities$EndTime <- stringr::str_extract(base::as.character(new_activities$EndTime), "\\d{2}:\\d{2}")
      
      # Stamp all incoming activities with the current date/time as the last change date
      new_activities$LastChangeDate <- base::Sys.time()
      
      if ("DeletedDate" %in% base::colnames(new_activities))
        new_activities$DeletedDate <- base::as.Date(new_activities$DeletedDate)
      
      # ResultMeasureValue must be character to handle both numeric and text results (e.g. "<MDL")
      new_results$ResultMeasureValue <- base::as.character(new_results$ResultMeasureValue)
      new_results$LastChangeDate     <- base::Sys.time()
      
      if ("SampleDateTime" %in% base::colnames(new_results))
        new_results$SampleDateTime <- base::as.Date(new_results$SampleDateTime)
      
      if ("DeletedDate" %in% base::colnames(new_results))
        new_results$DeletedDate <- base::as.Date(new_results$DeletedDate)
      
      write_log(base::paste("Loaded Input Files. Activities:", base::nrow(new_activities), "Results:", base::nrow(new_results)))
      
      # Open database connection using the parsed target and environment
      con <- get_db_connection("TWQD", env)
      
      # --- PRE-FLIGHT CHECKS ---
      
      # A. Check Projects: ensure every project referenced in Activities exists in the database
      req_projects <- base::unique(new_activities$ProjectIdentifier)
      db_projects  <- DBI::dbGetQuery(con, "SELECT ProjectIdentifier FROM Project")
      
      # Identify any projects in the file that are not yet in the database
      missing_projects <- base::setdiff(req_projects, db_projects$ProjectIdentifier)
      
      if (base::length(missing_projects) > 0) {
        # Ask the user whether to auto-load the missing projects from a template file
        choice <- utils::select.list(
          c("Yes, load Projects.xlsx", "No, cancel load"),
          title = base::paste(base::length(missing_projects), "Project(s) missing. Load from template?")
        )
        
        if (choice == "Yes, load Projects.xlsx") {
          Projdata                <- readxl::read_excel(base::paste0(upload_dir, "Projects.xlsx"))
          Projdata$LastChangeDate <- base::Sys.time()
          DBI::dbWriteTable(con, "Project", Projdata, append = TRUE, row.names = FALSE)
          write_log("SUCCESS: Projects loaded from template.")
        } else {
          # User chose not to proceed; disconnect and return to the menu
          DBI::dbDisconnect(con)
          next
        }
      }
      
      # B. Check Monitoring Locations: ensure every site referenced in Activities exists
      req_sites <- base::unique(new_activities$MonitoringLocationIdentifier)
      db_sites  <- DBI::dbGetQuery(con, "SELECT MonitoringLocationIdentifier FROM MonitoringLocation")
      
      # Identify any monitoring locations in the file not yet in the database
      missing_sites <- base::setdiff(req_sites, db_sites$MonitoringLocationIdentifier)
      
      if (base::length(missing_sites) > 0) {
        choice <- utils::select.list(
          c("Yes, load MonLocs.xlsx", "No, cancel load"),
          title = base::paste(base::length(missing_sites), "Site(s) missing. Load from template?")
        )
        
        if (choice == "Yes, load MonLocs.xlsx") {
          MonLocData                <- readxl::read_excel(base::paste0(upload_dir, "MonLocs.xlsx"))
          MonLocData$LastChangeDate <- base::Sys.time()
          DBI::dbWriteTable(con, "MonitoringLocation", MonLocData, append = TRUE, row.names = FALSE)
          write_log("SUCCESS: Monitoring Locations loaded from template.")
        } else {
          DBI::dbDisconnect(con)
          next
        }
      }
      
      # --- IDEMPOTENCY CHECK (PREVENT DUPLICATES) ---
      # Pull all existing ActivityIdentifiers from the database to compare against incoming data
      existing_ids <- DBI::dbGetQuery(con, "SELECT ActivityIdentifier FROM Activity")
      
      # Keep only Activities whose identifiers are NOT already in the database
      unique_activities <- new_activities %>%
        dplyr::anti_join(existing_ids, by = "ActivityIdentifier")
      
      # Keep only Results that belong to the Activities we are actually going to insert
      unique_results <- new_results %>%
        dplyr::semi_join(unique_activities, by = "ActivityIdentifier")
      
      # Report how many duplicate activities were detected and skipped
      count_dropped <- base::nrow(new_activities) - base::nrow(unique_activities)
      write_log(base::paste("IDEMPOTENCY CHECK:", count_dropped, "Activities already exist and were dropped."))
      write_log(base::paste("READY TO LOAD:", base::nrow(unique_activities), "Activities and", base::nrow(unique_results), "Results."))
      
      if (base::nrow(unique_activities) > 0) {
        
        # --- TRANSACTIONAL UPLOAD (ATOMIC COMMIT) ---
        # Re-apply type coercions on the filtered subset before writing
        unique_activities$StartDate          <- base::as.Date(unique_activities$StartDate)
        unique_results$ResultMeasureValue    <- base::as.character(unique_results$ResultMeasureValue)
        
        DBI::dbBegin(con) # begin a database transaction; nothing is committed until dbCommit()
        
        base::tryCatch({
          # Stage Activities into the database (within the open transaction)
          DBI::dbWriteTable(con, "Activity", unique_activities, append = TRUE, row.names = FALSE)
          write_log(base::paste("Staged", base::nrow(unique_activities), "rows into Activity Table."))
          
          # Stage Results into the database (Activity must exist first due to FK constraints)
          DBI::dbWriteTable(con, "Result", unique_results, append = TRUE, row.names = FALSE)
          write_log(base::paste("Staged", base::nrow(unique_results), "rows into Result Table."))
          
          DBI::dbCommit(con) # atomically commit both tables; either both succeed or neither do
          write_log("SUCCESS: Transaction Committed. Data is live in TWQD.")
          
        }, error = function(e) {
          DBI::dbRollback(con) # roll back all staged changes if any error occurred
          write_log(base::paste("TRANSACTION FAILED & ROLLED BACK. Error:", e$message))
        })
      }
      
      DBI::dbDisconnect(con) # always close the connection when done with this workflow
      write_log("--- TWQD LOAD COMPLETE. RETURNING TO MENU ---\n")
    }
    
    # =========================================================
    # WORKFLOW B: WQTS (Continuous Time Series Data)
    # =========================================================
    if (action == "Load Data to WQTS") {
      
      write_log("\n--- STARTING WQTS LOAD ---")
      
      # Build path to the single required WQTS input file
      file_wqts <- base::paste0(upload_dir, "WQTSData.xlsx")
      
      # Abort this iteration if the file is missing
      if (!base::file.exists(file_wqts)) {
        write_log("ERROR: WQTSData.xlsx not found in ready_for_upload folder.")
        next
      }
      
      # Open connection to the WQTS database (always Production in single-environment mode)
      con <- get_db_connection("WQTS")
      
      # Read the continuous time series data from the Excel template
      wqts_data <- readxl::read_excel(file_wqts)
      
      base::tryCatch({
        # Append all rows to the WQTS_Data table (no idempotency check for time series)
        DBI::dbWriteTable(con, "WQTS_Data", wqts_data, append = TRUE, row.names = FALSE)
        write_log(base::paste("SUCCESS: Uploaded", base::nrow(wqts_data), "rows to WQTS_Data."))
      }, error = function(e) {
        write_log(base::paste("ERROR: WQTS Upload Failed.", e$message))
      })
      
      DBI::dbDisconnect(con) # close the WQTS connection
      write_log("--- WQTS LOAD COMPLETE. RETURNING TO MENU ---\n")
    }
  }
}

# --- 3. EXECUTE SCRIPT ---

# Entry point: call the main loader function to start the interactive session
run_data_loader()

