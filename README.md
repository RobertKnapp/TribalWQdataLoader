README.md
# Tribal Water Quality Data Loader (TWQD & WQTS)

A robust, interactive R script designed to safely authenticate and load water quality data into the Tribal Water Quality Database (TWQD) and continuous Water Quality Time Series (WQTS) databases. 

This tool is built for Tribal water quality staff of all programming levels. It uses interactive menus and standard Excel templates to handle data uploads without requiring users to write or modify any R code.

## Key Features
*   **Zero-Prompt Authentication:** Securely manages Windows SSO (DSN) and manual database credentials in the background using a hidden `.Renviron` file.
*   **Safe Transactions:** Uses atomic database commits. If an Activity loads but the Result fails, the script automatically rolls back to prevent "orphaned" empty site visits.
*   **Idempotency (No Duplicates):** Automatically checks the database for existing records before uploading. You can safely run the exact same data file multiple times without causing SQL primary key crashes.
*   **Automated Pre-Flight Checks:** Verifies that required parent records (Organizations, Projects, Monitoring Locations) exist before loading data. Loaded Projects, and MonLocs if missing and Excel files are provided.
*   **Automated Documentation:** Creates logs directory on first run. Creates log file written to logs directory with each load successful or unsuccessful.

## Prerequisites
*   R and RStudio installed.
*   Access to the MS SQL Server hosting TWQD and WQTS.
*   The following R packages: `DBI`, `odbc`, `tidyverse`, and `readxl`.

## Setup Instructions

**1. Configure Your Secrets**
Never type your password into the R script. Instead, we use a hidden `.Renviron` file.
1. In RStudio, install `usethis` if you haven't already (`install.packages("usethis")`).
2. Run `usethis::edit_r_environ(scope = "project")` in the console.
3. Copy the contents of the `.Renviron.example` file into the new window, fill in your specific server paths and credentials, save, and restart your R session.
4. Ensure the .gitignore file is in your project. The one provided ignores the .Renviron file to protect your credeintials from getting pushed to GitHub.

**2. Prepare Your Data**
1. Ensure a folder named `ready_for_upload` exists in the same folder as the script.
2. Drop your filled Excel templates into this folder. Ensure they are named exactly:
   * `Activities.xlsx`
   * `Results.xlsx`
   * `Projects.xlsx` (Used automatically if a project is missing)
   * `MonLocs.xlsx` (Used automatically if a site is missing)
  