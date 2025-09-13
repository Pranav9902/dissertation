# EPL Injury Data Scraper for Past 10 Seasons (2015–2024)
# Author: Pranav Prasanth
# Description:
#   This script scrapes injury data for all English Premier League (EPL) players from Transfermarkt for the 2015–2024 seasons.
#   Output: epl_injuries_2015_2024.csv (main injury dataset), epl_injury_scrape_log.txt (scraping log)
# Usage:
#   Run with required R packages installed: worldfootballR, dplyr, purrr, lubridate.

library(worldfootballR)
library(dplyr)
library(purrr)
library(lubridate)

# Set file paths for output and logging
main_save_file <- "C:/Users/LENOVO/Downloads/Dissertation Dataset/epl_injuries_2015_2024.csv"
log_file <- "C:/Users/LENOVO/Downloads/Dissertation Dataset/epl_injury_scrape_log.txt"

years <- 2015:2024

# Helper function for safe scraping with error handling
safe_tm_player_injury_history <- safely(tm_player_injury_history, otherwise = NULL)

# Resume from existing data if available to avoid duplicate scraping
if (file.exists(main_save_file)) {
  all_injuries <- read.csv(main_save_file, stringsAsFactors = FALSE)
  completed_years <- unique(all_injuries$season_start_year)
  years <- setdiff(years, completed_years)
  cat("Resuming from years:", paste(years, collapse = ", "), "\n", file = log_file, append = TRUE)
} else {
  all_injuries <- data.frame()
}

# Iterate over each season
for (yr in years) {
  cat("Processing season:", yr, "\n", file = log_file, append = TRUE)
  team_urls <- tryCatch({
    tm_league_team_urls(country_name = "England", start_year = yr)
  }, error=function(e) {
    cat("Failed to get teams for", yr, ":", e$message, "\n", file = log_file, append = TRUE)
    character(0)
  })
  if (length(team_urls) == 0) next
  
  season_injuries <- data.frame()
  for (team_url in team_urls) {
    cat("  Team:", team_url, "\n", file = log_file, append = TRUE)
    player_urls <- tryCatch({
      tm_team_player_urls(team_url = team_url)
    }, error=function(e) {
      cat("    Failed to get players for team", team_url, ":", e$message, "\n", file = log_file, append = TRUE)
      character(0)
    })
    cat("    Number of player URLs for team:", length(player_urls), "\n", file = log_file, append = TRUE)
    if (length(player_urls) == 0) next
    
    team_injuries_list <- map(player_urls, function(purl) {
      cat("      Processing player:", purl, "\n", file = log_file, append = TRUE)
      res <- safe_tm_player_injury_history(player_urls = purl)
      # Filter injuries to those that occurred within the current season
      if (!is.null(res$result) && nrow(res$result) > 0) {
        df <- res$result
        if (!"date_from" %in% names(df)) {
          df$date_from <- NA
        }
        df$date_from <- as.Date(df$date_from)
        season_start <- as.Date(paste0(yr, "-07-01"))
        season_end   <- as.Date(paste0(yr + 1, "-06-30"))
        df <- df %>% filter(!is.na(date_from) & date_from >= season_start & date_from <= season_end)
        if (nrow(df) > 0) return(df)
        else NULL
      } else {
        cat("        No injuries or error for player", purl, "\n", file = log_file, append = TRUE)
        NULL
      }
    })
    team_injuries <- bind_rows(team_injuries_list)
    if (nrow(team_injuries) > 0) {
      team_injuries$season_start_year <- yr
      team_injuries$season_label <- paste0(yr, "/", substr(yr + 1, 3, 4))
      season_injuries <- bind_rows(season_injuries, team_injuries)
    }
    Sys.sleep(2) # Polite delay for server
  }
  cat("Rows found this season:", nrow(season_injuries), "\n", file = log_file, append = TRUE)
  if (nrow(season_injuries) > 0) {
    all_injuries <- bind_rows(all_injuries, season_injuries)
    write.csv(all_injuries, main_save_file, row.names = FALSE)
    cat("Saved injuries for season", yr, "\n", file = log_file, append = TRUE)
  } else {
    cat("No injuries found for season", yr, "\n", file = log_file, append = TRUE)
  }
  Sys.sleep(10) # Longer delay after each season
}

# Always ensure file is saved at the end
if (!file.exists(main_save_file)) {
  write.csv(all_injuries, main_save_file, row.names = FALSE)
  cat("Empty CSV created at", main_save_file, "\n", file = log_file, append = TRUE)
}

cat("Done! Final injury dataset saved to", main_save_file, "\n", file = log_file, append = TRUE)
