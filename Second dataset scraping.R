library(rvest)
library(dplyr)
library(stringr)
library(purrr)
library(lubridate)
library(httr)

# --- Logging ---
log_message <- function(msg) {
  cat(sprintf("[%s] %s\n", Sys.time(), msg), file = "scrape_log.txt", append = TRUE)
}

# --- Core settings ---
seasons <- 2015:2024
epl_comp_code <- "GB1"

# Get all EPL clubs for a given season
get_epl_club_urls <- function(season) {
  base_url <- paste0("https://www.transfermarkt.com/premier-league/startseite/wettbewerb/", epl_comp_code, "/plus/?saison_id=", season)
  log_message(paste("Reading EPL clubs from:", base_url))
  Sys.sleep(10)
  page <- tryCatch(read_html(GET(base_url, add_headers(
    "user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ))), error = function(e) {log_message(paste("Error reading clubs for season", season, ":", e$message)); return(NULL)})
  if (is.null(page)) return(data.frame())
  club_nodes <- html_nodes(page, xpath = "//td[@class='hauptlink no-border-links']/a")
  club_urls <- html_attr(club_nodes, "href")
  club_names <- html_text(club_nodes)
  club_ids <- as.character(str_extract(club_urls, "/verein/([0-9]+)") %>% str_remove_all("/verein/"))
  data.frame(
    club_name = club_names,
    club_url = paste0("https://www.transfermarkt.com", club_urls),
    club_id = club_ids,
    stringsAsFactors = FALSE
  )
}

# Get all player URLs from a club's squad page in a season
get_club_player_urls <- function(club_url, season) {
  squad_url <- paste0(club_url, "/saison_id/", season)
  log_message(paste("Reading players from squad page:", squad_url))
  Sys.sleep(10)
  page <- tryCatch(read_html(GET(squad_url, add_headers(
    "user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ))), error = function(e) {log_message(paste("Error reading squad page for", squad_url, ":", e$message)); return(NULL)})
  if (is.null(page)) return(data.frame())
  player_nodes <- html_nodes(page, xpath = "//td[@class='hauptlink']/a[contains(@href, '/profil/spieler/')]")
  if(length(player_nodes) == 0) {
    log_message(paste("No players found at", squad_url))
    return(data.frame())
  }
  player_urls <- unique(html_attr(player_nodes, "href"))
  player_names <- unique(html_text(player_nodes))
  player_profile_urls <- paste0("https://www.transfermarkt.com", player_urls)
  player_name_slugs <- sapply(strsplit(player_urls, "/"), function(x) x[2])
  player_ids <- str_match(player_urls, "spieler/([0-9]+)")[,2]
  # Defensive: if vectors are not the same length, skip
  n <- min(length(player_names), length(player_profile_urls), length(player_name_slugs), length(player_ids))
  if(n == 0) {
    log_message(sprintf("Problem extracting players: names=%d, urls=%d, slugs=%d, ids=%d", 
                        length(player_names), length(player_profile_urls), length(player_name_slugs), length(player_ids)))
    return(data.frame())
  }
  data.frame(
    player_name = player_names[1:n],
    player_profile_url = player_profile_urls[1:n],
    player_name_slug = player_name_slugs[1:n],
    player_id = player_ids[1:n],
    stringsAsFactors = FALSE
  )
}

# Scrape player's EPL match log for a season (raw table version)
scrape_player_matchlog <- function(player_name, player_name_slug, player_id, season, club_id) {
  if (is.na(player_id) || is.na(player_name_slug)) {
    log_message(paste("Skipping due to missing player_id or player_name_slug for", player_name))
    return(NULL)
  }
  matchlog_url <- paste0(
    "https://www.transfermarkt.com/", player_name_slug,
    "/leistungsdatendetails/spieler/", player_id,
    "/plus/1?saison=", season,
    "&verein=", club_id,
    "&liga=&wettbewerb=GB1"
  )
  log_message(paste("Scraping:", player_name, "-", matchlog_url))
  Sys.sleep(15)
  resp <- GET(matchlog_url, add_headers(
    "user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ))
  if (status_code(resp) == 404) {
    log_message(paste("404 not found:", matchlog_url))
    return(NULL)
  }
  page <- tryCatch(read_html(resp), error = function(e) {log_message(paste("Error scraping", matchlog_url, ":", e$message)); return(NULL)})
  if (is.null(page)) return(NULL)
  tables <- html_table(page, fill = TRUE)
  log <- NULL
  for(t in tables) {
    if(any(grepl("date", tolower(names(t)))) || any(grepl("datum", tolower(names(t))))) {
      log <- t
      break
    }
  }
  if(is.null(log) || nrow(log) == 0) {
    log_message(paste("No match log found for", player_name, "in season", season))
    return(NULL)
  }
  names(log) <- make.unique(tolower(make.names(names(log))))
  log_message(paste("Columns for", player_name, ":", paste(names(log), collapse=", ")))
  # Add player and context info to every row
  log$player_name <- player_name
  log$player_url <- matchlog_url
  log$season <- season
  log$club_id <- club_id
  log
}

# Fallback for missing columns
`%||%` <- function(a, b) if(!is.null(a) && !all(is.na(a))) a else b

# --- MAIN LOOP ---
all_logs <- list()
for(season in seasons) {
  log_message(paste("Processing season:", season))
  clubs <- get_epl_club_urls(season)
  if (nrow(clubs) == 0) next
  for(i in seq_len(nrow(clubs))) {
    club <- clubs[i,]
    log_message(paste("Processing club:", club$club_name, "(", club$club_id, ")"))
    players <- get_club_player_urls(club$club_url, season)
    if (nrow(players) == 0) next
    for(j in seq_len(nrow(players))) {
      player <- players[j,]
      res <- tryCatch(
        scrape_player_matchlog(player$player_name, player$player_name_slug, player$player_id, season, club$club_id),
        error = function(e) {log_message(paste("Error in scrape_player_matchlog for", player$player_name, ":", e$message)); NULL}
      )
      if(!is.null(res) && nrow(res) > 0) {
        all_logs[[length(all_logs)+1]] <- res
      }
    }
  }
}

final_df <- bind_rows(all_logs)
write.csv(final_df, "epl_matchlogs_transfermarkt_2015_2025_raw.csv", row.names = FALSE)
log_message("All seasons processed and combined CSV written.")