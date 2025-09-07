import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_data
def filter_large_csv(
    csv_path,
    season_options=None,
    position_options=None,
    event_period_options=None,
    club_options=None,
    chunk_size=1000
):
    needed_cols = [
        "date", "season_epl", "position_type", "event_period",
        "home_team_clean", "away_team_clean", "matchday",
        "player_name_matchlog", "injury", "games_missed", "injury_days",
        "goals", "assists", "starts", "subs_in", "injury_type",
        "club_missed_games_for", "injured_since", "injured_until",
        "minutes_played"
    ]
    filtered_chunks = []
    for chunk in pd.read_csv(csv_path, parse_dates=["date"], chunksize=chunk_size, usecols=needed_cols):
        mask = pd.Series([True] * len(chunk), index=chunk.index)
        if season_options:
            mask &= chunk["season_epl"].isin(season_options)
        if position_options:
            mask &= chunk["position_type"].isin(position_options)
        if event_period_options:
            mask &= chunk["event_period"].isin(event_period_options)
        if club_options:
            mask &= (chunk["home_team_clean"].isin(club_options)) | (chunk["away_team_clean"].isin(club_options))
        filtered_chunks.append(chunk[mask])
    if filtered_chunks:
        return pd.concat(filtered_chunks, ignore_index=True)
    else:
        return pd.DataFrame()

@st.cache_data
def load_data():
    df = pd.read_csv("feature_engineered_dataset.csv", parse_dates=["date"])
    return df

df = load_data()

def filter_in_memory(
    df,
    season_options=None,
    position_options=None,
    event_period_options=None,
    club_options=None
):
    df_filtered = df.copy()
    if season_options:
        df_filtered = df_filtered[df_filtered["season_epl"].isin(season_options)]
    if position_options:
        df_filtered = df_filtered[df_filtered["position_type"].isin(position_options)]
    if event_period_options:
        df_filtered = df_filtered[df_filtered["event_period"].isin(event_period_options)]
    if club_options:
        df_filtered = df_filtered[
            (df_filtered["home_team_clean"].isin(club_options)) |
            (df_filtered["away_team_clean"].isin(club_options))
        ]
    return df_filtered

# --- Club Name Mapping ---
CLUB_NAME_MAP = {
    "Arsenal": "Arsenal FC",
    "Aston Villa": "Aston Villa",
    "Bournemouth": "AFC Bournemouth",
    "Brentford": "Brentford FC",
    "Brighton": "Brighton & Hove Albion",
    "Burnley": "Burnley FC",
    "Cardiff": "Cardiff City",
    "Chelsea": "Chelsea FC",
    "Crystal Palace": "Crystal Palace",
    "Everton": "Everton FC",
    "Forest": "Nottingham Forest",
    "Fulham": "Fulham FC",
    "Huddersfield": "Huddersfield Town",
    "Hull City": "Hull City",
    "Leeds": "Leeds United",
    "Leicester": "Leicester City",
    "Liverpool": "Liverpool FC",
    "Luton": "Luton Town",
    "Man City": "Manchester City",
    "Man Utd": "Manchester United",
    "Middlesbrough": "Middlesbrough FC",
    "Newcastle": "Newcastle United",
    "Norwich": "Norwich City",
    "Sheff Utd": "Sheffield United",
    "Southampton": "Southampton FC",
    "Stoke City": "Stoke City",
    "Sunderland": "Sunderland AFC",
    "Swansea": "Swansea City",
    "Tottenham": "Tottenham Hotspur",
    "Watford": "Watford FC",
    "West Brom": "West Bromwich Albion",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers"
}

st.set_page_config(page_title="EPL Player Injury & Performance Dashboard", layout="wide")
st.title("⚽ EPL Player Injury & Performance Dashboard")

tab1, tab2 = st.tabs(["📊 Global Dashboard", "🏟️ Club & Player Explorer"])

with tab1:
    st.header("🌍 Global Overview (League Level)")

    # --- KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Matches", df["matchday"].nunique())
    with col2:
        st.metric("Distinct Players", df["player_name_matchlog"].nunique())
    with col3:
        pct_injured = 100 * df[df["injury"] == 1]["player_name_matchlog"].nunique() / df["player_name_matchlog"].nunique()
        st.metric("% Players Injured", f"{pct_injured:.1f}%")
    with col4:
        st.metric("Avg Games Missed per Player", f"{df['games_missed'].mean():.2f}")

    # --- Global Chart Selector ---
    global_chart_options = [
        "Injuries per Season",
        "Injuries by Position",
        "Injury Duration Categories",
        "Minutes Played Pre vs Post Injury",
        "Goals/Assists Pre vs Post Injury (per 90)",
        "Workload & Starts Distribution (injured vs non-injured)",
        "Events by Period",
        "Recovery Curve"
    ]
    selected_global_chart = st.selectbox("Select a global chart to display", global_chart_options)

    if selected_global_chart == "Injuries per Season":
        st.markdown("### 📈 Injuries per Season")
        injuries_per_season = df[df["injury"] == 1].groupby("season_epl")["injury"].count().reset_index()
        fig_season = px.line(injuries_per_season, x="season_epl", y="injury", markers=True, title="Total Injuries Each Season")
        st.plotly_chart(fig_season, use_container_width=True)

    elif selected_global_chart == "Injuries by Position":
        st.markdown("### 🟦 Injuries by Position")
        injuries_by_pos = df[df["injury"] == 1].groupby("position_type")["injury"].count().reset_index()
        fig_pos = px.bar(injuries_by_pos, x="position_type", y="injury", title="Injuries by Position")
        st.plotly_chart(fig_pos, use_container_width=True)

    elif selected_global_chart == "Injury Duration Categories":
        st.markdown("### 📊 Injury Duration Categories")
        bins = [0, 14, 30, df["injury_days"].max()]
        labels = ["Short (<15d)", "Medium (15–30d)", "Long (>30d)"]
        df["injury_duration_cat"] = pd.cut(df["injury_days"], bins=bins, labels=labels, include_lowest=True)
        duration_counts = df[df["injury"] == 1]["injury_duration_cat"].value_counts().reindex(labels)
        fig_duration = px.bar(x=labels, y=duration_counts.values, labels={"x": "Duration", "y": "Count"}, title="Injury Duration Categories")
        st.plotly_chart(fig_duration, use_container_width=True)

    elif selected_global_chart == "Minutes Played Pre vs Post Injury":
        st.markdown("### ⚽ Minutes Played Pre vs Post Injury")
        perf_df = df[df["event_period"].isin(["pre", "post"])].copy()
        perf_group = perf_df.groupby("event_period")["minutes_played"].mean().reset_index()
        fig_min = px.bar(perf_group, x="event_period", y="minutes_played", title="Avg Minutes Played Pre vs Post Injury")
        st.plotly_chart(fig_min, use_container_width=True)

    elif selected_global_chart == "Goals/Assists Pre vs Post Injury (per 90)":
        st.markdown("### ⚡ Goals/Assists Pre vs Post Injury (per 90 mins)")
        import numpy as np
        perf_df = df[df["event_period"].isin(["pre", "post"])].copy()
        for col in ["goals", "assists"]:
            perf_df[f"{col}_per90"] = perf_df[col] / (perf_df["minutes_played"].replace(0, np.nan) / 90)
        ga_group = perf_df.groupby("event_period")[["goals_per90", "assists_per90"]].mean().reset_index()
        fig_ga = px.bar(ga_group, x="event_period", y=["goals_per90", "assists_per90"], barmode="group", title="Goals/Assists per 90 Pre vs Post Injury")
        st.plotly_chart(fig_ga, use_container_width=True)

    elif selected_global_chart == "Workload & Starts Distribution (injured vs non-injured)":
        st.markdown("### 🔁 Workload & Starts Distribution")
        df.loc[:, "injured_ever"] = df.groupby("player_name_matchlog")["injury"].transform("max")
        fig_workload = px.box(df, x="injured_ever", y="minutes_played", points="all", title="Minutes Played: Injured vs Non-Injured")
        st.plotly_chart(fig_workload, use_container_width=True)

    elif selected_global_chart == "Events by Period":
        st.markdown("### Events by Period")
        events_group = df.groupby("event_period")[["starts", "subs_in", "minutes_played"]].mean().reset_index()
        fig_events = px.bar(events_group, x="event_period", y=["starts", "subs_in", "minutes_played"], barmode="group", title="Events by Period")
        st.plotly_chart(fig_events, use_container_width=True)

    elif selected_global_chart == "Recovery Curve":
        st.markdown("### 📈 Recovery Curve")
        recovery_df = df[df["event_period"].isin(["pre", "post"])]
        recovery_curve = recovery_df.groupby(["event_period", "matchday"])["minutes_played"].mean().reset_index()
        fig_recovery = px.line(recovery_curve, x="matchday", y="minutes_played", color="event_period", title="Avg Minutes per Game: Pre vs Post Injury")
        st.plotly_chart(fig_recovery, use_container_width=True)

    st.header("⚡ Impact on Performance")

    # --- Minutes Played Pre vs Post Injury ---
    st.markdown("### ⚽ Minutes Played Pre vs Post Injury")
    perf_df = df[df["event_period"].isin(["pre", "post"])]
    perf_group = perf_df.groupby("event_period")["minutes_played"].mean().reset_index()
    fig_min = px.bar(perf_group, x="event_period", y="minutes_played", title="Avg Minutes Played Pre vs Post Injury")
    st.plotly_chart(fig_min, use_container_width=True)

    # --- Goals/Assists Pre vs Post Injury (per 90) ---
    st.markdown("### ⚡ Goals/Assists Pre vs Post Injury (per 90 mins)")
    for col in ["goals", "assists"]:
        perf_df[f"{col}_per90"] = perf_df[col] / (perf_df["minutes_played"] / 90)
    ga_group = perf_df.groupby("event_period")[["goals_per90", "assists_per90"]].mean().reset_index()
    fig_ga = px.bar(ga_group, x="event_period", y=["goals_per90", "assists_per90"], barmode="group", title="Goals/Assists per 90 Pre vs Post Injury")
    st.plotly_chart(fig_ga, use_container_width=True)

    # --- Workload & Starts Distribution (injured vs non-injured) ---
    st.markdown("### 🔁 Workload & Starts Distribution")
    df["injured_ever"] = df.groupby("player_name_matchlog")["injury"].transform("max")
    fig_workload = px.box(df, x="injured_ever", y="minutes_played", points="all", title="Minutes Played: Injured vs Non-Injured")
    st.plotly_chart(fig_workload, use_container_width=True)

    st.header("📊 Injury Period Analysis")

    # --- Events by period ---
    st.markdown("### Events by Period")
    events_group = df.groupby("event_period")[["starts", "subs_in", "minutes_played"]].mean().reset_index()
    fig_events = px.bar(events_group, x="event_period", y=["starts", "subs_in", "minutes_played"], barmode="group", title="Events by Period")
    st.plotly_chart(fig_events, use_container_width=True)

    # --- Recovery curve: avg minutes per game after return vs before injury ---
    st.markdown("### 📈 Recovery Curve")
    recovery_df = df[df["event_period"].isin(["pre", "post"])]
    recovery_curve = recovery_df.groupby(["event_period", "matchday"])["minutes_played"].mean().reset_index()
    fig_recovery = px.line(recovery_curve, x="matchday", y="minutes_played", color="event_period", title="Avg Minutes per Game: Pre vs Post Injury")
    st.plotly_chart(fig_recovery, use_container_width=True)

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Data")
    season_options = sorted(df["season_epl"].dropna().unique())
    selected_season = st.sidebar.multiselect("Select Season", season_options, default=season_options)
    position_options = sorted(df["position_type"].dropna().unique())
    selected_positions = st.sidebar.multiselect("Select Positions", position_options, default=position_options)
    event_period_options = ["pre", "during", "post"]
    selected_event_periods = st.sidebar.multiselect("Select Event Period", event_period_options, default=event_period_options)
    if "home_team_clean" in df.columns:
        club_options = sorted(pd.concat([df["home_team_clean"], df["away_team_clean"]]).dropna().unique())
        selected_clubs = st.sidebar.multiselect("Select Clubs", club_options, default=club_options)
    else:
        selected_clubs = []

    # --- Add a Run Filter button ---
    if "df_filtered" not in st.session_state:
        st.session_state.df_filtered = pd.DataFrame()

    if st.sidebar.button("Run Filter"):
        with st.spinner("Filtering data, please wait..."):
            st.session_state.df_filtered = filter_large_csv(
                "feature_engineered_dataset.csv",
                season_options=selected_season,
                position_options=selected_positions,
                event_period_options=selected_event_periods,
                club_options=selected_clubs
            )

    df_filtered = st.session_state.df_filtered

    if df_filtered.empty:
        st.warning("⚠️ No data matches your selected filters. Click 'Run Filter' to apply filters.")
        st.stop()

    # --- KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Matches", df_filtered["matchday"].nunique() if "matchday" in df_filtered.columns else "N/A")
    with col2:
        st.metric("Distinct Players", df_filtered["player_name_matchlog"].nunique() if "player_name_matchlog" in df_filtered.columns else "N/A")
    with col3:
        st.metric("Avg Minutes Played", f"{df_filtered['minutes_played'].mean():.2f}" if "minutes_played" in df_filtered.columns else "N/A")
    with col4:
        st.metric("Avg Goals per Player", f"{df_filtered['goals'].mean():.2f}" if "goals" in df_filtered.columns else "N/A")

    # --- Chart Selector ---
    chart_options = [
        "Starts Distribution",
        "Minutes Played Distribution",
        "Goals Distribution",
        "Assists Distribution",
        "Events by Injury Period",
        "Injury Duration (Days)",
        "Games Missed due to Injury"
    ]
    selected_chart = st.selectbox("Select a chart to display", chart_options)

    if selected_chart == "Starts Distribution" and "starts" in df_filtered.columns:
        st.markdown("### 🎯 Distribution of Starts")
        fig1 = px.histogram(df_filtered, x="starts", nbins=30, title="Histogram: Starts")
        st.plotly_chart(fig1, use_container_width=True)

    elif selected_chart == "Minutes Played Distribution" and "minutes_played" in df_filtered.columns:
        st.markdown("### ⏱️ Distribution of Minutes Played")
        sample_df = df_filtered.sample(n=min(1000, len(df_filtered)), random_state=42)
        fig2 = px.histogram(sample_df, x="minutes_played", nbins=30, title="Histogram: Minutes Played")
        st.plotly_chart(fig2, use_container_width=True)

    elif selected_chart == "Goals Distribution" and "goals" in df_filtered.columns:
        st.markdown("### ⚽ Goals Distribution")
        fig3 = px.histogram(df_filtered, x="goals", nbins=20, title="Histogram: Goals")
        st.plotly_chart(fig3, use_container_width=True)

    elif selected_chart == "Assists Distribution" and "assists" in df_filtered.columns:
        st.markdown("### 🎯 Assists Distribution")
        fig4 = px.histogram(df_filtered, x="assists", nbins=20, title="Histogram: Assists")
        st.plotly_chart(fig4, use_container_width=True)

    elif selected_chart == "Events by Injury Period" and "event_period" in df_filtered.columns:
        st.markdown("### ⚠️ Events by Injury Period")
        fig5 = px.histogram(df_filtered, x="event_period", color="event_period", title="Events Count by Period")
        st.plotly_chart(fig5, use_container_width=True)

    elif selected_chart == "Injury Duration (Days)" and "injury_days" in df_filtered.columns:
        st.markdown("### 🩺 Injury Duration (Days)")
        fig6 = px.histogram(df_filtered, x="injury_days", nbins=30, title="Distribution of Injury Days")
        st.plotly_chart(fig6, use_container_width=True)

    elif selected_chart == "Games Missed due to Injury" and "games_missed" in df_filtered.columns:
        st.markdown("### 🚑 Games Missed due to Injury")
        fig7 = px.histogram(df_filtered, x="games_missed", nbins=30, title="Games Missed Distribution")
        st.plotly_chart(fig7, use_container_width=True)

    # --- Player Lookup ---
    st.markdown("### 🔎 Individual Player Explorer")
    player_col = "player_name_matchlog"
    if player_col in df_filtered.columns and not df_filtered[player_col].dropna().empty:
        selected_player = st.selectbox("Choose a player", sorted(df_filtered[player_col].dropna().unique()))
        player_df = df_filtered[df_filtered[player_col] == selected_player]

        if not player_df.empty:
            st.write(f"#### Player: **{selected_player}**")
            feature_cols = ["minutes_played", "goals", "assists", "starts", "injured_summary", "absence", "injury_days", "games_missed", "injury_burden_days"]
            summary_cols = [col for col in feature_cols if col in player_df.columns]
            st.dataframe(player_df[summary_cols].drop_duplicates())

            if "event_period" in player_df.columns:
                st.write(player_df[["date", "event_period", "minutes_played", "goals", "assists"]].sort_values("date"))
    else:
        st.info("No player data available for the current filter selection.")

    st.write("Missing club_missed_games_for:", df["club_missed_games_for"].isna().sum())

    st.write("Selector club options:", club_options)
    st.write("Unique clubs in club_missed_games_for:", df["club_missed_games_for"].dropna().unique())

    # --- Impact on Performance
    st.header("⚡ Impact on Performance")

    # Minutes Played Pre vs Post Injury
    st.markdown("### ⚽ Minutes Played Pre vs Post Injury")
    perf_df = df[df["event_period"].isin(["pre", "post"])]
    perf_group = perf_df.groupby("event_period")["minutes_played"].mean().reset_index()
    fig_min = px.bar(perf_group, x="event_period", y="minutes_played", title="Avg Minutes Played Pre vs Post Injury")
    st.plotly_chart(fig_min, use_container_width=True)

    # Goals/Assists Pre vs Post Injury (per 90)
    st.markdown("### ⚡ Goals/Assists Pre vs Post Injury (per 90 mins)")
    for col in ["goals", "assists"]:
        perf_df[f"{col}_per90"] = perf_df[col] / (perf_df["minutes_played"] / 90)
    ga_group = perf_df.groupby("event_period")[["goals_per90", "assists_per90"]].mean().reset_index()
    fig_ga = px.bar(ga_group, x="event_period", y=["goals_per90", "assists_per90"], barmode="group", title="Goals/Assists per 90 Pre vs Post Injury")
    st.plotly_chart(fig_ga, use_container_width=True)

    # Workload & Starts Distribution (injured vs non-injured)
    st.markdown("### 🔁 Workload & Starts Distribution")
    df["injured_ever"] = df.groupby("player_name_matchlog")["injury"].transform("max")
    fig_workload = px.box(df, x="injured_ever", y="minutes_played", points="all", title="Minutes Played: Injured vs Non-Injured")
    st.plotly_chart(fig_workload, use_container_width=True)

with tab2:
    st.header("🏟️ Club & Player Explorer")

    # --- Club Selector ---
    club_options = sorted(pd.concat([df["home_team_clean"], df["away_team_clean"]]).dropna().unique())
    selected_club = st.selectbox("Select a Club", club_options)
    season_options = sorted(df["season_epl"].dropna().unique())
    selected_season = st.selectbox("Select a Season", season_options)

    # --- Subset for club & season ---
    club_name_for_data = CLUB_NAME_MAP.get(selected_club, selected_club)
    club_df = df[
        (df["season_epl"] == selected_season) &
        (df["club_missed_games_for"] == club_name_for_data)
    ]

    # --- Injured Players ---
    injured_players = club_df[club_df["injury"] == 1]["player_name_matchlog"].unique()

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("🧍 Players Injured", len(injured_players))
    with kpi2:
        st.metric("🚑 Total Injuries", int(club_df["injury"].sum()))
    with kpi3:
        st.metric("⏱️ Avg Recovery (days)", f"{club_df['injury_days'].mean():.1f}" if "injury_days" in club_df.columns else "N/A")
    with kpi4:
        st.metric("⚽ Games Missed", int(club_df["games_missed"].sum()) if "games_missed" in club_df.columns else "N/A")

    # --- Injured Players List ---
    st.markdown("### 🚑 Injured Players")
    st.dataframe(
        club_df[club_df["injury"] == 1][
            ["player_name_matchlog", "injured_since", "injured_until", "injury_days", "games_missed", "injury"]
        ].drop_duplicates().sort_values("injured_since")
    )

    # --- Injury Timeline ---
    if "date" in club_df.columns and "injury" in club_df.columns:
        st.markdown("### 📅 Injury Timeline")
        fig_timeline = px.histogram(
            club_df[club_df["injury"] == 1], x="date",
            title=f"Injuries Over Time - {selected_club} ({selected_season})"
        )
        st.plotly_chart(fig_timeline, use_container_width=True)

    # --- (Optional) Pie Chart: Injury Type Distribution ---
    if "injury_type" in club_df.columns and club_df["injury_type"].notna().any():
        st.markdown("### 🥧 Injury Type Distribution")
        fig_pie = px.pie(
            club_df[club_df["injury"] == 1 & club_df["injury_type"].notna()],
            names="injury_type",
            title="Injury Type Distribution"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- Player Deep Dive ---
    if len(injured_players) > 0:
        selected_player = st.selectbox("Choose a player for deep dive", injured_players)
        player_df = club_df[club_df["player_name_matchlog"] == selected_player]

        st.markdown(f"#### Player: {selected_player}")
        st.dataframe(
            player_df[
                ["date", "event_period", "minutes_played", "goals", "assists", "injury_days", "injured_since", "injured_until"]
            ].sort_values("date")
        )

        # Timeline plot: minutes played over time
        if "date" in player_df.columns:
            st.markdown("##### Minutes Played Over Time")
            fig_minutes_timeline = px.line(
                player_df, x="date", y="minutes_played",
                title="Minutes Played Over Time"
            )
            st.plotly_chart(fig_minutes_timeline, use_container_width=True)

    # --- Test: Show injuries for the selected club (any season) ---
    test_df = df[df["club_missed_games_for"] == selected_club]
    st.write("Injuries for selected club (any season):", test_df[test_df["injury"] == 1])

    st.dataframe(df_filtered.head(100))  # Show only first 100 rows

st.write(st.session_state)
