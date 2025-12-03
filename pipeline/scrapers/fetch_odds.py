import requests
import csv
from datetime import datetime, timezone
import os

# API URL for NBA
SPORTS_APIS = {
    "nba": "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?limit=250",
}


def fetch_odds(sport="nba", debug=False, days_ahead=0):
    """
    Fetch odds for NBA games.

    Args:
        sport: Sport to fetch (defaults to 'nba')
        debug: If True, print debug information
        days_ahead: Number of days ahead to fetch (0=today, 1=tomorrow, etc.)

    Returns:
        List of games with odds data
    """
    if sport not in SPORTS_APIS:
        raise ValueError(f"Sport must be one of {list(SPORTS_APIS.keys())}")

    api_url = SPORTS_APIS[sport]
    response = requests.get(api_url)
    data = response.json()

    games = []
    skipped_no_date = 0
    skipped_wrong_date = 0
    skipped_no_odds = 0
    errors = 0

    # Get target date (today + days_ahead)
    from datetime import timedelta
    target_date = (datetime.now() + timedelta(days=days_ahead)).date()

    if debug:
        print(f"  Total events from API: {len(data.get('items', []))}")
        print(f"  Target date: {target_date}")

        # Debug: Show dates of all events
        print(f"  Checking event dates...")
        event_dates = []
        for i, event in enumerate(data.get("items", [])[:5]):  # Check first 5
            try:
                event_json = requests.get(event["$ref"]).json()
                if "date" in event_json:
                    event_date_str = event_json["date"]
                    event_datetime = datetime.fromisoformat(
                        event_date_str.replace('Z', '+00:00'))
                    event_date = event_datetime.date()
                    event_dates.append(event_date)
                    print(
                        f"    Event {i+1}: {event_date} at {event_datetime.strftime('%I:%M %p')}")
            except:
                pass
        if event_dates:
            print(f"  Date range: {min(event_dates)} to {max(event_dates)}")

    for event in data.get("items", []):
        try:
            # Event JSON
            event_json = requests.get(event["$ref"]).json()

            if "competitions" not in event_json:
                continue

            # Check if game is today
            if "date" in event_json:
                # Parse the event date (ISO format - comes in UTC)
                event_date_str = event_json["date"]
                event_datetime_utc = datetime.fromisoformat(
                    event_date_str.replace('Z', '+00:00'))

                # Convert to local time for correct date
                event_datetime_local = event_datetime_utc.astimezone()
                event_date = event_datetime_local.date()

                # Skip if not target date
                if event_date != target_date:
                    skipped_wrong_date += 1
                    if debug and skipped_wrong_date <= 2:
                        local_time = event_datetime_local.strftime('%I:%M %p')
                        print(
                            f"  Skipping game on {event_date} at {local_time} (target: {target_date})")
                    continue
            else:
                skipped_no_date += 1
                continue

            comp_json = requests.get(
                event_json["competitions"][0]["$ref"]).json()

            teams = comp_json["competitors"]

            # Determine which team is away and which is home
            # competitors array is not always in away/home order
            away_team = None
            home_team = None

            for team in teams:
                if team.get("homeAway") == "away":
                    away_team = team
                elif team.get("homeAway") == "home":
                    home_team = team

            # Fallback if homeAway not specified
            if not away_team or not home_team:
                away_team = teams[0]
                home_team = teams[1]

            # Fetch team details from their $ref URLs
            team1_json = requests.get(away_team["team"]["$ref"]).json()
            team2_json = requests.get(home_team["team"]["$ref"]).json()

            team1 = team1_json.get("displayName", "Unknown")  # Away team
            team2 = team2_json.get("displayName", "Unknown")  # Home team

            odds_ref = comp_json.get("odds")

            if not odds_ref or "$ref" not in odds_ref:
                skipped_no_odds += 1
                if debug:
                    print(f"  No odds reference for {team1} vs {team2}")
                continue

            # Fetch the odds data from the $ref URL
            odds_json = requests.get(odds_ref["$ref"]).json()

            # The odds_json should have an "items" array with different providers
            odds_items = odds_json.get("items", [])
            if len(odds_items) == 0:
                skipped_no_odds += 1
                if debug:
                    print(f"  No odds items for {team1} vs {team2}")
                continue

            # Try to find a provider with complete odds (spread, moneyline, total)
            # Some providers only offer certain bet types
            best_odds = None

            for odds_item in odds_items:
                spread = odds_item.get("spread")
                moneyline1 = odds_item.get("awayTeamOdds", {}).get("moneyLine")
                moneyline2 = odds_item.get("homeTeamOdds", {}).get("moneyLine")
                total = odds_item.get("overUnder")

                # Check if this provider has more complete data
                if best_odds is None:
                    best_odds = odds_item
                else:
                    # Count how many fields this provider has
                    current_count = sum([
                        spread is not None,
                        moneyline1 is not None,
                        moneyline2 is not None,
                        total is not None
                    ])
                    best_count = sum([
                        best_odds.get("spread") is not None,
                        best_odds.get("awayTeamOdds", {}).get(
                            "moneyLine") is not None,
                        best_odds.get("homeTeamOdds", {}).get(
                            "moneyLine") is not None,
                        best_odds.get("overUnder") is not None
                    ])

                    if current_count > best_count:
                        best_odds = odds_item

            o = best_odds

            if debug:
                print(f"  Found odds for {team1} (away) vs {team2} (home)")
                print(
                    f"    Provider: {o.get('provider', {}).get('name', 'Unknown')}")
                print(f"    Spread: {o.get('spread')}")
                away_ml = o.get('awayTeamOdds', {}).get('moneyLine')
                home_ml = o.get('homeTeamOdds', {}).get('moneyLine')
                print(
                    f"    Away ML ({team1}): {away_ml}, Home ML ({team2}): {home_ml}, Total: {o.get('overUnder')}")
                print(f"    Checked {len(odds_items)} providers")

            entry = {
                "sport": sport.upper(),
                "away_team": team1,  # team1 is away
                "home_team": team2,  # team2 is home
                "spread": o.get("spread"),
                "away_moneyline": o.get("awayTeamOdds", {}).get("moneyLine"),
                "home_moneyline": o.get("homeTeamOdds", {}).get("moneyLine"),
                "total": o.get("overUnder"),
            }

            games.append(entry)
        except Exception as e:
            # Skip games that have errors (e.g., missing data)
            errors += 1
            if debug:
                print(f"  Error processing game: {e}")
            continue

    if debug:
        print(f"\n  Summary:")
        print(f"    Games found: {len(games)}")
        print(f"    Skipped (wrong date): {skipped_wrong_date}")
        print(f"    Skipped (no odds): {skipped_no_odds}")
        print(f"    Errors: {errors}")

    return games


def save_to_csv(games, sport="nba"):
    """
    Save games data to CSV file.

    Args:
        games: List of game dictionaries for a specific sport
        sport: Sport name to include in filename (defaults to 'nfl')

    Returns:
        Filename of saved CSV
    """
    if not games:
        print(f"No {sport.upper()} games found for today.")
        return

    # Save into parent /data folder (pipeline/data/)
    # Get the parent directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(parent_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    filename = os.path.join(
        data_dir, f"{sport}_odds_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
    keys = ["sport", "away_team", "home_team", "spread",
            "away_moneyline", "home_moneyline", "total"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(games)

    print(f"Saved {len(games)} {sport.upper()} games → {filename}")
    return filename


if __name__ == "__main__":
    import sys
    from datetime import timedelta

    # Check for command line options
    debug_mode = '--debug' in sys.argv or '-d' in sys.argv
    auto_mode = '--auto' in sys.argv or '-a' in sys.argv  # Auto-detect next games

    # Check for days ahead option (e.g., --days=1 for tomorrow)
    days_ahead = 0
    for arg in sys.argv:
        if arg.startswith('--days='):
            days_ahead = int(arg.split('=')[1])

    if auto_mode:
        print("\n⚡ AUTO MODE: Fetching next available NBA games")

        # Try today first, then tomorrow
        games_found = False
        for day_offset in range(3):  # Try today, tomorrow, day after
            try:
                games = fetch_odds("nba", debug=debug_mode, days_ahead=day_offset)
                if games:
                    target_date = (datetime.now() +
                                   timedelta(days=day_offset)).date()
                    print(
                        f"  ✓ Found {len(games)} NBA games on {target_date.strftime('%B %d, %Y')}")
                    save_to_csv(games, "nba")
                    games_found = True
                    break
            except Exception as e:
                if debug_mode:
                    print(f"  Error checking day +{day_offset}: {e}")

        if not games_found:
            print("  No NBA games found in next 3 days")

    else:
        # Manual mode with specific date
        target_date = (datetime.now() + timedelta(days=days_ahead)).date()
        print(
            f"\nFetching NBA odds for games on {target_date.strftime('%B %d, %Y')}")
        if debug_mode:
            print("  (Debug mode enabled)")
        if days_ahead > 0:
            print(f"  (Fetching games {days_ahead} day(s) ahead)")

        print(f"\n{'='*50}")
        print("Fetching NBA odds...")
        print('='*50)

        try:
            games = fetch_odds("nba", debug=debug_mode, days_ahead=days_ahead)
            save_to_csv(games, "nba")
        except Exception as e:
            print(f"Error fetching NBA odds: {e}")

    print("\n" + "="*50)
    print("NBA odds fetched!")
    print("="*50)
