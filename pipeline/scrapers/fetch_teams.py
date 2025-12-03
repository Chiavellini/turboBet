import requests
import pandas as pd
import time
import os
from datetime import datetime
from requests.exceptions import RequestException, Timeout, ConnectionError

# NBA team IDs (all 30 teams)
NBA_TEAM_IDS = {
    'Atlanta Hawks': 1610612737,
    'Boston Celtics': 1610612738,
    'Brooklyn Nets': 1610612751,
    'Charlotte Hornets': 1610612766,
    'Chicago Bulls': 1610612741,
    'Cleveland Cavaliers': 1610612739,
    'Dallas Mavericks': 1610612742,
    'Denver Nuggets': 1610612743,
    'Detroit Pistons': 1610612765,
    'Golden State Warriors': 1610612744,
    'Houston Rockets': 1610612745,
    'Indiana Pacers': 1610612754,
    'LA Clippers': 1610612746,
    'Los Angeles Lakers': 1610612747,
    'Memphis Grizzlies': 1610612763,
    'Miami Heat': 1610612748,
    'Milwaukee Bucks': 1610612749,
    'Minnesota Timberwolves': 1610612750,
    'New Orleans Pelicans': 1610612740,
    'New York Knicks': 1610612752,
    'Oklahoma City Thunder': 1610612760,
    'Orlando Magic': 1610612753,
    'Philadelphia 76ers': 1610612755,
    'Phoenix Suns': 1610612756,
    'Portland Trail Blazers': 1610612757,
    'Sacramento Kings': 1610612758,
    'San Antonio Spurs': 1610612759,
    'Toronto Raptors': 1610612761,
    'Utah Jazz': 1610612762,
    'Washington Wizards': 1610612764
}


def fetch_with_retry(url, headers, params, max_retries=3, timeout=30, retry_delay=2):
    """
    Fetch data from API with retry logic and exponential backoff.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        params: Request parameters
        max_retries: Maximum number of retry attempts (default: 3)
        timeout: Request timeout in seconds (default: 30)
        retry_delay: Initial delay between retries in seconds (default: 2)
    
    Returns:
        Response object if successful, None if all retries fail
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=timeout
            )
            response.raise_for_status()
            return response
        except (Timeout, ConnectionError) as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                print(f"  ⚠ Attempt {attempt + 1} failed: {type(e).__name__}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  ✗ All {max_retries} attempts failed: {type(e).__name__}")
                return None
        except RequestException as e:
            # For other HTTP errors (4xx, 5xx), don't retry
            print(f"  ✗ HTTP error: {e}")
            return None
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            return None
    
    return None


def get_team_stats(team_id, season='2025-26'):
    """
    Fetch team stats from NBA.com API including days rest splits
    """

    # Required headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true'
    }

    team_data = {}

    # 1. Get overall team stats
    url_general = "https://stats.nba.com/stats/teamdashboardbygeneralsplits"
    params_general = {
        'DateFrom': '', 'DateTo': '', 'GameSegment': '', 'LastNGames': 0,
        'LeagueID': '00', 'Location': '', 'MeasureType': 'Base', 'Month': 0,
        'OpponentTeamID': 0, 'Outcome': '', 'PORound': 0, 'PaceAdjust': 'N',
        'PerMode': 'PerGame', 'Period': 0, 'PlusMinus': 'N', 'Rank': 'N',
        'Season': season, 'SeasonSegment': '', 'SeasonType': 'Regular Season',
        'ShotClockRange': '', 'Split': 'general', 'TeamID': team_id,
        'VsConference': '', 'VsDivision': ''
    }

    response = fetch_with_retry(url_general, headers, params_general)
    if response is None:
        print(f"Error fetching general stats for team {team_id}: All retry attempts failed")
        return None
    
    try:
        data = response.json()
        if data['resultSets'] and len(data['resultSets']) > 0:
            headers_list = data['resultSets'][0]['headers']
            rows = data['resultSets'][0]['rowSet']
            if rows:
                team_data = dict(zip(headers_list, rows[0]))
    except Exception as e:
        print(f"Error parsing general stats for team {team_id}: {e}")
        return None

    time.sleep(0.5)  # Small delay between API calls

    # 2. Get opponent stats (defensive stats)
    url_opp = "https://stats.nba.com/stats/teamdashboardbygeneralsplits"
    params_opp = params_general.copy()
    params_opp['MeasureType'] = 'Opponent'  # Get opponent stats (defense)

    response = fetch_with_retry(url_opp, headers, params_opp)
    if response is not None:
        try:
            data = response.json()
            if data['resultSets'] and len(data['resultSets']) > 0:
                headers_list = data['resultSets'][0]['headers']
                rows = data['resultSets'][0]['rowSet']
                if rows:
                    opp_data = dict(zip(headers_list, rows[0]))

                    # Opponent stats already have OPP_ prefix in the API
                    team_data['OPP_PTS'] = opp_data.get('OPP_PTS', 0)
                    team_data['OPP_FG_PCT'] = opp_data.get('OPP_FG_PCT', 0)
                    team_data['OPP_FG3_PCT'] = opp_data.get('OPP_FG3_PCT', 0)
                    team_data['OPP_FT_PCT'] = opp_data.get('OPP_FT_PCT', 0)
                    team_data['OPP_REB'] = opp_data.get('OPP_REB', 0)
                    team_data['OPP_AST'] = opp_data.get('OPP_AST', 0)
                    team_data['OPP_TOV'] = opp_data.get('OPP_TOV', 0)
                    print(
                        f"  ✓ Opponent stats fetched (OPP_PTS: {team_data['OPP_PTS']:.1f})")
                else:
                    print(f"  Warning: No opponent stat rows found")
        except Exception as e:
            print(f"  Error parsing opponent stats for team {team_id}: {e}")
    else:
        print(f"  Warning: Failed to fetch opponent stats for team {team_id}")

    time.sleep(0.5)

    # 3. Get days rest splits by analyzing game log
    url_gamelog = "https://stats.nba.com/stats/teamgamelog"
    params_gamelog = {
        'DateFrom': '',
        'DateTo': '',
        'LeagueID': '00',
        'Season': season,
        'SeasonType': 'Regular Season',
        'TeamID': team_id
    }

    response = fetch_with_retry(url_gamelog, headers, params_gamelog)
    if response is not None:
        try:
            data = response.json()

            if data.get('resultSets') and len(data['resultSets']) > 0:
                headers_list = data['resultSets'][0]['headers']
                game_rows = data['resultSets'][0]['rowSet']

                if game_rows:
                    from datetime import datetime, timedelta

                    # Organize by days rest
                    rest_buckets = {'0': [], '1': [], '2': [], '3': [], '4+': []}
                    prev_date = None

                    # Sort games by date (most recent first in API, so reverse)
                    game_rows_sorted = sorted(
                        game_rows, key=lambda x: x[headers_list.index('GAME_DATE')])

                    for game in game_rows_sorted:
                        game_dict = dict(zip(headers_list, game))
                        game_date_str = game_dict.get('GAME_DATE', '')

                        if game_date_str and prev_date:
                            # Calculate days rest
                            game_date = datetime.strptime(
                                game_date_str, '%b %d, %Y')
                            # -1 because game day doesn't count
                            days_rest = (game_date - prev_date).days - 1

                            # Categorize
                            if days_rest == 0:
                                bucket = '0'
                            elif days_rest == 1:
                                bucket = '1'
                            elif days_rest == 2:
                                bucket = '2'
                            elif days_rest == 3:
                                bucket = '3'
                            else:  # 4+
                                bucket = '4+'

                            rest_buckets[bucket].append(game_dict)

                        if game_date_str:
                            prev_date = datetime.strptime(
                                game_date_str, '%b %d, %Y')

                    # Calculate stats for each rest bucket
                    for rest_days, games in rest_buckets.items():
                        if games:
                            rest_label = rest_days.replace('+', 'plus')
                            prefix = f"REST_{rest_label}_"

                            gp = len(games)
                            wins = sum(1 for g in games if g.get('WL') == 'W')
                            losses = gp - wins
                            w_pct = wins / gp if gp > 0 else 0
                            avg_pts = sum(g.get('PTS', 0)
                                          for g in games) / gp if gp > 0 else 0
                            avg_plus_minus = sum(g.get('PLUS_MINUS', 0)
                                                 for g in games) / gp if gp > 0 else 0

                            team_data[f"{prefix}GP"] = gp
                            team_data[f"{prefix}W"] = wins
                            team_data[f"{prefix}L"] = losses
                            team_data[f"{prefix}W_PCT"] = w_pct
                            team_data[f"{prefix}PTS"] = avg_pts
                            team_data[f"{prefix}PLUS_MINUS"] = avg_plus_minus

                            print(
                                f"  ✓ Rest {rest_days} days: {gp} GP, {w_pct:.3f} W%, {avg_pts:.1f} PTS")
                        else:
                            # No games in this bucket
                            rest_label = rest_days.replace('+', 'plus')
                            prefix = f"REST_{rest_label}_"
                            team_data[f"{prefix}GP"] = 0
                            team_data[f"{prefix}W"] = 0
                            team_data[f"{prefix}L"] = 0
                            team_data[f"{prefix}W_PCT"] = 0
                            team_data[f"{prefix}PTS"] = 0
                            team_data[f"{prefix}PLUS_MINUS"] = 0

                    print(f"  ✓ Days rest analysis complete")
                else:
                    print(f"  Warning: No game log data found")
        except Exception as e:
            print(f"  Error analyzing days rest: {str(e)[:60]}")
    else:
        print(f"  Warning: Failed to fetch game log for team {team_id}")

    return team_data if team_data else None


def scrape_all_teams(season='2025-26', output_file='nba_team_stats.csv'):
    """
    Scrape stats for all NBA teams and save to CSV
    Only keeps columns relevant for predicting points and win/loss
    """
    all_stats = []

    print(f"Scraping NBA team stats for {season} season...")
    print("-" * 50)

    for team_name, team_id in NBA_TEAM_IDS.items():
        print(f"Fetching data for {team_name}...")

        stats = get_team_stats(team_id, season)

        if stats:
            stats['TEAM_NAME'] = team_name
            stats['TEAM_ID'] = team_id
            all_stats.append(stats)
            print(f"✓ {team_name} - Complete")
        else:
            print(f"✗ {team_name} - Failed")

        # Be respectful to NBA.com servers - increased delay to reduce rate limiting
        time.sleep(1.0)

    # Convert to DataFrame
    df = pd.DataFrame(all_stats)

    # Define relevant columns for predicting points scored and win/loss
    # Keep only stats that actually impact scoring and winning
    relevant_columns = [
        'TEAM_NAME', 'TEAM_ID',

        # Overall record and performance
        'GP', 'W', 'L', 'W_PCT', 'MIN',

        # Offensive stats (points scoring)
        'PTS',           # Points per game
        'FGM', 'FGA', 'FG_PCT',      # Field goals
        'FG3M', 'FG3A', 'FG3_PCT',   # Three pointers
        'FTM', 'FTA', 'FT_PCT',      # Free throws

        # Efficiency and pace
        'OREB', 'DREB', 'REB',       # Rebounding (second chances)
        'AST',                        # Ball movement
        'TOV',                        # Turnovers (negative)
        'STL', 'BLK',                # Defense
        'PLUS_MINUS',                 # Overall efficiency

        # Opponent stats (defensive performance)
        'OPP_PTS',           # Points allowed per game
        'OPP_FG_PCT',        # Opponent field goal percentage
        'OPP_FG3_PCT',       # Opponent 3-point percentage
        'OPP_FT_PCT',        # Opponent free throw percentage
        'OPP_REB',           # Opponent rebounds
        'OPP_AST',           # Opponent assists
        'OPP_TOV',           # Opponent turnovers (good for us)

        # Days rest splits (performance by rest days)
        'REST_0_GP', 'REST_0_W', 'REST_0_L', 'REST_0_W_PCT', 'REST_0_PTS', 'REST_0_PLUS_MINUS',
        'REST_1_GP', 'REST_1_W', 'REST_1_L', 'REST_1_W_PCT', 'REST_1_PTS', 'REST_1_PLUS_MINUS',
        'REST_2_GP', 'REST_2_W', 'REST_2_L', 'REST_2_W_PCT', 'REST_2_PTS', 'REST_2_PLUS_MINUS',
        'REST_3_GP', 'REST_3_W', 'REST_3_L', 'REST_3_W_PCT', 'REST_3_PTS', 'REST_3_PLUS_MINUS',
        'REST_4plus_GP', 'REST_4plus_W', 'REST_4plus_L', 'REST_4plus_W_PCT', 'REST_4plus_PTS', 'REST_4plus_PLUS_MINUS',
    ]

    # Only keep columns that exist in the dataframe
    available_columns = [col for col in relevant_columns if col in df.columns]
    df = df[available_columns]

    # Save to parent /data folder (pipeline/data/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(parent_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    output_path = os.path.join(data_dir, output_file)
    df.to_csv(output_path, index=False)

    print("-" * 50)
    print(f"✓ Scraping complete! Data saved to {output_path}")
    print(f"Total teams scraped: {len(all_stats)}")
    print(f"Columns included: {len(df.columns)}")

    return df


if __name__ == "__main__":
    import sys

    # Test mode - just scrape one team for quick testing
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        print("=" * 50)
        print("TEST MODE - Scraping one team only")
        print("=" * 50)

        # Test with Boston Celtics
        test_team = 'Boston Celtics'
        test_id = NBA_TEAM_IDS[test_team]

        print(f"\nTesting with {test_team} (ID: {test_id})")
        print("-" * 50)

        stats = get_team_stats(test_id, season='2025-26')

        if stats:
            stats['TEAM_NAME'] = test_team
            stats['TEAM_ID'] = test_id

            print("\n" + "=" * 50)
            print("SUCCESS! Stats retrieved:")
            print("=" * 50)
            print(f"Team: {stats['TEAM_NAME']}")
            print(
                f"Record: {stats.get('W', 0)}-{stats.get('L', 0)} ({stats.get('W_PCT', 0):.3f})")
            print(f"Points Per Game: {stats.get('PTS', 0)}")
            print(f"Opp Points Per Game: {stats.get('OPP_PTS', 0)}")
            print(f"Plus/Minus: {stats.get('PLUS_MINUS', 0)}")
            print(f"\nTotal stats collected: {len(stats)} columns")
            print("\nAll columns:")
            for key in sorted(stats.keys()):
                print(f"  - {key}: {stats[key]}")
        else:
            print("ERROR: Failed to retrieve stats")

        print("\n" + "=" * 50)
        print("To run full scraper, use: python3 fetch_teams.py")
        print("=" * 50)

    else:
        # Full scrape mode
        df = scrape_all_teams(
            season='2025-26', output_file='nba_team_stats_2025-26.csv')

        # Display preview
        print("\nPreview of scraped data:")
        preview_cols = ['TEAM_NAME', 'W_PCT', 'PTS']
        # Add opponent columns if they exist
        if 'OPP_PTS' in df.columns:
            preview_cols.extend(['OPP_PTS', 'PLUS_MINUS'])
        # Add rest day samples if they exist
        if 'REST_0_W_PCT' in df.columns:
            preview_cols.extend(['REST_0_W_PCT', 'REST_2_W_PCT'])
        print(df[preview_cols].head(10))

        print("\nAvailable columns:")
        print(df.columns.tolist())
