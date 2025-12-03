#!/usr/bin/env python3
"""
Simple NBA Odds Prediction (without PySpark)
Uses pandas for simpler, faster predictions on small datasets.
"""

import pandas as pd
import os
import sys
import glob


def load_data(data_dir):
    """Load most recent stats and odds files"""

    # Find files
    stats_files = glob.glob(os.path.join(data_dir, "nba_team_stats_*.csv"))
    odds_files = glob.glob(os.path.join(data_dir, "nba_odds_*.csv"))

    if not stats_files:
        print("❌ Error: No NBA team stats file found")
        print("   Run: cd pipeline/scrapers && python3 nba_scraper.py")
        sys.exit(1)

    if not odds_files:
        print("❌ Error: No NBA odds file found")
        print("   Run: cd pipeline/scrapers && python3 fetch_odds.py --auto")
        sys.exit(1)

    stats_file = sorted(stats_files)[-1]
    odds_file = sorted(odds_files)[-1]

    print(f"Loading data...")
    print(f"  Stats: {os.path.basename(stats_file)}")
    print(f"  Odds:  {os.path.basename(odds_file)}")

    stats_df = pd.read_csv(stats_file)
    odds_df = pd.read_csv(odds_file)
    odds_df = odds_df[odds_df['sport'] == 'NBA']  # NBA only

    print(f"  ✓ {len(stats_df)} teams, {len(odds_df)} matchups\n")

    return stats_df, odds_df, os.path.basename(odds_file)


def calculate_team_strength(stats_df):
    """Calculate simple team strength metric"""

    # Offensive rating (higher is better)
    stats_df['off_rating'] = (
        stats_df['PTS'] * 0.4 +
        stats_df['FG_PCT'] * 100 * 0.3 +
        stats_df['AST'] * 0.2 +
        stats_df['FG3_PCT'] * 50 * 0.1
    )

    # Defensive rating (lower opponent stats is better)
    stats_df['def_rating'] = (
        (120 - stats_df['OPP_PTS']) * 0.5 +
        ((1 - stats_df['OPP_FG_PCT']) * 100 * 0.3) +
        stats_df['STL'] * 0.15 +
        stats_df['BLK'] * 0.05
    )

    # Overall strength
    stats_df['team_strength'] = (
        stats_df['off_rating'] * 0.55 +
        stats_df['def_rating'] * 0.35 +
        stats_df['W_PCT'] * 50 * 0.10
    )

    return stats_df


def predict_odds(odds_df, stats_df):
    """Predict odds for each matchup"""

    # Merge stats for both teams
    merged = odds_df.merge(
        stats_df[['TEAM_NAME', 'team_strength', 'PTS', 'OPP_PTS', 'W_PCT']],
        left_on='away_team',
        right_on='TEAM_NAME',
        how='left',
        suffixes=('', '_away')
    ).merge(
        stats_df[['TEAM_NAME', 'team_strength', 'PTS', 'OPP_PTS', 'W_PCT']],
        left_on='home_team',
        right_on='TEAM_NAME',
        how='left',
        suffixes=('_away', '_home')
    )

    # Predict spread (positive = home favored)
    merged['predicted_spread'] = (
        (merged['team_strength_home'] - merged['team_strength_away']) / 3.0
    ).round(1)

    # Predict total
    merged['predicted_total'] = (
        (merged['PTS_away'] + merged['PTS_home']) * 1.02
    ).round(1)

    # Predict moneylines from spread
    def spread_to_moneyline(spread, is_favorite):
        """Convert spread to American odds moneyline"""
        abs_spread = abs(spread)
        if is_favorite:  # Negative moneyline
            return -100 - (abs_spread * 20)
        else:  # Positive moneyline
            return 100 + (abs_spread * 20)

    merged['predicted_home_ml'] = merged['predicted_spread'].apply(
        lambda s: spread_to_moneyline(
            s, s < 0) if s < 0 else spread_to_moneyline(s, False)
    ).round(0)

    merged['predicted_away_ml'] = merged['predicted_spread'].apply(
        lambda s: spread_to_moneyline(
            s, s > 0) if s > 0 else spread_to_moneyline(s, False)
    ).round(0)

    # Calculate differences
    merged['spread_diff'] = abs(
        merged['spread'] - merged['predicted_spread']).round(1)
    merged['total_diff'] = abs(
        merged['total'] - merged['predicted_total']).round(1)

    # Home court adjustment (typically 3-4 points)
    merged['predicted_spread_hca'] = (
        merged['predicted_spread'] + 3.0).round(1)

    return merged


def print_results(results):
    """Print formatted results"""

    print("\n" + "="*100)
    print("PREDICTED vs ACTUAL ODDS")
    print("="*100)
    print(f"\n{'Matchup':<50} {'Actual':^15} {'Predicted':^15} {'Diff':^10}")
    print(f"{'':50} {'Spread':^15} {'Spread':^15} {'':^10}")
    print("-" * 100)

    for _, row in results.iterrows():
        matchup = f"{row['away_team']} @ {row['home_team']}"
        print(
            f"{matchup:<50} {row['spread']:>7.1f}       {row['predicted_spread']:>7.1f}       {row['spread_diff']:>7.1f}")

    print("\n" + "="*100)
    print("ACCURACY METRICS")
    print("="*100)

    avg_spread_err = results['spread_diff'].mean()
    avg_total_err = results['total_diff'].mean()

    print(f"\nAverage Errors:")
    print(f"  Spread: ±{avg_spread_err:.2f} points")
    print(f"  Total:  ±{avg_total_err:.2f} points")

    print(f"\nBest Predictions (smallest error):")
    best = results.nsmallest(3, 'spread_diff')
    for _, row in best.iterrows():
        print(f"  {row['away_team']} @ {row['home_team']}: "
              f"Actual {row['spread']:.1f}, Predicted {row['predicted_spread']:.1f} "
              f"(±{row['spread_diff']:.1f})")

    print(f"\nWorst Predictions (largest error):")
    worst = results.nlargest(3, 'spread_diff')
    for _, row in worst.iterrows():
        print(f"  {row['away_team']} @ {row['home_team']}: "
              f"Actual {row['spread']:.1f}, Predicted {row['predicted_spread']:.1f} "
              f"(±{row['spread_diff']:.1f})")

    print(f"\nOportunidades de valor (diferencias más grandes entre nuuestro algoritmo y Vegas):")
    print("(Nuestras mejores apuestas)")
    value_bets = results.nlargest(3, 'spread_diff')
    for _, row in value_bets.iterrows():
        if row['predicted_spread'] > row['spread']:
            suggestion = f"Consider: {row['home_team']} +{abs(row['spread']):.1f}"
        else:
            suggestion = f"Consider: {row['away_team']} +{abs(row['spread']):.1f}"
        print(f"  {row['away_team']} @ {row['home_team']}: {suggestion}")


def main():
    """Main execution"""

    # Get data directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "data")

    # Load data
    stats_df, odds_df, odds_filename = load_data(data_dir)

    print("="*100)
    print("NBA ODDS PREDICTION (Simple Algorithm)")
    print("="*100)

    # Calculate strengths
    print("\nCalculating team strengths...")
    stats_df = calculate_team_strength(stats_df)
    print(f"  ✓ Strength metrics calculated")

    # Predict
    print("\nPredicting matchup odds...")
    results = predict_odds(odds_df, stats_df)
    print(f"  ✓ {len(results)} matchups predicted")

    # Display results
    print_results(results)

    # Save
    output_file = os.path.join(data_dir, f"odds_prediction_{odds_filename}")

    # Select key columns for output
    output_cols = [
        'away_team', 'home_team',
        'spread', 'predicted_spread', 'spread_diff',
        'away_moneyline', 'predicted_away_ml',
        'home_moneyline', 'predicted_home_ml',
        'total', 'predicted_total', 'total_diff',
        'team_strength_away', 'team_strength_home'
    ]

    results[output_cols].to_csv(output_file, index=False)
    print(f"\n✓ Detailed comparison saved to: {os.path.basename(output_file)}")

    print("\n" + "="*100)


if __name__ == "__main__":
    main()
