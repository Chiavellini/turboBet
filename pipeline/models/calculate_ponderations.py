"""
PySpark script to generate predictive odds based on NBA team statistics.

This script:
1. Loads team statistics and today's odds
2. Calculates predictive odds using team performance metrics
3. Compares predictions with sportsbook odds
4. Outputs results to ponderations_for_today.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import os
from datetime import datetime


def initialize_spark():
    """Initialize Spark session"""
    spark = SparkSession.builder \
        .appName("NBA Odds Ponderation") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, data_dir):
    """Load team stats and odds CSVs"""

    # Load team stats
    team_stats_path = os.path.join(data_dir, "nba_team_stats_2025-26.csv")
    team_stats = spark.read.csv(team_stats_path, header=True, inferSchema=True)

    # Find the most recent odds file
    odds_files = [f for f in os.listdir(data_dir) if f.startswith("nba_odds_") and f.endswith(".csv")]
    if not odds_files:
        raise FileNotFoundError("No odds file found in data directory")

    latest_odds_file = sorted(odds_files)[-1]
    odds_path = os.path.join(data_dir, latest_odds_file)
    odds = spark.read.csv(odds_path, header=True, inferSchema=True)

    print(f"Loaded team stats: {team_stats.count()} teams")
    print(f"Loaded odds from: {latest_odds_file} ({odds.count()} games)")

    return team_stats, odds


def calculate_team_power_rating(team_stats):
    """
    Calculate a comprehensive power rating for each team based on:
    - Offensive efficiency (points scored)
    - Defensive efficiency (points allowed)
    - Win percentage
    - Plus/minus differential
    - Field goal percentages
    """

    # Normalize stats to 0-100 scale for each metric
    window_spec = Window.orderBy(F.lit(1))

    team_ratings = team_stats.select(
        F.col("TEAM_NAME"),
        F.col("PTS").alias("pts_per_game"),
        F.col("OPP_PTS").alias("opp_pts_per_game"),
        F.col("W_PCT").alias("win_pct"),
        F.col("PLUS_MINUS").alias("plus_minus"),
        F.col("FG_PCT").alias("fg_pct"),
        F.col("FG3_PCT").alias("fg3_pct"),
        F.col("OPP_FG_PCT").alias("opp_fg_pct"),
        F.col("REB").alias("rebounds"),
        F.col("AST").alias("assists"),
        F.col("TOV").alias("turnovers"),
        F.col("OPP_TOV").alias("opp_turnovers"),

        # Rest day performance (use 1-day rest as baseline)
        F.col("REST_1_W_PCT").alias("rest_1_win_pct"),
        F.col("REST_1_PTS").alias("rest_1_pts")
    )

    # Calculate composite power rating (weighted average)
    # Higher weight on direct indicators: win%, plus_minus, pts differential
    team_ratings = team_ratings.withColumn(
        "offensive_rating",
        (F.col("pts_per_game") * 0.4 +
         F.col("fg_pct") * 100 * 0.3 +
         F.col("fg3_pct") * 100 * 0.15 +
         F.col("assists") * 2 * 0.15)
    ).withColumn(
        "defensive_rating",
        (100 - F.col("opp_pts_per_game") * 0.4 +
         (1 - F.col("opp_fg_pct")) * 100 * 0.4 +
         F.col("opp_turnovers") * 3 * 0.2)
    ).withColumn(
        "power_rating",
        (F.col("win_pct") * 100 * 0.25 +
         F.col("plus_minus") * 2 * 0.2 +
         F.col("offensive_rating") * 0.3 +
         F.col("defensive_rating") * 0.25)
    )

    return team_ratings


def calculate_matchup_predictions(odds, away_ratings, home_ratings):
    """
    Calculate predicted outcomes for each matchup based on team ratings.

    Returns predictions including:
    - Predicted winner
    - Predicted point differential
    - Predicted total points
    - Win probability for each team
    """

    # Join away team stats
    predictions = odds.join(
        away_ratings,
        odds.away_team == away_ratings.TEAM_NAME,
        "left"
    ).select(
        odds["*"],
        F.col("power_rating").alias("away_power_rating"),
        F.col("offensive_rating").alias("away_offensive"),
        F.col("defensive_rating").alias("away_defensive"),
        F.col("pts_per_game").alias("away_avg_pts"),
        F.col("opp_pts_per_game").alias("away_avg_opp_pts")
    )

    # Join home team stats
    predictions = predictions.join(
        home_ratings,
        predictions.home_team == home_ratings.TEAM_NAME,
        "left"
    ).select(
        predictions["*"],
        F.col("power_rating").alias("home_power_rating"),
        F.col("offensive_rating").alias("home_offensive"),
        F.col("defensive_rating").alias("home_defensive"),
        F.col("pts_per_game").alias("home_avg_pts"),
        F.col("opp_pts_per_game").alias("home_avg_opp_pts")
    )

    # Calculate home court advantage (typically ~3 points in NBA)
    HOME_COURT_ADVANTAGE = 3.0

    # Predict point differential (positive = home team favored)
    predictions = predictions.withColumn(
        "predicted_point_diff",
        (F.col("home_power_rating") - F.col("away_power_rating")) / 10 + HOME_COURT_ADVANTAGE
    )

    # Predict total points (average of both teams' offensive vs defensive matchup)
    predictions = predictions.withColumn(
        "predicted_total_points",
        ((F.col("away_avg_pts") + F.col("home_avg_opp_pts")) / 2 +
         (F.col("home_avg_pts") + F.col("away_avg_opp_pts")) / 2)
    )

    # Calculate win probability using logistic function
    # Probability = 1 / (1 + e^(-point_diff / 10))
    predictions = predictions.withColumn(
        "home_win_probability",
        1 / (1 + F.exp(-F.col("predicted_point_diff") / 10))
    ).withColumn(
        "away_win_probability",
        1 - F.col("home_win_probability")
    )

    # Convert probabilities to American odds
    predictions = predictions.withColumn(
        "predicted_home_moneyline",
        F.when(F.col("home_win_probability") > 0.5,
               -100 * F.col("home_win_probability") / (1 - F.col("home_win_probability"))
        ).otherwise(
            100 * (1 - F.col("home_win_probability")) / F.col("home_win_probability")
        )
    ).withColumn(
        "predicted_away_moneyline",
        F.when(F.col("away_win_probability") > 0.5,
               -100 * F.col("away_win_probability") / (1 - F.col("away_win_probability"))
        ).otherwise(
            100 * (1 - F.col("away_win_probability")) / F.col("away_win_probability")
        )
    )

    # Calculate value indicators (difference between predicted and sportsbook odds)
    # Note: sportsbook spread uses opposite sign convention (negative = home favored)
    # So we need to flip the sign: predicted (positive = home favored) vs -spread
    predictions = predictions.withColumn(
        "spread_difference",
        F.col("predicted_point_diff") + F.col("spread")
    ).withColumn(
        "total_difference",
        F.col("total") - F.col("predicted_total_points")
    ).withColumn(
        "home_ml_difference",
        F.col("home_moneyline") - F.col("predicted_home_moneyline")
    ).withColumn(
        "away_ml_difference",
        F.col("away_moneyline") - F.col("predicted_away_moneyline")
    )

    # Determine betting recommendations
    predictions = predictions.withColumn(
        "spread_recommendation",
        F.when(F.col("spread_difference") > 3, "BET HOME TEAM (better than spread)")
        .when(F.col("spread_difference") < -3, "BET AWAY TEAM (better than spread)")
        .otherwise("NO VALUE")
    ).withColumn(
        "total_recommendation",
        F.when(F.col("total_difference") > 5, "BET UNDER (predicted lower)")
        .when(F.col("total_difference") < -5, "BET OVER (predicted higher)")
        .otherwise("NO VALUE")
    ).withColumn(
        "moneyline_recommendation",
        F.when(
            (F.col("home_win_probability") > 0.55) & (F.col("home_moneyline") > 0),
            "VALUE: BET HOME TEAM"
        ).when(
            (F.col("away_win_probability") > 0.55) & (F.col("away_moneyline") > 0),
            "VALUE: BET AWAY TEAM"
        ).otherwise("NO VALUE")
    )

    return predictions


def save_ponderations(predictions, output_dir):
    """Save predictions to CSV"""

    # Select and order columns for output
    output_df = predictions.select(
        "sport",
        "away_team",
        "home_team",

        # Sportsbook odds
        F.round("spread", 1).alias("sportsbook_spread"),
        "away_moneyline",
        "home_moneyline",
        F.round("total", 1).alias("sportsbook_total"),

        # Predicted values
        F.round("predicted_point_diff", 1).alias("predicted_spread"),
        F.round("predicted_away_moneyline", 0).alias("predicted_away_ml"),
        F.round("predicted_home_moneyline", 0).alias("predicted_home_ml"),
        F.round("predicted_total_points", 1).alias("predicted_total"),

        # Win probabilities
        F.round("away_win_probability", 3).alias("away_win_prob"),
        F.round("home_win_probability", 3).alias("home_win_prob"),

        # Power ratings
        F.round("away_power_rating", 1).alias("away_power"),
        F.round("home_power_rating", 1).alias("home_power"),

        # Value indicators
        F.round("spread_difference", 1).alias("spread_diff"),
        F.round("total_difference", 1).alias("total_diff"),

        # Recommendations
        "spread_recommendation",
        "total_recommendation",
        "moneyline_recommendation"
    )

    # Convert to Pandas for easy CSV writing
    output_pandas = output_df.toPandas()

    # Save to CSV
    output_path = os.path.join(output_dir, "ponderations_for_today.csv")
    output_pandas.to_csv(output_path, index=False)

    print(f"\n{'='*80}")
    print(f"Ponderations saved to: {output_path}")
    print(f"{'='*80}")

    return output_path


def print_summary(predictions):
    """Print summary of predictions"""

    predictions_pd = predictions.select(
        "away_team",
        "home_team",
        "predicted_point_diff",
        "spread",
        "spread_recommendation",
        "total_recommendation",
        "moneyline_recommendation"
    ).toPandas()

    print("\n" + "="*80)
    print("BETTING RECOMMENDATIONS SUMMARY")
    print("="*80)

    for idx, row in predictions_pd.iterrows():
        print(f"\n{row['away_team']} @ {row['home_team']}")
        print(f"  Predicted spread: {row['predicted_point_diff']:.1f} | Sportsbook: {row['spread']:.1f}")
        print(f"  Spread: {row['spread_recommendation']}")
        print(f"  Total: {row['total_recommendation']}")
        print(f"  Moneyline: {row['moneyline_recommendation']}")

    print("\n" + "="*80)

    # Count value bets
    value_bets = predictions_pd[
        (predictions_pd['spread_recommendation'] != 'NO VALUE') |
        (predictions_pd['total_recommendation'] != 'NO VALUE') |
        (predictions_pd['moneyline_recommendation'] != 'NO VALUE')
    ]

    print(f"FOUND {len(value_bets)} GAMES WITH POTENTIAL VALUE BETS")
    print("="*80 + "\n")


def main():
    """Main execution function"""

    print("\n" + "="*80)
    print("NBA ODDS PONDERATION CALCULATOR")
    print("="*80 + "\n")

    # Initialize Spark
    spark = initialize_spark()

    # Set up paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(pipeline_dir, "data")

    print(f"Data directory: {data_dir}\n")

    # Load data
    print("Step 1: Loading data...")
    team_stats, odds = load_data(spark, data_dir)

    # Calculate team ratings
    print("\nStep 2: Calculating team power ratings...")
    team_ratings = calculate_team_power_rating(team_stats)

    # Show top teams
    print("\nTop 5 Teams by Power Rating:")
    team_ratings.select("TEAM_NAME", F.round("power_rating", 1).alias("power_rating")) \
        .orderBy(F.desc("power_rating")) \
        .show(5, truncate=False)

    # Calculate predictions
    print("Step 3: Calculating matchup predictions...")
    predictions = calculate_matchup_predictions(odds, team_ratings, team_ratings)

    # Save results
    print("\nStep 4: Saving ponderations...")
    output_path = save_ponderations(predictions, data_dir)

    # Print summary
    print_summary(predictions)

    # Stop Spark
    spark.stop()

    print(f"Analysis complete! Check {output_path} for full results.\n")


if __name__ == "__main__":
    main()
