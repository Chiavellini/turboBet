#!/usr/bin/env python3
"""
NBA Odds Prediction Algorithm using PySpark
Predicts betting odds based on team statistics and compares to actual odds.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os
import sys

def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("NBA Odds Prediction") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def load_team_stats(spark, stats_file):
    """Load NBA team statistics CSV"""
    print(f"Loading team stats from: {stats_file}")
    
    df = spark.read.csv(stats_file, header=True, inferSchema=True)
    
    print(f"  ✓ Loaded {df.count()} teams")
    print(f"  Columns: {len(df.columns)}")
    
    return df


def load_betting_odds(spark, odds_file):
    """Load actual betting odds CSV"""
    print(f"\nLoading betting odds from: {odds_file}")
    
    df = spark.read.csv(odds_file, header=True, inferSchema=True)
    
    # Filter for NBA games only
    df = df.filter(F.col("sport") == "NBA")
    
    print(f"  ✓ Loaded {df.count()} NBA games")
    
    return df


def calculate_team_strength(stats_df):
    """
    Calculate overall team strength metrics
    Higher values = stronger team
    """
    print("\nCalculating team strength metrics...")
    
    # Offensive Rating: weighted combination of scoring efficiency
    stats_df = stats_df.withColumn(
        "offensive_rating",
        (F.col("PTS") * 0.4) +           # Points per game (40% weight)
        (F.col("FG_PCT") * 100 * 0.3) +  # Field goal % (30% weight)
        (F.col("AST") * 0.2) +           # Assists (20% weight)
        (F.col("FG3_PCT") * 50 * 0.1)    # 3PT % (10% weight)
    )
    
    # Defensive Rating: lower opponent stats = better defense
    stats_df = stats_df.withColumn(
        "defensive_rating",
        (120 - F.col("OPP_PTS")) * 0.5 +           # Points allowed (50% weight)
        ((1 - F.col("OPP_FG_PCT")) * 100 * 0.3) +  # Opp FG% (30% weight)
        (F.col("STL") * 0.15) +                     # Steals (15% weight)
        (F.col("BLK") * 0.05)                       # Blocks (5% weight)
    )
    
    # Overall Strength: combination of offense and defense
    stats_df = stats_df.withColumn(
        "team_strength",
        (F.col("offensive_rating") * 0.55) +  # Offense 55%
        (F.col("defensive_rating") * 0.35) +  # Defense 35%
        (F.col("W_PCT") * 50 * 0.10)          # Win % 10%
    )
    
    print(f"  ✓ Calculated strength metrics for {stats_df.count()} teams")
    
    return stats_df


def predict_matchup_odds(odds_df, stats_df):
    """
    Predict odds for each matchup based on team stats
    """
    print("\nPredicting odds for matchups...")
    
    # Join away team stats
    matchups = odds_df.alias("odds") \
        .join(
            stats_df.alias("away"),
            F.col("odds.away_team") == F.col("away.TEAM_NAME"),
            "left"
        )
    
    # Join home team stats
    matchups = matchups \
        .join(
            stats_df.alias("home"),
            F.col("odds.home_team") == F.col("home.TEAM_NAME"),
            "left"
        )
    
    # Calculate predicted spread based on strength difference
    # Positive spread = home team favored
    matchups = matchups.withColumn(
        "predicted_spread",
        (F.col("home.team_strength") - F.col("away.team_strength")) / 3.0
    )
    
    # Predict total points (over/under)
    # Average of both teams' scoring + pace adjustment
    matchups = matchups.withColumn(
        "predicted_total",
        (F.col("away.PTS") + F.col("home.PTS")) * 1.02  # Slight adjustment
    )
    
    # Predict moneylines based on spread
    # More negative spread = stronger favorite = more negative moneyline
    matchups = matchups.withColumn(
        "predicted_home_ml",
        F.when(F.col("predicted_spread") > 0, 
               -100 - (F.col("predicted_spread") * 20))  # Favorite
        .otherwise(
               100 + (F.abs(F.col("predicted_spread")) * 20))  # Underdog
    )
    
    matchups = matchups.withColumn(
        "predicted_away_ml",
        F.when(F.col("predicted_spread") < 0,
               -100 - (F.abs(F.col("predicted_spread")) * 20))  # Favorite
        .otherwise(
               100 + (F.col("predicted_spread") * 20))  # Underdog
    )
    
    # Select relevant columns for comparison
    comparison = matchups.select(
        F.col("odds.away_team").alias("away_team"),
        F.col("odds.home_team").alias("home_team"),
        
        # Actual odds
        F.col("odds.spread").alias("actual_spread"),
        F.col("odds.away_moneyline").alias("actual_away_ml"),
        F.col("odds.home_moneyline").alias("actual_home_ml"),
        F.col("odds.total").alias("actual_total"),
        
        # Predicted odds
        F.round("predicted_spread", 1).alias("predicted_spread"),
        F.round("predicted_away_ml", 0).alias("predicted_away_ml"),
        F.round("predicted_home_ml", 0).alias("predicted_home_ml"),
        F.round("predicted_total", 1).alias("predicted_total"),
        
        # Team strengths for reference
        F.round("away.team_strength", 1).alias("away_strength"),
        F.round("home.team_strength", 1).alias("home_strength"),
        F.round("away.W_PCT", 3).alias("away_win_pct"),
        F.round("home.W_PCT", 3).alias("home_win_pct"),
    )
    
    # Calculate differences (prediction error)
    comparison = comparison.withColumn(
        "spread_diff",
        F.round(F.abs(F.col("actual_spread") - F.col("predicted_spread")), 1)
    )
    
    comparison = comparison.withColumn(
        "total_diff",
        F.round(F.abs(F.col("actual_total") - F.col("predicted_total")), 1)
    )
    
    print(f"  ✓ Predicted odds for {comparison.count()} matchups")
    
    return comparison


def analyze_predictions(comparison_df):
    """Analyze prediction accuracy"""
    print("\n" + "="*70)
    print("PREDICTION ACCURACY ANALYSIS")
    print("="*70)
    
    # Calculate average errors
    avg_spread_error = comparison_df.agg(
        F.avg("spread_diff").alias("avg_spread_error")
    ).collect()[0]["avg_spread_error"]
    
    avg_total_error = comparison_df.agg(
        F.avg("total_diff").alias("avg_total_error")
    ).collect()[0]["avg_total_error"]
    
    print(f"\nAverage Prediction Errors:")
    print(f"  Spread: ±{avg_spread_error:.2f} points")
    print(f"  Total:  ±{avg_total_error:.2f} points")
    
    # Find best and worst predictions
    print(f"\nBest Spread Predictions:")
    best_spread = comparison_df.orderBy("spread_diff").limit(3)
    best_spread.select("away_team", "home_team", "actual_spread", 
                       "predicted_spread", "spread_diff").show(truncate=False)
    
    print(f"\nWorst Spread Predictions:")
    worst_spread = comparison_df.orderBy(F.desc("spread_diff")).limit(3)
    worst_spread.select("away_team", "home_team", "actual_spread", 
                        "predicted_spread", "spread_diff").show(truncate=False)


def main():
    """Main execution function"""
    # Get data directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "data")
    
    # Find the most recent files
    import glob
    
    stats_files = glob.glob(os.path.join(data_dir, "nba_team_stats_*.csv"))
    odds_files = glob.glob(os.path.join(data_dir, "nba_odds_*.csv"))
    
    if not stats_files:
        print("❌ Error: No NBA team stats file found in pipeline/data/")
        print("   Run: python3 nba_scraper.py")
        sys.exit(1)
    
    if not odds_files:
        print("❌ Error: No NBA odds file found in pipeline/data/")
        print("   Run: python3 fetch_odds.py --auto")
        sys.exit(1)
    
    stats_file = sorted(stats_files)[-1]  # Most recent
    odds_file = sorted(odds_files)[-1]    # Most recent
    
    print("=" * 70)
    print("NBA ODDS PREDICTION & COMPARISON")
    print("=" * 70)
    print(f"\nUsing files:")
    print(f"  Stats: {os.path.basename(stats_file)}")
    print(f"  Odds:  {os.path.basename(odds_file)}")
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Load data
        stats_df = load_team_stats(spark, stats_file)
        odds_df = load_betting_odds(spark, odds_file)
        
        # Calculate team strengths
        stats_with_strength = calculate_team_strength(stats_df)
        
        # Predict matchup odds
        comparison = predict_matchup_odds(odds_df, stats_with_strength)
        
        # Show all predictions
        print("\n" + "="*70)
        print("PREDICTED vs ACTUAL ODDS")
        print("="*70)
        comparison.select(
            "away_team", "home_team",
            "actual_spread", "predicted_spread", "spread_diff",
            "actual_total", "predicted_total", "total_diff"
        ).show(100, truncate=False)
        
        # Analyze accuracy
        analyze_predictions(comparison)
        
        # Save results
        output_file = os.path.join(data_dir, f"odds_comparison_{os.path.basename(odds_file)}")
        comparison.coalesce(1).write.mode("overwrite").option("header", True).csv(output_file)
        print(f"\n✓ Comparison saved to: {output_file}")
        
        # Also save as single file for easier viewing
        output_single = output_file.replace(".csv", "_single.csv")
        comparison.toPandas().to_csv(output_single, index=False)
        print(f"✓ Single file saved to: {output_single}")
        
    finally:
        spark.stop()
    
    print("\n" + "="*70)
    print("COMPLETE!")
    print("="*70)


if __name__ == "__main__":
    main()

