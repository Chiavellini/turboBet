#!/usr/bin/env python3
"""
Visualize NBA Predictions vs Vegas Odds
Shows probability comparisons and best betting opportunities
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import glob
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10


def american_odds_to_probability(odds):
    """Convert American odds to implied probability"""
    if pd.isna(odds):
        return None

    odds = float(odds)
    if odds < 0:  # Favorite
        return abs(odds) / (abs(odds) + 100)
    else:  # Underdog
        return 100 / (odds + 100)


def spread_to_win_probability(spread):
    """
    Convert point spread to win probability
    Uses empirical formula from historical NBA data
    """
    if pd.isna(spread):
        return 0.5

    # Approximate: each point of spread ~= 2.5% win probability change
    # Home team with -7 spread has ~67.5% win probability
    prob = 0.5 + (spread * 0.025)
    return max(0.01, min(0.99, prob))  # Clamp between 1% and 99%


def calculate_edge(actual_prob, predicted_prob):
    """
    Calculate betting edge (difference in implied probabilities)
    Positive edge = Our model thinks team has better chance than Vegas
    """
    return predicted_prob - actual_prob


def load_latest_prediction():
    """Load most recent prediction file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), "data")

    pred_files = glob.glob(os.path.join(data_dir, "odds_prediction_*.csv"))

    if not pred_files:
        print("❌ No prediction files found!")
        print("   Run: cd pipeline/models && python3 simple_predict.py")
        sys.exit(1)

    latest = sorted(pred_files)[-1]
    print(f"Loading: {os.path.basename(latest)}\n")

    return pd.read_csv(latest), data_dir


def create_visualizations(df, output_dir):
    """Create all visualizations as separate PNG files"""

    # Calculate probabilities
    print("Calculating probabilities...")

    # Home team probabilities from spreads
    df['vegas_home_prob'] = df['spread'].apply(
        lambda x: spread_to_win_probability(-x))
    df['predicted_home_prob'] = df['predicted_spread'].apply(
        lambda x: spread_to_win_probability(-x))

    # Away team probabilities (inverse)
    df['vegas_away_prob'] = 1 - df['vegas_home_prob']
    df['predicted_away_prob'] = 1 - df['predicted_home_prob']

    # Calculate edges (where we disagree most with Vegas)
    df['home_edge'] = calculate_edge(
        df['vegas_home_prob'], df['predicted_home_prob'])
    df['away_edge'] = calculate_edge(
        df['vegas_away_prob'], df['predicted_away_prob'])

    # Find best opportunities
    df['max_edge'] = df[['home_edge', 'away_edge']].abs().max(axis=1)
    df['best_bet_team'] = df.apply(
        lambda row: row['home_team'] if abs(row['home_edge']) > abs(
            row['away_edge']) else row['away_team'],
        axis=1
    )
    df['best_bet_edge'] = df.apply(
        lambda row: row['home_edge'] if abs(row['home_edge']) > abs(
            row['away_edge']) else row['away_edge'],
        axis=1
    )

    # Sort by edge for best opportunities
    df_sorted = df.sort_values('max_edge', ascending=False)

    print("\nCreating individual visualizations...")

    # ========== PLOT 1: Best Betting Opportunities ==========
    fig1, ax1 = plt.subplots(figsize=(12, 8))

    n_top = min(10, len(df_sorted))
    top_bets = df_sorted.head(n_top).copy()

    y_pos = np.arange(len(top_bets))
    colors = ['green' if edge >
              0 else 'red' for edge in top_bets['best_bet_edge']]

    bars = ax1.barh(y_pos, top_bets['best_bet_edge'] * 100,
                    color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels([f"{row['away_team']} @ {row['home_team']}\n({row['best_bet_team']})"
                         for _, row in top_bets.iterrows()], fontsize=10)
    ax1.set_xlabel('Edge (% Probability Difference)',
                   fontsize=12, fontweight='bold')
    ax1.set_title('🎯 Best Betting Opportunities (Model vs Vegas)',
                  fontsize=14, fontweight='bold', pad=20)
    ax1.axvline(0, color='black', linestyle='-', linewidth=0.8)
    ax1.grid(axis='x', alpha=0.3)

    # Add value labels
    for i, (_, row) in enumerate(top_bets.iterrows()):
        edge = row['best_bet_edge'] * 100
        label = f"{edge:+.1f}%"
        x_pos = edge + (1 if edge > 0 else -1)
        ax1.text(x_pos, i, label, va='center', fontweight='bold', fontsize=10)

    plt.tight_layout()
    output_file_1 = os.path.join(
        output_dir, '1_best_betting_opportunities.png')
    plt.savefig(output_file_1, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {os.path.basename(output_file_1)}")
    plt.close()

    # ========== PLOT 2: Probability Comparison (All Games) ==========
    fig2, ax2 = plt.subplots(figsize=(10, 8))

    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        matchup = f"{row['away_team'][:10]} @ {row['home_team'][:10]}"

        # Vegas probabilities
        ax2.scatter(row['vegas_away_prob'] * 100, i, color='blue', s=150, marker='o',
                    label='Vegas' if i == 0 else '', alpha=0.6, edgecolors='darkblue', linewidths=1.5)
        ax2.scatter(row['vegas_home_prob'] * 100, i, color='blue', s=150, marker='o', alpha=0.6,
                    edgecolors='darkblue', linewidths=1.5)

        # Our predictions
        ax2.scatter(row['predicted_away_prob'] * 100, i, color='red', s=150, marker='x',
                    label='Our Model' if i == 0 else '', linewidths=3)
        ax2.scatter(row['predicted_home_prob'] * 100, i,
                    color='red', s=150, marker='x', linewidths=3)

        # Connect with lines
        ax2.plot([row['vegas_away_prob'] * 100, row['predicted_away_prob'] * 100],
                 [i, i], 'gray', alpha=0.3, linestyle='--')
        ax2.plot([row['vegas_home_prob'] * 100, row['predicted_home_prob'] * 100],
                 [i, i], 'gray', alpha=0.3, linestyle='--')

    ax2.set_yticks(range(len(df_sorted)))
    ax2.set_yticklabels([f"{row['away_team'][:12]} @ {row['home_team'][:12]}"
                         for _, row in df_sorted.iterrows()], fontsize=9)
    ax2.set_xlabel('Win Probability (%)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Matchup', fontsize=12, fontweight='bold')
    ax2.set_title('Win Probability: Vegas (○) vs Our Model (×)',
                  fontsize=14, fontweight='bold', pad=20)
    ax2.set_ylim(-1, len(df))
    ax2.set_xlim(0, 100)
    ax2.axvline(50, color='black', linestyle='-', linewidth=1.5, alpha=0.5)
    ax2.legend(loc='lower right', fontsize=11)
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    output_file_2 = os.path.join(output_dir, '2_probability_comparison.png')
    plt.savefig(output_file_2, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {os.path.basename(output_file_2)}")
    plt.close()

    # ========== PLOT 3: Edge Distribution ==========
    fig3, ax3 = plt.subplots(figsize=(10, 6))

    all_edges = pd.concat([df['home_edge'], df['away_edge']]) * 100

    ax3.hist(all_edges, bins=20, color='steelblue',
             alpha=0.7, edgecolor='black', linewidth=1.5)
    ax3.axvline(0, color='red', linestyle='--', linewidth=2.5, label='No Edge')
    ax3.axvline(all_edges.mean(), color='green', linestyle=':', linewidth=2.5,
                label=f'Mean Edge: {all_edges.mean():.1f}%')
    ax3.set_xlabel('Edge (%)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Frequency', fontsize=12, fontweight='bold')
    ax3.set_title('Distribution of Betting Edges',
                  fontsize=14, fontweight='bold', pad=20)
    ax3.legend(fontsize=11)
    ax3.grid(alpha=0.3)

    plt.tight_layout()
    output_file_3 = os.path.join(output_dir, '3_edge_distribution.png')
    plt.savefig(output_file_3, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {os.path.basename(output_file_3)}")
    plt.close()

    # ========== PLOT 4: Spread Comparison ==========
    fig4, ax4 = plt.subplots(figsize=(10, 10))

    ax4.scatter(df['spread'], df['predicted_spread'], s=200, alpha=0.6, color='purple',
                edgecolors='darkviolet', linewidths=2)

    # Perfect prediction line
    min_val = min(df['spread'].min(), df['predicted_spread'].min()) - 2
    max_val = max(df['spread'].max(), df['predicted_spread'].max()) + 2
    ax4.plot([min_val, max_val], [min_val, max_val], 'r--',
             linewidth=2.5, label='Perfect Prediction', alpha=0.8)

    # Annotate games
    for _, row in df.iterrows():
        ax4.annotate(f"{row['away_team'][:7]} @\n{row['home_team'][:7]}",
                     (row['spread'], row['predicted_spread']),
                     fontsize=9, alpha=0.8, fontweight='bold',
                     xytext=(5, 5), textcoords='offset points',
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3))

    ax4.set_xlabel('Vegas Spread', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Predicted Spread', fontsize=12, fontweight='bold')
    ax4.set_title('Spread Comparison (closer to red line = better prediction)',
                  fontsize=14, fontweight='bold', pad=20)
    ax4.legend(fontsize=11)
    ax4.grid(alpha=0.3)
    ax4.axhline(0, color='black', linewidth=1)
    ax4.axvline(0, color='black', linewidth=1)

    plt.tight_layout()
    output_file_4 = os.path.join(output_dir, '4_spread_comparison.png')
    plt.savefig(output_file_4, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {os.path.basename(output_file_4)}")
    plt.close()

    # ========== PLOT 5: Best Bets Summary Table ==========
    fig5, ax5 = plt.subplots(figsize=(12, 8))
    ax5.axis('off')

    # Create summary table
    summary_data = []
    for _, row in df_sorted.head(10).iterrows():  # Show top 10
        matchup = f"{row['away_team'][:12]} @ {row['home_team'][:12]}"

        if abs(row['home_edge']) > abs(row['away_edge']):
            team = row['home_team']
            vegas_prob = row['vegas_home_prob'] * 100
            our_prob = row['predicted_home_prob'] * 100
            edge = row['home_edge'] * 100
        else:
            team = row['away_team']
            vegas_prob = row['vegas_away_prob'] * 100
            our_prob = row['predicted_away_prob'] * 100
            edge = row['away_edge'] * 100

        summary_data.append([
            matchup,
            team[:15],
            f"{vegas_prob:.1f}%",
            f"{our_prob:.1f}%",
            f"{edge:+.1f}%"
        ])

    table = ax5.table(cellText=summary_data,
                      colLabels=['Matchup', 'Best Bet',
                                 'Vegas Prob', 'Our Prob', 'Edge'],
                      cellLoc='center',
                      loc='center',
                      colWidths=[0.35, 0.25, 0.13, 0.13, 0.14])

    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.5)

    # Style header
    for i in range(5):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white', fontsize=12)

    # Color code rows alternately and edge column
    for i in range(1, len(summary_data) + 1):
        # Alternate row colors
        row_color = '#F0F0F0' if i % 2 == 0 else 'white'
        for j in range(5):
            if j != 4:  # Don't color edge column yet
                table[(i, j)].set_facecolor(row_color)

        # Color code edge column
        edge_val = float(summary_data[i-1][4].strip('%+'))
        if edge_val > 5:
            table[(i, 4)].set_facecolor('#2ECC71')  # Strong green
            table[(i, 4)].set_text_props(weight='bold', color='white')
        elif edge_val > 0:
            table[(i, 4)].set_facecolor('#90EE90')  # Light green
        elif edge_val > -5:
            table[(i, 4)].set_facecolor('#FFB6C6')  # Light red
        else:
            table[(i, 4)].set_facecolor('#E74C3C')  # Strong red
            table[(i, 4)].set_text_props(weight='bold', color='white')

    ax5.set_title('🏆 Best Betting Opportunities - Ranked',
                  fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
    output_file_5 = os.path.join(output_dir, '5_best_bets_table.png')
    plt.savefig(output_file_5, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {os.path.basename(output_file_5)}")
    plt.close()

    print(f"\n✅ All 5 visualizations saved to: pipeline/data/")

    return df_sorted


def print_betting_summary(df):
    """Print detailed betting recommendations"""

    print("\n" + "="*80)
    print("🎯 BETTING RECOMMENDATIONS")
    print("="*80)

    top_bets = df.head(5)

    for i, (_, row) in enumerate(top_bets.iterrows(), 1):
        print(f"\n{i}. {row['away_team']} @ {row['home_team']}")
        print("-" * 80)

        # Determine which team has the edge
        if abs(row['home_edge']) > abs(row['away_edge']):
            bet_team = row['home_team']
            vegas_prob = row['vegas_home_prob'] * 100
            our_prob = row['predicted_home_prob'] * 100
            edge = row['home_edge'] * 100
            spread = row['spread']
        else:
            bet_team = row['away_team']
            vegas_prob = row['vegas_away_prob'] * 100
            our_prob = row['predicted_away_prob'] * 100
            edge = row['away_edge'] * 100
            spread = -row['spread']

        print(f"   💰 RECOMMENDED BET: {bet_team}")
        print(f"   📊 Vegas Win Probability:  {vegas_prob:.1f}%")
        print(f"   🤖 Our Model Probability:  {our_prob:.1f}%")
        print(f"   📈 Edge:                   {edge:+.1f}%")
        print(f"   📉 Spread:                 {spread:+.1f}")

        if edge > 5:
            print(
                f"   ⭐ STRONG VALUE - Model sees {abs(edge):.1f}% more value than Vegas")
        elif edge > 2:
            print(
                f"   ✓ MODERATE VALUE - Model sees {abs(edge):.1f}% more value")
        else:
            print(
                f"   ℹ️  SLIGHT VALUE - Model sees {abs(edge):.1f}% more value")

    print("\n" + "="*80)
    print("⚠️  DISCLAIMER: These are model predictions, not guaranteed outcomes!")
    print("    Always bet responsibly and within your means.")
    print("="*80)


def main():
    """Main execution"""

    print("="*80)
    print("📊 NBA PREDICTIONS VISUALIZER")
    print("="*80)

    # Load data
    df, output_dir = load_latest_prediction()

    print(f"Loaded {len(df)} games\n")

    # Create visualizations
    df_sorted = create_visualizations(df, output_dir)

    # Print betting summary
    print_betting_summary(df_sorted)

    print(f"\n✅ Complete! Check the visualization for detailed comparison.")


if __name__ == "__main__":
    main()
