"""
Professional, beginner-friendly betting value visualizations.

Creates clean, easy-to-understand graphics showing:
- Which bets have value
- Clear explanations for betting newcomers
- Professional appearance
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle, FancyBboxPatch
import numpy as np
import os
from datetime import datetime

# Professional styling
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'


def load_data(data_dir):
    """Load ponderations data from CSV"""
    ponderations_path = os.path.join(data_dir, "ponderations_for_today.csv")

    if not os.path.exists(ponderations_path):
        raise FileNotFoundError(f"Ponderations file not found: {ponderations_path}")

    df = pd.read_csv(ponderations_path)
    print(f"Loaded {len(df)} games from ponderations file")

    # Create cleaner team names
    df['away_short'] = df['away_team'].str.split().str[-1]
    df['home_short'] = df['home_team'].str.split().str[-1]
    df['matchup'] = df['away_short'] + ' @ ' + df['home_short']

    return df


def create_daily_betting_sheet(df, output_dir):
    """
    Create a clean, professional daily betting sheet - like what you'd see
    in a sports newspaper or betting guide.
    """
    fig = plt.figure(figsize=(17, 11))
    fig.patch.set_facecolor('white')

    # Title section
    title_text = f"NBA Value Betting Sheet - {datetime.now().strftime('%A, %B %d, %Y')}"
    fig.text(0.5, 0.96, title_text, ha='center', fontsize=18, fontweight='bold', color='#2c3e50')
    fig.text(0.5, 0.93, "Recommended bets where our predictions differ significantly from sportsbook odds",
             ha='center', fontsize=11, color='#7f8c8d', style='italic')

    # Create sections
    gs = fig.add_gridspec(len(df) + 2, 1, hspace=0.4, top=0.90, bottom=0.05, left=0.05, right=0.95)

    # Legend section
    ax_legend = fig.add_subplot(gs[0, 0])
    ax_legend.axis('off')

    legend_text = "How to Read: GREEN = STRONG VALUE BET | YELLOW = VALUE BET | GRAY = NO VALUE | Edge = Our advantage in points"
    ax_legend.text(0.5, 0.5, legend_text, ha='center', va='center',
                  fontsize=10, bbox=dict(boxstyle='round,pad=0.5', facecolor='#ecf0f1', edgecolor='#95a5a6', linewidth=2))

    # Game rows
    for idx, (_, game) in enumerate(df.iterrows()):
        ax = fig.add_subplot(gs[idx + 1, 0])
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 3)
        ax.axis('off')

        # Background
        bg_color = '#f8f9fa'
        rect = FancyBboxPatch((0, 0), 10, 3, boxstyle="round,pad=0.1",
                             edgecolor='#dee2e6', facecolor=bg_color, linewidth=2)
        ax.add_patch(rect)

        # Game header
        ax.text(5, 2.5, f"{game['away_team']} @ {game['home_team']}",
               ha='center', va='top', fontsize=12, fontweight='bold', color='#2c3e50')

        # Power ratings
        power_text = f"Power Ratings: {game['away_short']} {game['away_power']:.1f} | {game['home_short']} {game['home_power']:.1f}"
        ax.text(5, 2.1, power_text, ha='center', va='top', fontsize=9, color='#7f8c8d')

        # Three betting sections
        y_pos = 1.5

        # SPREAD
        spread_width = 3.2
        spread_x = 0.3
        if 'HOME' in game['spread_recommendation']:
            spread_color = '#d4edda'
            spread_border = '#28a745'
            bet_team = game['home_short']
            bet_line = game['sportsbook_spread']
            strength = "STRONG" if abs(game['spread_diff']) > 10 else "VALUE"
        elif 'AWAY' in game['spread_recommendation']:
            spread_color = '#d4edda'
            spread_border = '#28a745'
            bet_team = game['away_short']
            bet_line = -game['sportsbook_spread']
            strength = "STRONG" if abs(game['spread_diff']) > 10 else "VALUE"
        else:
            spread_color = '#e9ecef'
            spread_border = '#adb5bd'
            bet_team = "No Value"
            bet_line = None
            strength = ""

        spread_box = FancyBboxPatch((spread_x, 0.2), spread_width, 1.6, boxstyle="round,pad=0.05",
                                   edgecolor=spread_border, facecolor=spread_color, linewidth=2)
        ax.add_patch(spread_box)

        ax.text(spread_x + spread_width/2, 1.5, "SPREAD BET", ha='center', fontsize=9, fontweight='bold', color='#495057')
        if bet_line is not None:
            ax.text(spread_x + spread_width/2, 1.1, f"{strength}", ha='center', fontsize=8,
                   color=spread_border, fontweight='bold')
            ax.text(spread_x + spread_width/2, 0.75, f"Bet {bet_team}", ha='center', fontsize=11, fontweight='bold')
            ax.text(spread_x + spread_width/2, 0.4, f"{bet_line:+.1f}", ha='center', fontsize=13, fontweight='bold', color='#2c3e50')
            edge_text = f"Edge: {abs(game['spread_diff']):.1f} pts"
            ax.text(spread_x + spread_width/2, 0.05, edge_text, ha='center', fontsize=8, color='#6c757d')
        else:
            ax.text(spread_x + spread_width/2, 0.75, "No Value", ha='center', fontsize=10, color='#6c757d')
            ax.text(spread_x + spread_width/2, 0.35, f"Book: {game['sportsbook_spread']:+.1f}", ha='center', fontsize=9, color='#adb5bd')

        # TOTAL
        total_width = 3.2
        total_x = 3.8
        if 'OVER' in game['total_recommendation']:
            total_color = '#fff3cd'
            total_border = '#ffc107'
            bet_type = "OVER"
            bet_line = game['sportsbook_total']
            strength = "STRONG" if abs(game['total_diff']) > 8 else "VALUE"
        elif 'UNDER' in game['total_recommendation']:
            total_color = '#fff3cd'
            total_border = '#ffc107'
            bet_type = "UNDER"
            bet_line = game['sportsbook_total']
            strength = "STRONG" if abs(game['total_diff']) > 8 else "VALUE"
        else:
            total_color = '#e9ecef'
            total_border = '#adb5bd'
            bet_type = "No Value"
            bet_line = None
            strength = ""

        total_box = FancyBboxPatch((total_x, 0.2), total_width, 1.6, boxstyle="round,pad=0.05",
                                  edgecolor=total_border, facecolor=total_color, linewidth=2)
        ax.add_patch(total_box)

        ax.text(total_x + total_width/2, 1.5, "TOTAL (O/U)", ha='center', fontsize=9, fontweight='bold', color='#495057')
        if bet_line is not None:
            ax.text(total_x + total_width/2, 1.1, f"{strength}", ha='center', fontsize=8,
                   color=total_border, fontweight='bold')
            ax.text(total_x + total_width/2, 0.75, f"Bet {bet_type}", ha='center', fontsize=11, fontweight='bold')
            ax.text(total_x + total_width/2, 0.4, f"{bet_line:.1f}", ha='center', fontsize=13, fontweight='bold', color='#2c3e50')
            edge_text = f"Edge: {abs(game['total_diff']):.1f} pts"
            ax.text(total_x + total_width/2, 0.05, edge_text, ha='center', fontsize=8, color='#6c757d')
        else:
            ax.text(total_x + total_width/2, 0.75, "No Value", ha='center', fontsize=10, color='#6c757d')
            ax.text(total_x + total_width/2, 0.35, f"Book: {game['sportsbook_total']:.1f}", ha='center', fontsize=9, color='#adb5bd')

        # MONEYLINE
        ml_width = 2.5
        ml_x = 7.2
        if 'HOME' in game['moneyline_recommendation']:
            ml_color = '#d1ecf1'
            ml_border = '#17a2b8'
            bet_team = game['home_short']
            bet_odds = game['home_moneyline']
            win_prob = game['home_win_prob']
            strength = "VALUE"
        elif 'AWAY' in game['moneyline_recommendation']:
            ml_color = '#d1ecf1'
            ml_border = '#17a2b8'
            bet_team = game['away_short']
            bet_odds = game['away_moneyline']
            win_prob = game['away_win_prob']
            strength = "VALUE"
        else:
            ml_color = '#e9ecef'
            ml_border = '#adb5bd'
            bet_team = "No Value"
            bet_odds = None
            win_prob = None
            strength = ""

        ml_box = FancyBboxPatch((ml_x, 0.2), ml_width, 1.6, boxstyle="round,pad=0.05",
                               edgecolor=ml_border, facecolor=ml_color, linewidth=2)
        ax.add_patch(ml_box)

        ax.text(ml_x + ml_width/2, 1.5, "MONEYLINE", ha='center', fontsize=9, fontweight='bold', color='#495057')
        if bet_odds is not None:
            ax.text(ml_x + ml_width/2, 1.1, f"{strength}", ha='center', fontsize=8,
                   color=ml_border, fontweight='bold')
            ax.text(ml_x + ml_width/2, 0.75, f"{bet_team}", ha='center', fontsize=11, fontweight='bold')
            ax.text(ml_x + ml_width/2, 0.4, f"{bet_odds:+.0f}", ha='center', fontsize=13, fontweight='bold', color='#2c3e50')
            ax.text(ml_x + ml_width/2, 0.05, f"{win_prob:.0%} win prob", ha='center', fontsize=8, color='#6c757d')
        else:
            ax.text(ml_x + ml_width/2, 0.6, "No Value", ha='center', fontsize=10, color='#6c757d')

    # Footer
    ax_footer = fig.add_subplot(gs[-1, 0])
    ax_footer.axis('off')
    footer_text = "Bet responsibly. Past performance does not guarantee future results. Edge values show point difference between our prediction and sportsbook."
    ax_footer.text(0.5, 0.5, footer_text, ha='center', va='center',
                  fontsize=8, color='#6c757d', style='italic')

    plt.savefig(os.path.join(output_dir, 'daily_betting_sheet.png'), dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved: daily_betting_sheet.png")
    plt.close()


def create_value_summary_chart(df, output_dir):
    """
    Clean horizontal bar chart showing value across all games.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.patch.set_facecolor('white')
    fig.suptitle('Value Opportunities Overview', fontsize=16, fontweight='bold', color='#2c3e50')

    # Spread value chart
    games = df['matchup']
    spread_vals = df['spread_diff']

    colors_spread = []
    for val, rec in zip(spread_vals, df['spread_recommendation']):
        if rec != 'NO VALUE':
            colors_spread.append('#28a745')
        else:
            colors_spread.append('#dee2e6')

    bars1 = ax1.barh(games, spread_vals, color=colors_spread, edgecolor='#495057', linewidth=1.5)
    ax1.axvline(x=0, color='#495057', linestyle='-', linewidth=1.5)
    ax1.axvline(x=3, color='#28a745', linestyle='--', linewidth=1, alpha=0.5, label='Value threshold')
    ax1.axvline(x=-3, color='#28a745', linestyle='--', linewidth=1, alpha=0.5)

    # Add value labels
    for i, (bar, val, rec) in enumerate(zip(bars1, spread_vals, df['spread_recommendation'])):
        if rec != 'NO VALUE':
            label = f'{abs(val):.1f} edge'
            x_pos = val + (0.5 if val > 0 else -0.5)
            ax1.text(x_pos, i, label, va='center', ha='left' if val > 0 else 'right',
                    fontweight='bold', fontsize=9, color='#28a745',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='#28a745', linewidth=1.5))

    ax1.set_xlabel('Point Difference (Our Prediction - Sportsbook)', fontweight='bold', color='#2c3e50')
    ax1.set_title('Spread Value by Game', fontweight='bold', pad=15, color='#2c3e50')
    ax1.grid(axis='x', alpha=0.3, linestyle='--')
    ax1.set_axisbelow(True)
    ax1.legend(loc='best')

    # Total value chart
    total_vals = df['total_diff']

    colors_total = []
    for val, rec in zip(total_vals, df['total_recommendation']):
        if rec != 'NO VALUE':
            colors_total.append('#ffc107')
        else:
            colors_total.append('#dee2e6')

    bars2 = ax2.barh(games, total_vals, color=colors_total, edgecolor='#495057', linewidth=1.5)
    ax2.axvline(x=0, color='#495057', linestyle='-', linewidth=1.5)
    ax2.axvline(x=5, color='#ffc107', linestyle='--', linewidth=1, alpha=0.5, label='Value threshold')
    ax2.axvline(x=-5, color='#ffc107', linestyle='--', linewidth=1, alpha=0.5)

    # Add value labels
    for i, (bar, val, rec) in enumerate(zip(bars2, total_vals, df['total_recommendation'])):
        if rec != 'NO VALUE':
            label = f'{abs(val):.1f} edge'
            x_pos = val + (0.5 if val > 0 else -0.5)
            ax2.text(x_pos, i, label, va='center', ha='left' if val > 0 else 'right',
                    fontweight='bold', fontsize=9, color='#f57c00',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='#ffc107', linewidth=1.5))

    ax2.set_xlabel('Point Difference (Sportsbook - Our Prediction)', fontweight='bold', color='#2c3e50')
    ax2.set_title('Total (Over/Under) Value by Game', fontweight='bold', pad=15, color='#2c3e50')
    ax2.grid(axis='x', alpha=0.3, linestyle='--')
    ax2.set_axisbelow(True)
    ax2.legend(loc='best')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'value_overview.png'), dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved: value_overview.png")
    plt.close()


def create_beginner_guide_cards(df, output_dir):
    """
    Create educational cards for each game with beginner-friendly explanations.
    """
    for idx, (_, game) in enumerate(df.iterrows()):
        fig = plt.figure(figsize=(14, 10))
        fig.patch.set_facecolor('#f8f9fa')

        # Title
        title = f"{game['away_team']} @ {game['home_team']}"
        fig.text(0.5, 0.95, title, ha='center', fontsize=16, fontweight='bold', color='#2c3e50')

        gs = fig.add_gridspec(4, 1, hspace=0.4, top=0.90, bottom=0.05, left=0.08, right=0.92)

        # Info section
        ax_info = fig.add_subplot(gs[0, 0])
        ax_info.axis('off')

        info_text = f"Power Ratings: {game['away_short']} ({game['away_power']:.1f}) vs {game['home_short']} ({game['home_power']:.1f})\n"
        info_text += f"Win Probabilities: {game['away_short']} {game['away_win_prob']:.1%} | {game['home_short']} {game['home_win_prob']:.1%}"

        ax_info.text(0.5, 0.5, info_text, ha='center', va='center', fontsize=11,
                    bbox=dict(boxstyle='round,pad=0.8', facecolor='white', edgecolor='#dee2e6', linewidth=2))

        # SPREAD section
        ax_spread = fig.add_subplot(gs[1, 0])
        ax_spread.axis('off')

        if 'HOME' in game['spread_recommendation']:
            bg_color = '#d4edda'
            border_color = '#28a745'
            title_color = '#155724'
            bet_team = game['home_short']
            bet_line = game['sportsbook_spread']

            explanation = f"📊 SPREAD BET RECOMMENDATION\n\n"
            explanation += f"✓ Bet {bet_team} {bet_line:+.1f}\n\n"
            explanation += f"What this means:\n"
            explanation += f"• Bet on {bet_team} with a {abs(bet_line):.1f} point cushion\n"
            explanation += f"• Sportsbook line: {game['home_short']} {game['sportsbook_spread']:+.1f}\n"
            explanation += f"• Our prediction: {game['home_short']} wins by {game['predicted_spread']:.1f}\n"
            explanation += f"• Your edge: {abs(game['spread_diff']):.1f} points\n\n"
            explanation += f"Why it's value: We predict this game to be closer than the sportsbook thinks,\n"
            explanation += f"giving {bet_team} a better chance to cover the spread."

        elif 'AWAY' in game['spread_recommendation']:
            bg_color = '#d4edda'
            border_color = '#28a745'
            title_color = '#155724'
            bet_team = game['away_short']
            bet_line = -game['sportsbook_spread']

            explanation = f"📊 SPREAD BET RECOMMENDATION\n\n"
            explanation += f"✓ Bet {bet_team} {bet_line:+.1f}\n\n"
            explanation += f"What this means:\n"
            explanation += f"• Bet on {bet_team} with a {abs(bet_line):.1f} point cushion\n"
            explanation += f"• Sportsbook line: {game['away_short']} {bet_line:+.1f} / {game['home_short']} {game['sportsbook_spread']:+.1f}\n"
            explanation += f"• Our prediction: {game['home_short']} wins by {game['predicted_spread']:.1f}\n"
            explanation += f"• Your edge: {abs(game['spread_diff']):.1f} points\n\n"
            explanation += f"Why it's value: We predict this game to be closer than the sportsbook thinks,\n"
            explanation += f"giving {bet_team} a better chance to cover the spread."

        else:
            bg_color = '#e9ecef'
            border_color = '#adb5bd'
            title_color = '#6c757d'

            explanation = f"📊 SPREAD BET\n\n"
            explanation += f"✗ No Value Found\n\n"
            explanation += f"• Sportsbook line: {game['home_short']} {game['sportsbook_spread']:+.1f}\n"
            explanation += f"• Our prediction: {game['home_short']} {game['predicted_spread']:+.1f}\n"
            explanation += f"• Difference: {abs(game['spread_diff']):.1f} points\n\n"
            explanation += f"Our prediction is too close to the sportsbook line to recommend a bet."

        ax_spread.text(0.5, 0.5, explanation, ha='center', va='center', fontsize=10, family='monospace',
                      bbox=dict(boxstyle='round,pad=1', facecolor=bg_color, edgecolor=border_color, linewidth=3))

        # TOTAL section
        ax_total = fig.add_subplot(gs[2, 0])
        ax_total.axis('off')

        if 'OVER' in game['total_recommendation']:
            bg_color = '#fff3cd'
            border_color = '#ffc107'

            explanation = f"🎯 TOTAL BET RECOMMENDATION\n\n"
            explanation += f"✓ Bet OVER {game['sportsbook_total']:.1f}\n\n"
            explanation += f"What this means:\n"
            explanation += f"• Bet that both teams will score MORE than {game['sportsbook_total']:.1f} combined points\n"
            explanation += f"• Sportsbook total: {game['sportsbook_total']:.1f}\n"
            explanation += f"• Our prediction: {game['predicted_total']:.1f} total points\n"
            explanation += f"• Your edge: {abs(game['total_diff']):.1f} points\n\n"
            explanation += f"Why it's value: We predict a higher-scoring game than the sportsbook expects."

        elif 'UNDER' in game['total_recommendation']:
            bg_color = '#fff3cd'
            border_color = '#ffc107'

            explanation = f"🎯 TOTAL BET RECOMMENDATION\n\n"
            explanation += f"✓ Bet UNDER {game['sportsbook_total']:.1f}\n\n"
            explanation += f"What this means:\n"
            explanation += f"• Bet that both teams will score LESS than {game['sportsbook_total']:.1f} combined points\n"
            explanation += f"• Sportsbook total: {game['sportsbook_total']:.1f}\n"
            explanation += f"• Our prediction: {game['predicted_total']:.1f} total points\n"
            explanation += f"• Your edge: {abs(game['total_diff']):.1f} points\n\n"
            explanation += f"Why it's value: We predict a lower-scoring game than the sportsbook expects."

        else:
            bg_color = '#e9ecef'
            border_color = '#adb5bd'

            explanation = f"🎯 TOTAL BET\n\n"
            explanation += f"✗ No Value Found\n\n"
            explanation += f"• Sportsbook total: {game['sportsbook_total']:.1f}\n"
            explanation += f"• Our prediction: {game['predicted_total']:.1f}\n"
            explanation += f"• Difference: {abs(game['total_diff']):.1f} points\n\n"
            explanation += f"Our prediction is too close to the sportsbook line to recommend a bet."

        ax_total.text(0.5, 0.5, explanation, ha='center', va='center', fontsize=10, family='monospace',
                     bbox=dict(boxstyle='round,pad=1', facecolor=bg_color, edgecolor=border_color, linewidth=3))

        # MONEYLINE section
        ax_ml = fig.add_subplot(gs[3, 0])
        ax_ml.axis('off')

        if game['moneyline_recommendation'] != 'NO VALUE':
            bg_color = '#d1ecf1'
            border_color = '#17a2b8'

            if 'HOME' in game['moneyline_recommendation']:
                bet_team = game['home_short']
                odds = game['home_moneyline']
                prob = game['home_win_prob']
            else:
                bet_team = game['away_short']
                odds = game['away_moneyline']
                prob = game['away_win_prob']

            if odds > 0:
                payout = f"${100 + odds} on a $100 bet"
            else:
                payout = f"${100 + (100 * 100 / abs(odds)):.0f} on a ${abs(odds):.0f} bet"

            explanation = f"💰 MONEYLINE BET RECOMMENDATION\n\n"
            explanation += f"✓ Bet {bet_team} to win straight up ({odds:+.0f})\n\n"
            explanation += f"What this means:\n"
            explanation += f"• Simple bet: {bet_team} just needs to WIN the game\n"
            explanation += f"• Odds: {odds:+.0f} (payout: {payout})\n"
            explanation += f"• Our win probability: {prob:.1%}\n\n"
            explanation += f"Why it's value: Our model gives {bet_team} a better chance to win\n"
            explanation += f"than what the odds suggest, making this a value bet."

        else:
            bg_color = '#e9ecef'
            border_color = '#adb5bd'

            explanation = f"💰 MONEYLINE BET\n\n"
            explanation += f"✗ No Value Found\n\n"
            explanation += f"• {game['away_short']}: {game['away_moneyline']:+.0f} ({game['away_win_prob']:.1%} win prob)\n"
            explanation += f"• {game['home_short']}: {game['home_moneyline']:+.0f} ({game['home_win_prob']:.1%} win prob)\n\n"
            explanation += f"The odds accurately reflect the win probabilities."

        ax_ml.text(0.5, 0.5, explanation, ha='center', va='center', fontsize=10, family='monospace',
                  bbox=dict(boxstyle='round,pad=1', facecolor=bg_color, edgecolor=border_color, linewidth=3))

        filename = f"beginner_guide_{game['away_short']}_at_{game['home_short']}.png"
        plt.savefig(os.path.join(output_dir, filename), dpi=300, bbox_inches='tight', facecolor='#f8f9fa')
        print(f"Saved: {filename}")
        plt.close()


def main():
    """Main execution function"""

    print("\n" + "="*80)
    print("PROFESSIONAL BETTING VALUE VISUALIZATIONS")
    print("="*80 + "\n")

    # Set up paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(pipeline_dir, "data")
    output_dir = script_dir

    print(f"Data directory: {data_dir}")
    print(f"Output directory: {output_dir}\n")

    # Load data
    print("Loading ponderations data...")
    df = load_data(data_dir)

    # Generate visualizations
    print("\nGenerating professional visualizations...\n")

    print("1. Creating daily betting sheet...")
    create_daily_betting_sheet(df, output_dir)

    print("2. Creating value overview charts...")
    create_value_summary_chart(df, output_dir)

    print("3. Creating beginner-friendly game guides...")
    create_beginner_guide_cards(df, output_dir)

    print("\n" + "="*80)
    print("VISUALIZATION COMPLETE!")
    print(f"All visualizations saved to: {output_dir}")
    print("="*80 + "\n")

    # Print summary
    value_games = df[
        (df['spread_recommendation'] != 'NO VALUE') |
        (df['total_recommendation'] != 'NO VALUE') |
        (df['moneyline_recommendation'] != 'NO VALUE')
    ]

    print(f"Summary:")
    print(f"  - {len(df)} total games analyzed")
    print(f"  - {len(value_games)} games with betting value")
    print(f"  - {len(df[df['spread_recommendation'] != 'NO VALUE'])} spread opportunities")
    print(f"  - {len(df[df['total_recommendation'] != 'NO VALUE'])} total opportunities")
    print(f"  - {len(df[df['moneyline_recommendation'] != 'NO VALUE'])} moneyline opportunities")
    print()


if __name__ == "__main__":
    main()
