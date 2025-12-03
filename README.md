# 🎲 Turbobet Pipeline

Sports betting data pipeline for collecting odds, team statistics, and predicting outcomes.

## Features

- 🏈 **NFL betting odds** (spread, moneyline, total)
- 🏀 **NBA betting odds** (spread, moneyline, total)  
- ⚾ **MLB betting odds** (spread, moneyline, total)
- 📊 **NBA team statistics** (62+ metrics including opponent stats and rest days)
- 🤖 **Odds prediction algorithm** (compare Vegas odds to data-driven predictions)

## 📚 Quick Links

- **[RUN_INSTRUCTIONS.md](RUN_INSTRUCTIONS.md)** - Detailed step-by-step execution guide
- **[pipeline/models/README.md](pipeline/models/README.md)** - Algorithm explanation

## Quick Setup (First Time Only)

```bash
# 1. Create virtual environment
python3 -m venv venv

# 2. Activate it
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate     # Windows

# 3. Install dependencies  
pip install -r requirements.txt
```

## How to Run (After Installation)

**Important:** Each script must be run from its specific directory (see table below)

| Step | Script | Run From | Command |
|------|--------|----------|---------|
| 1 | `nba_scraper.py` | `pipeline/scrapers/` | `python3 nba_scraper.py` |
| 2 | `fetch_odds.py` | `pipeline/scrapers/` | `python3 fetch_odds.py --auto` |
| 3 | `simple_predict.py` | `pipeline/models/` | `python3 simple_predict.py` |
| 4 | `test_prediction.py` | **project root** | `python3 test_prediction.py` |
| 5 | `visualize_predictions.py` | `pipeline/models/` | `python3 visualize_predictions.py` |

### Step-by-Step Execution Order

```bash
# Steps 1 & 2: From scrapers directory
cd /Users/bernardodelrio/Desktop/turbobet/pipeline/scrapers

# 1. SCRAPE NBA TEAM STATISTICS (required first)
#    Duration: ~60-90 seconds
#    Output: pipeline/data/nba_team_stats_2025-26.csv
python3 nba_scraper.py

# 2. SCRAPE BETTING ODDS (required second)
#    Duration: ~10-15 seconds
#    Output: pipeline/data/nba_odds_*.csv, nfl_odds_*.csv, mlb_odds_*.csv
python3 fetch_odds.py --auto

# Step 3: From models directory
cd /Users/bernardodelrio/Desktop/turbobet/pipeline/models

# 3. PREDICT & COMPARE ODDS (requires steps 1 & 2)
#    Duration: ~2 seconds
#    Output: pipeline/data/odds_prediction_*.csv
python3 simple_predict.py

# Step 4: From project root
cd /Users/bernardodelrio/Desktop/turbobet

# 4. RUN TEST PREDICTION (requires step 3)
#    Duration: ~2 seconds
python3 test_prediction.py

# Step 5: From models directory
cd /Users/bernardodelrio/Desktop/turbobet/pipeline/models

# 5. CREATE VISUALIZATIONS (requires step 4)
#    Duration: ~5 seconds
#    Output: 5 PNG files in pipeline/data/
python3 visualize_predictions.py
```

### 🚀 Single Command (Run Everything)

**Option 0: Fresh Start (no CSVs or PNGs yet)**:

```bash
cd /Users/bernardodelrio/Desktop/turboBet/pipeline/scrapers && python3 fetch_teams.py && python3 fetch_odds.py --auto && cd ../models && python3 calculate_ponderations.py && cd ../bets && python3 visualize_bet_values.py && cd ../..
```

**Option 1: One-Liner** (copy-paste entire line):

```bash
cd /Users/bernardodelrio/Desktop/turbobet/pipeline/scrapers && python3 nba_scraper.py && python3 fetch_odds.py --auto && cd ../models && python3 simple_predict.py && cd ../.. && python3 test_prediction.py && cd pipeline/models && python3 visualize_predictions.py && cd ../.. && open pipeline/data/*.png
```

**Option 2: Shell Script**:

```bash
bash /Users/bernardodelrio/Desktop/turbobet/run_all.sh
```

**Note:** This is the complete daily workflow. Team stats update after every game, so running the full pipeline daily ensures maximum accuracy.

### Individual Script Options

**NBA Stats Scraper** (from `pipeline/scrapers/`):
```bash
python3 nba_scraper.py        # All 30 teams
python3 nba_scraper.py test   # Test with 1 team (fast)
```

**Odds Scraper** (from `pipeline/scrapers/`):
```bash
python3 fetch_odds.py --auto   # Auto-find next games
python3 fetch_odds.py          # Today's games
python3 fetch_odds.py --days=1 # Tomorrow's games
python3 fetch_odds.py --debug  # With debug output
```

**Prediction** (from `pipeline/models/`):
```bash
python3 simple_predict.py        # Pandas version (recommended)
python3 predict_odds.py          # PySpark version (requires Java)
```

**Test Prediction** (from project root):
```bash
python3 test_prediction.py       # Wrapper to run predictions
```

**Visualization** (from `pipeline/models/`):
```bash
python3 visualize_predictions.py # Creates 5 separate PNG charts
```

### View Results

```bash
# From project root
cd /Users/bernardodelrio/Desktop/turbobet

# View all visualizations (macOS)
open pipeline/data/*.png

# Or view CSV files
open pipeline/data/odds_prediction_*.csv
open pipeline/data/nba_team_stats_2025-26.csv
```

## Output Files

All outputs are saved to `pipeline/data/`:

**Data Files:**
- `nba_team_stats_2025-26.csv` - All 30 NBA teams' statistics
- `nba_odds_*.csv` - NBA betting odds
- `nfl_odds_*.csv` - NFL betting odds
- `mlb_odds_*.csv` - MLB betting odds
- `odds_prediction_*.csv` - Predicted vs actual odds comparison

**Visualization Files:**
- `1_best_betting_opportunities.png` - Top value bets ranked
- `2_probability_comparison.png` - Vegas vs model probabilities
- `3_edge_distribution.png` - Edge histogram
- `4_spread_comparison.png` - Prediction accuracy scatter plot
- `5_best_bets_table.png` - Summary table with recommendations

## What the Prediction Algorithm Does

The algorithm:
1. Loads NBA team stats (offense, defense, win %, etc.)
2. Calculates team strength ratings
3. Predicts spread, moneyline, and totals for each matchup
4. Compares predictions to actual Vegas odds
5. Identifies games where predictions differ most (potential value)

**Example Output:**
```
Matchup                                    Actual    Predicted    Diff
Detroit Pistons @ Atlanta Hawks             -9.5       -8.2      1.3
Indiana Pacers @ Cleveland Cavaliers         4.5        5.1      0.6

Average Errors:
  Spread: ±2.3 points
  Total:  ±4.1 points
```

## Requirements

- Python 3.9.6+
- Internet connection
- ~50MB for dependencies

## Project Structure

```
turbobet/                           ← Project root
├── test_prediction.py              # Test wrapper (run from root)
├── pipeline/
│   ├── data/                       # All outputs (CSVs & PNGs)
│   │   ├── nba_team_stats_*.csv
│   │   ├── nba_odds_*.csv
│   │   ├── nfl_odds_*.csv
│   │   ├── mlb_odds_*.csv
│   │   ├── odds_prediction_*.csv
│   │   ├── 1_best_betting_opportunities.png
│   │   ├── 2_probability_comparison.png
│   │   ├── 3_edge_distribution.png
│   │   ├── 4_spread_comparison.png
│   │   └── 5_best_bets_table.png
│   ├── scrapers/                   # Data collection (run from here)
│   │   ├── fetch_odds.py          # Scrape betting odds
│   │   └── nba_scraper.py         # Scrape NBA stats
│   └── models/                     # Prediction & viz (run from here)
│       ├── simple_predict.py      # Predict odds (pandas)
│       ├── predict_odds.py        # Predict odds (PySpark)
│       ├── visualize_predictions.py # Create charts
│       └── README.md              # Algorithm docs
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── RUN_INSTRUCTIONS.md             # Detailed execution guide
└── .gitignore                      # Git ignore rules
```

## Data Sources

- **Betting Odds:** ESPN Sports Core API
- **NBA Statistics:** NBA.com Official Stats API

## License

Private project for data analysis and research purposes.

