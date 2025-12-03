# PySpark Installation and Usage Guide

This guide provides step-by-step instructions to install and run the PySpark-based odds ponderation script.

## Prerequisites

- **Python 3.8+** (recommended: Python 3.9 or 3.10)
- **Java 8 or Java 11** (required by PySpark)

---

## Installation Steps

### Step 1: Install Java

PySpark requires Java to run. Check if Java is installed:

```bash
java -version
```

#### If Java is NOT installed:

**On macOS (using Homebrew):**
```bash
brew install openjdk@11
```

After installation, add to your PATH:
```bash
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

**On Windows:**
1. Download Java 11 from [Adoptium](https://adoptium.net/)
2. Install and set JAVA_HOME environment variable
3. Add Java to PATH

Verify installation:
```bash
java -version
```

---

### Step 2: Set Up Python Virtual Environment (Recommended)

Navigate to your project directory:
```bash
cd /Users/bernardodelrio/Desktop/turboBet
```

Create and activate a virtual environment:

**On macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**On Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate
```

---

### Step 3: Install PySpark and Dependencies

With your virtual environment activated, install PySpark:

```bash
pip install pyspark
```

Install other project dependencies:
```bash
pip install pandas requests
```

Verify PySpark installation:
```bash
python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully!')"
```

---

## Running the Odds Ponderation Script

### Step 1: Scrape Today's Data

Before running the ponderation script, ensure you have the latest data:

```bash
# Scrape team stats (if not already done for 2025-26 season)
python3 pipeline/scrapers/fetch_teams.py

# Scrape today's odds
python3 pipeline/scrapers/fetch_odds.py
```

This will generate:
- `pipeline/data/nba_team_stats_2025-26.csv` - Team statistics
- `pipeline/data/nba_odds_YYYYMMDD_HHMM.csv` - Today's odds

---

### Step 2: Run the Ponderation Calculator

```bash
python3 pipeline/models/calculate_ponderations.py
```

**Expected Output:**

```
================================================================================
NBA ODDS PONDERATION CALCULATOR
================================================================================

Data directory: /Users/bernardodelrio/Desktop/turboBet/pipeline/data

Step 1: Loading data...
Loaded team stats: 30 teams
Loaded odds from: nba_odds_20251202_2301.csv (6 games)

Step 2: Calculating team power ratings...

Top 5 Teams by Power Rating:
+------------------+------------+
|TEAM_NAME         |power_rating|
+------------------+------------+
|Boston Celtics    |115.3       |
|Golden State      |112.8       |
|...               |...         |
+------------------+------------+

Step 3: Calculating matchup predictions...

Step 4: Saving ponderations...

================================================================================
Ponderations saved to: pipeline/data/ponderations_for_today.csv
================================================================================

BETTING RECOMMENDATIONS SUMMARY
================================================================================

Washington Wizards @ Philadelphia 76ers
  Predicted spread: -11.2 | Sportsbook: -13.5
  Spread: BET HOME TEAM (better than spread)
  Total: NO VALUE
  Moneyline: NO VALUE

...
================================================================================
FOUND 3 GAMES WITH POTENTIAL VALUE BETS
================================================================================
```

---

### Step 3: View Results

The output file `ponderations_for_today.csv` will be created in `pipeline/data/` with the following columns:

| Column | Description |
|--------|-------------|
| `sport` | Sport type (NBA) |
| `away_team` | Away team name |
| `home_team` | Home team name |
| `sportsbook_spread` | DraftKings spread |
| `away_moneyline` | DraftKings away moneyline |
| `home_moneyline` | DraftKings home moneyline |
| `sportsbook_total` | DraftKings over/under |
| `predicted_spread` | Model's predicted spread |
| `predicted_away_ml` | Model's predicted away moneyline |
| `predicted_home_ml` | Model's predicted home moneyline |
| `predicted_total` | Model's predicted total points |
| `away_win_prob` | Probability away team wins (0-1) |
| `home_win_prob` | Probability home team wins (0-1) |
| `away_power` | Away team power rating |
| `home_power` | Home team power rating |
| `spread_diff` | Difference between sportsbook and predicted spread |
| `total_diff` | Difference between sportsbook and predicted total |
| `spread_recommendation` | Betting advice for spread |
| `total_recommendation` | Betting advice for over/under |
| `moneyline_recommendation` | Betting advice for moneyline |

Open the CSV file:
```bash
# View in terminal
cat pipeline/data/ponderations_for_today.csv

# Or open in Excel/Numbers/Google Sheets
```

---

## Troubleshooting

### Issue: "JAVA_HOME is not set"

**Solution:**
Find your Java installation path:
```bash
# macOS/Linux
which java
/usr/libexec/java_home
```

Set JAVA_HOME:
```bash
# macOS (add to ~/.zshrc)
export JAVA_HOME=$(/usr/libexec/java_home)

# Linux (add to ~/.bashrc)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Reload shell:
```bash
source ~/.zshrc  # or source ~/.bashrc
```

---

### Issue: "No module named 'pyspark'"

**Solution:**
Ensure your virtual environment is activated and install PySpark:
```bash
source .venv/bin/activate
pip install pyspark
```

---

### Issue: "No odds file found"

**Solution:**
Run the odds scraper first:
```bash
python3 pipeline/scrapers/fetch_odds.py
```

---

### Issue: PySpark runs slowly or uses too much memory

**Solution:**
The script is configured with 4GB driver memory. For less powerful machines, edit line 20 in `calculate_ponderations.py`:

```python
.config("spark.driver.memory", "2g")  # Reduce from 4g to 2g
```

---

## Understanding the Output

### Power Ratings
- Higher power rating = stronger team
- Combines win%, points differential, offensive/defensive efficiency
- Typical range: 85-115

### Spread Difference
- **Positive value**: Sportsbook favors home team MORE than model predicts → Consider betting away team
- **Negative value**: Sportsbook favors home team LESS than model predicts → Consider betting home team
- **Threshold**: ±3 points for recommendations

### Total Difference
- **Positive value**: Sportsbook total is HIGHER than predicted → Consider betting UNDER
- **Negative value**: Sportsbook total is LOWER than predicted → Consider betting OVER
- **Threshold**: ±5 points for recommendations

### Win Probability
- Converted to American odds format
- Values closer to 1.0 = higher confidence
- Used to identify moneyline value (especially for underdogs)

---

## Daily Workflow

1. **Morning** - Scrape fresh data:
   ```bash
   python3 pipeline/scrapers/fetch_odds.py
   ```

2. **Run Analysis**:
   ```bash
   python3 pipeline/models/calculate_ponderations.py
   ```

3. **Review Results**:
   ```bash
   open pipeline/data/ponderations_for_today.csv
   ```

4. **Look for**:
   - Games with "BET HOME/AWAY TEAM" recommendations
   - Large spread_diff or total_diff values
   - High win probability underdogs (value moneylines)

---

## Advanced Options

### Adjust Home Court Advantage

Edit line 145 in `calculate_ponderations.py`:
```python
HOME_COURT_ADVANTAGE = 3.0  # Default is 3 points, adjust as needed
```

### Modify Value Thresholds

Edit lines 213-222 to change when recommendations trigger:
```python
# Current thresholds:
# Spread: ±3 points
# Total: ±5 points
```

### Include Days Rest Analysis

The script already loads rest day performance. To incorporate it into predictions, you can weight recent rest day performance more heavily by modifying the power rating calculation (lines 90-105).

---

## Notes

- **Disclaimer**: This tool is for educational purposes. Always gamble responsibly.
- **Data Quality**: Predictions are only as good as the input data. Ensure scrapers run successfully.
- **Updates**: Re-run scrapers daily for most accurate odds and team stats.
- **Validation**: Compare predictions against actual outcomes to improve the model over time.

---

## Quick Reference Commands

```bash
# Activate environment
source .venv/bin/activate

# Scrape data
python3 pipeline/scrapers/fetch_teams.py       # Once per season
python3 pipeline/scrapers/fetch_odds.py        # Daily

# Generate ponderations
python3 pipeline/models/calculate_ponderations.py

# View results
cat pipeline/data/ponderations_for_today.csv
```
