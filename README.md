# FMCSA Fleet Inspection Lead Generator

Automated daily pipeline that pulls DOT/FMCSA inspection data, identifies trucking carriers with **out-of-service (OOS) violations** and **20+ power units**, and publishes a downloadable CSV to a clean static website via GitHub Pages.

---

## What It Does

Every day at 12:00 PM EST, this pipeline:

1. **Queries the FMCSA Open Data Portal** for inspections from the previous day
2. **Filters for out-of-service violations** (vehicles/drivers pulled off the road for serious safety defects)
3. **Cross-references the Company Census** to find carriers with 20+ power units
4. **Outputs a CSV** with company name, DOT number, phone, address, fleet size, violation details, and a direct link to the carrier's SAFER snapshot
5. **Publishes to a website** where you can click a button and download the file

---

## Output CSV Columns

| Column | Description |
|---|---|
| `dot_number` | Carrier's unique USDOT number |
| `legal_name` | Registered company name |
| `dba_name` | Doing-business-as name (if any) |
| `phone` | Company phone number |
| `email` | Company email (if on file) |
| `physical_address/city/state/zip` | Company physical location |
| `mailing_address/city/state/zip` | Company mailing address |
| `power_units` | Number of registered power units |
| `drivers` | Number of registered drivers |
| `carrier_operation` | Interstate/intrastate, for-hire/private |
| `inspection_id` | Unique inspection identifier |
| `inspection_date` | Date of the inspection |
| `inspection_state` | State where inspection occurred |
| `inspection_level` | Inspection level (I–VI) |
| `oos_violation_count` | Number of OOS violations in this inspection |
| `violation_codes` | Semicolon-separated FMCSR violation codes |
| `violation_descriptions` | Brief descriptions of violations |
| `safer_link` | Direct URL to carrier's SAFER snapshot |

---

## Setup Guide (Step-by-Step)

### Prerequisites

- A GitHub account (free tier works fine)
- Python 3.9+ (only needed if you want to run locally)
- Optionally, a [Socrata app token](https://data.transportation.gov/profile/edit/developer_settings) (free — reduces API throttling)

### Step 1: Create the GitHub Repository

1. Go to [github.com/new](https://github.com/new)
2. Name it something like `fmcsa-leads`
3. Set it to **Private** (this is your sales data — keep it private)
4. Do **not** initialize with a README (we'll push our own)
5. Click **Create repository**

### Step 2: Push This Code to Your Repo

On your local machine, open a terminal:

```bash
# Clone or copy this project folder, then:
cd fmcsa-leads

git init
git add .
git commit -m "Initial commit — FMCSA lead pipeline"

# Replace with YOUR repo URL from Step 1:
git remote add origin https://github.com/YOUR_USERNAME/fmcsa-leads.git
git branch -M main
git push -u origin main
```

### Step 3: Enable GitHub Pages

1. In your repo, go to **Settings → Pages**
2. Under **Source**, select **Deploy from a branch**
3. Set the branch to `main` and the folder to `/docs`
4. Click **Save**
5. After a minute, your site will be live at:
   `https://YOUR_USERNAME.github.io/fmcsa-leads/`

> **Note:** Since your repo is private, only people with repo access can see the GitHub Pages site. If you want the site publicly accessible but the code private, you'll need GitHub Pro/Team or can deploy to a different host.

### Step 4: (Optional) Add a Socrata App Token

A Socrata app token is free and prevents your API requests from being throttled. Without one, you're limited to ~1,000 requests per hour per IP; with one, you get 10,000+.

1. Go to [data.transportation.gov/profile/edit/developer_settings](https://data.transportation.gov/profile/edit/developer_settings)
2. Create an account if needed, then generate an **App Token**
3. In your GitHub repo, go to **Settings → Secrets and variables → Actions**
4. Click **New repository secret**
5. Name: `SOCRATA_APP_TOKEN`
6. Value: paste your app token
7. Click **Add secret**

### Step 5: Run It for the First Time

1. Go to the **Actions** tab in your repo
2. Click **Daily FMCSA Lead Fetch** in the left sidebar
3. Click **Run workflow** (top right)
4. Leave the defaults (or enter a specific date / days-back value)
5. Click the green **Run workflow** button
6. Wait 2–5 minutes for it to complete

Once it finishes, your website will show the latest data and you can download the CSV.

### Step 6: Verify the Automated Schedule

The workflow is set to run automatically every day at **5:00 PM UTC** (12:00 PM EST / 1:00 PM EDT). After 24 hours, check the Actions tab to confirm it ran successfully.

> **Tip:** GitHub may delay scheduled workflows by up to 15 minutes, and they can be skipped entirely if the repo has been inactive for 60+ days. To prevent this, the manual trigger option is always available as a backup.

---

## Running Locally

If you want to test or run the script on your own machine:

```bash
# Install dependencies
pip install -r requirements.txt

# Pull yesterday's data
python scripts/fetch_inspections.py

# Pull a specific date
python scripts/fetch_inspections.py --date 2026-03-25

# Pull the last 7 days
python scripts/fetch_inspections.py --days-back 7

# With a Socrata app token
python scripts/fetch_inspections.py --app-token YOUR_TOKEN_HERE
```

Output will be in `docs/data/`.

---

## Adjusting the Filters

To change the **minimum power units**, edit `MIN_POWER_UNITS` in `scripts/fetch_inspections.py` (default is 20).

To change the **schedule**, edit the cron expression in `.github/workflows/daily_fetch.yml`. Some useful alternatives:

```yaml
# Twice daily (morning and afternoon EST):
- cron: "0 14,19 * * *"

# Weekdays only at noon EST:
- cron: "0 17 * * 1-5"

# Every 6 hours:
- cron: "0 */6 * * *"
```

---

## How the Data Works (Background)

- **Source:** FMCSA Open Data Portal at data.transportation.gov
- **Refresh rate:** Daily from a 24-hour-old database, typically available by 12:00 PM EST
- **"Failed inspection":** There's no pass/fail flag. The meaningful signal is **out-of-service (OOS)** — a violation severe enough that the vehicle or driver was pulled from the road. This is what we filter on.
- **Coverage:** 3 years of historical data is available for backfilling
- **Privacy:** Driver information is not included in public data (per federal privacy rules)

### Data Pipeline Diagram

```
data.transportation.gov
├── Vehicle Inspection File (fx4q-ay7w)     ─┐
├── Vehicle Inspections & Violations (niy2-gm2b) ─┤──→ Python script ──→ CSV ──→ GitHub Pages
└── Company Census File (az4p-a2qs)          ─┘
```

---

## Backfilling Historical Data

To generate a lead list from the past week (good for initial outreach):

```bash
python scripts/fetch_inspections.py --days-back 7
```

For the past 30 days:

```bash
python scripts/fetch_inspections.py --days-back 30
```

> **Note:** Pulling large date ranges may take several minutes due to API pagination. A 30-day pull will likely return thousands of inspection records.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| "0 leads found" | Normal for some days — not every day produces OOS violations at 20+ unit carriers. Try `--days-back 7` to verify. |
| API throttling / slow | Add a Socrata app token (Step 4 above). |
| GitHub Action not running on schedule | Repos inactive for 60+ days may have Actions paused. Push a commit or run manually to reactivate. |
| CSV is empty | Check the Actions log for errors. The most common issue is the Socrata API being temporarily unavailable. |
| Website not updating | Make sure GitHub Pages is set to deploy from `main` branch, `/docs` folder. Clear your browser cache. |

---

## License

This project uses publicly available U.S. government data from the FMCSA Open Data Program. The data itself is in the public domain. This code is provided as-is for your use.
