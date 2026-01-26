# Data Aggregator Agent

Automatically collects and consolidates data from 10 Google Sheets into one master sheet.

## Setup Instructions

### 1. Configure Source Sheets

Edit `source_sheets.json` and add your 10 Google Sheet IDs:

```json
{
    "sheets": [
        {
            "id": "YOUR_SHEET_1_ID",
            "name": "Sheet 1 Name",
            "sheetName": "Sheet1"
        },
        // ... add all 10 sheets
    ]
}
```

**Note:** Make sure the service account email has read access to all source sheets:
- `link-scraper@link-scraper-482105.iam.gserviceaccount.com`

### 2. Share Master Sheet

Share your master sheet (ID: `1yq2UwI94lwfYPY86CFwGbBsm3kpdqKrefYgrw3lEAwk`) with the service account email and give it **Editor** access.

### 3. Run Locally

```bash
npm run aggregate
```

## GitHub Actions Automation

The workflow runs **twice daily**:
- 6:00 AM Pakistan Time
- 6:00 PM Pakistan Time

### Setup GitHub Secrets

1. Go to your GitHub repo → Settings → Secrets and variables → Actions
2. Add these secrets:

#### `GOOGLE_CREDENTIALS`
Copy the entire content of `credentials.json`:
```json
{
  "type": "service_account",
  "project_id": "link-scraper-482105",
  ...
}
```

#### `SOURCE_SHEETS_CONFIG` (Optional)
If you want to keep source sheets config in GitHub secrets instead of the repo, paste the entire `source_sheets.json` content.

### Manual Trigger

You can also trigger the workflow manually from GitHub Actions tab.

## How It Works

1. **Reads** data from all 10 configured source sheets
2. **Filters** rows where App Link (Column C) is NOT "NOT_FOUND"
3. **Cleans** the data (removes special characters, normalizes whitespace)
4. **Deduplicates** based on Ads URL (Column B)
5. **Appends** new rows to master sheet

## Master Sheet Structure

| Column | Data |
|--------|------|
| A | Advertiser Name |
| B | Ads URL |
| C | App Link |
| D | App Name |
| E | Video ID |
| F | Source Sheet |
| G | Date Added |

## Files

- `data_aggregator.js` - Main aggregation script
- `source_sheets.json` - Source sheets configuration
- `.github/workflows/data-aggregator.yml` - GitHub Actions workflow
