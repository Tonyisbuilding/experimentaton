# Google Maps Lead Scraper

A hardened Playwright-powered scraper that combines ultra-specific locations with a set of
keywords to capture company names and first-party website URLs from Google Maps while avoiding
cross-lead website contamination.

## Features

- Cartesian search over any list of keywords × micro-locations.
- Batch harvesting in blocks of fifty leads with an operator-controlled pause between batches.
- Domain-level deduplication so a website never appears on more than one lead.
- Aggregator/directory filter to keep results focused on genuine company domains.
- Persistent Playwright profile to reduce repeat consent prompts and provide stable behaviour.
- Optional push of each 50-lead batch to an Apps Script webhook that updates your Google Sheet in
  real time.

## Getting Started

1. **Install dependencies**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   playwright install chromium
   ```

2. **Adjust keyword/location inputs**

   Update `config.py` with your micro-locations and keyword list. Each run iterates every
   combination.

3. **Run the scraper**

   ```bash
   python main.py            # launches a visible browser window
   python main.py --headless # optional headless automation
   ```

   The scraper writes batches to `output/leads_batch_XXX.csv`, maintains a consolidated
   `output/leads_full.csv`, sends each batch to the configured Apps Script webhook, and prompts you
   before proceeding to the next batch of fifty unique leads.

## Output Schema

Each CSV row contains:

| column    | description                                        |
|-----------|----------------------------------------------------|
| name      | Business name as shown in Google Maps               |
| website   | Canonical website URL (blank if none detected)      |
| location  | Location string used for the query                  |
| keyword   | Keyword used for the query                          |
| place_url | Google Maps place URL (useful for auditing)         |

## Best Practices

- Run with a visible browser (`python main.py`) when fine-tuning selectors—headless should only be
  used once everything is stable.
- If Google presents a captcha, the scraper aborts to avoid lockouts; wait before retrying.
- Maintain humane pacing by honouring the built-in batch pauses. Increase delays in `config.py` if
  you expand the search footprint.

## Extending

- Add new aggregators to `AGGREGATOR_DOMAINS` if you see unwanted directory results.
- Persist past batches if you want to resume runs later—consider checkpointing `seen_domains`.
- Slot an enrichment step after `export_current_batch` once high-quality lead capture is proven.
# new-scraper
