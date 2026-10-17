# Signalpost agent

Takes Norwegian organisation numbers and returns company facts with source links and dates.

## Run
```bash
pip install -r requirements.txt
export GEMINI_API_KEY=...   # optional: enables website description/products
python signalpost.py run --input orgnrs.txt --out profiles/
```

## Sources
- Brønnøysund Register Centre open APIs (Enhetsregisteret, Regnskapsregisteret)
- The company's own website (URL taken from the registry), robots.txt respected

## Models / APIs
- Google Gemini Flash (free tier, model set via GEMINI_MODEL), optional fallback Mistral (free mode)
- LLM is used only for website description/products, on pages verified to belong to the company

## Matching
Every record is checked against the org number. Website pages are used only if they contain the
org number or exact legal name. Unverified pages produce no facts.

## Updates
Re-running keeps timestamps of unchanged facts and writes a `changes` list for changed ones.

## Expected run cost
Registry APIs: free. About 1 LLM call per company on a free tier: $0 (subject to daily limits).
