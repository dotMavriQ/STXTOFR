# Source Notes

The legacy archive showed that the earlier project called everything a scraper even when upstream data came from structured endpoints. This rebuild keeps the source coverage but uses the strongest integration path available per provider.

- Circle K: frontend-backed structured station search payload
- Rasta: HTML pages with service icon extraction
- IDS: `stations.json` plus station detail pages
- Preem: station API endpoint
- Espresso House: coffee shop API
- Trafikverket: official data API
- TRB: WordPress AJAX endpoint returning JSON with embedded HTML fragments

