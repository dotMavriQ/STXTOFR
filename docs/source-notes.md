# Source Integration Notes

STXTOFR uses the strongest available integration path for each provider. Where a free structured API or feed exists it is preferred over HTML extraction. Scrape-backed adapters carry lower confidence scores in the normalisation output to reflect the inherent instability of unstructured sources.

Raw payloads are archived for every fetch regardless of integration type, so normalisation can be rerun from the archive without a live network call.

| Provider | Integration type | Primary source endpoint |
| --- | --- | --- |
| Circle K | HTTP scrape | Station search page; structured JSON payload embedded in response |
| Rasta | HTML scrape | Listing page plus per-facility detail pages |
| IDS | JSON feed + scrape | `stations.json` with detail page enrichment for opening hours |
| Preem | REST API | Station endpoint with nested fuels and services |
| Espresso House | REST API | Coffee shop JSON endpoint |
| Trafikverket | REST API | Trafikverket query API (`api.trafikinfo.trafikverket.se`) |
| TRB | JSONP widget feed | Store locator widget bootstrap feed on `cdn.storelocatorwidgets.com` |

## Parser Strategy

Each provider adapter implements two methods: `fetch` and `normalize`. The `fetch` method handles transport and archives the raw response. The `normalize` method maps the provider-specific payload shape to the shared `NormalizedFacility` model. The two are kept separate so the archive can be replayed against updated normalisation logic without repeating network calls.

HTML-backed adapters use BeautifulSoup for extraction. JSON-backed adapters use direct dict access with mapper defaults for missing fields. Both emit `NormalizationIssue` records for fields that cannot be recovered cleanly, which are stored separately from the normalised output and exposed through `GET /issues`.

## Confidence Scoring

Each normalised facility carries a `confidence_score` between 0 and 1. Scores are set per adapter and adjusted down when coordinates, address, or identity fields are missing or derived from imprecise sources. Scrape-backed adapters start at a lower baseline than API-native adapters.
