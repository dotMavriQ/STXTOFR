# Provider Strategies

## Circle K

- classification: API-possible but not confirmed
- primary approach: structured station-search response exposed behind the public site
- fallback: archive parser failure and keep list-only records when detail fragments are missing

## Rasta

- classification: scrape-only
- primary approach: listing page plus facility detail pages
- fallback: partial record with low confidence when coordinates or address fields are missing

## IDS

- classification: hybrid
- primary approach: `stations.json` feed with detail page enrichment for opening hours
- fallback: feed-only normalization if detail fetch fails

## Preem

- classification: API-native
- primary approach: station API endpoint with nested fuels and services
- fallback: unfiltered station pull with mapper defaults

## Espresso House

- classification: API-native
- primary approach: coffee shop JSON endpoint
- fallback: mapper-side Sweden filtering if upstream country filtering shifts

## Trafikverket Parking

- classification: API-native
- primary approach: Trafikverket query API with raw response archival
- fallback: fail fast and retain raw response metadata for replay

## TRB

- classification: hybrid
- primary approach: WordPress AJAX JSON response with HTML cleanup during normalization
- fallback: partial JSON normalization with extraction warnings

