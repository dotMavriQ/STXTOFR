# Provider Strategies

## Circle K

- classification: scrape-backed
- primary approach: station search page returns a structured JSON payload in the HTML response body; the adapter extracts this directly without a formal API contract
- fallback: archive the parse failure and retain partial records when the payload shape shifts

## Rasta

- classification: scrape
- primary approach: listing page for the station index, detail pages for address, phone, hours, and service data
- fallback: partial record with a low confidence score when coordinates or address fields cannot be extracted from the detail page

## IDS

- classification: feed + scrape
- primary approach: `stations.json` feed for station identity and coordinates, with detail page enrichment to recover opening hours
- fallback: feed-only normalisation if the detail fetch fails; opening hours left null

## Preem

- classification: REST API
- primary approach: station API endpoint returns nested station records with fuels, services, and address fields
- fallback: unfiltered station pull with mapper defaults when optional nested fields are absent

## Espresso House

- classification: REST API
- primary approach: coffee shop JSON endpoint with opening hours and amenity flags per location
- fallback: mapper-side Sweden filter as a safeguard if the upstream country field changes

## Trafikverket Parking

- classification: REST API
- primary approach: Trafikverket query API with raw XML request and JSON response archival
- fallback: fail fast on HTTP error; raw response metadata is preserved for replay once the upstream issue resolves

## TRB

- classification: feed + scrape
- primary approach: TRB station page is fetched to extract the store locator widget UID; the UID is then used to fetch the structured JSONP station bootstrap feed from `cdn.storelocatorwidgets.com`; HTML description fields are cleaned with BeautifulSoup during normalisation
- fallback: partial JSON normalisation with extraction warnings when store fields are incomplete; see [docs/trb-capture.md](trb-capture.md) for the manual browser capture path
