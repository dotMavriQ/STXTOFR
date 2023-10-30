# TRB Capture Workflow

This is the fallback workflow for capturing TRB station coordinates and related metadata when the public store-locator API does not answer reliably from the CLI environment.

## Fields We Want

The expected TRB record shape is:

- `place`
- `lat`
- `long`
- `address`
- `city`
- `county`
- `zip`
- `phone`
- `hours`
- `fuels`
- `services`
- `description`

The current TRB adapter maps these from the public store-locator widget payload.

## Quick Path

Run the capture script first:

```bash
PYTHONPATH=. python scripts/capture_trb_locator.py
```

If the live call succeeds, artifacts are written under `tmp/trb_capture/`.

If the live call fails or returns zero records, use the browser-assisted flow below.

## Browser-Assisted Capture

1. Open `https://trb.se/hitta-tankstation/` in a desktop browser.
2. Open DevTools and go to the `Network` tab.
3. Filter for `json/` or `storelocatorwidgets`.
4. Interact with the station finder so it performs a search.
5. Click the request that looks like:

```text
https://cdn.storelocatorwidgets.com/json/<uid>-Swedish?callback=slw...
```

6. Open the response body.
7. Save the full response text exactly as returned, including the `slwapi(...)` wrapper, to a local file such as `tmp/trb_widget_response.txt`.

## Convert Saved Response

Once the response body is saved:

```bash
PYTHONPATH=. python scripts/capture_trb_locator.py \
  --response-file tmp/trb_widget_response.txt \
  --uid zD99kjHRWXwQtFDaUIvDfrXYnu9SezCG
```

## Output Files

The script writes:

- `*_widget_response.txt`: raw JSONP response
- `*_decoded_payload.json`: decoded widget payload
- `*_records.json`: flattened TRB records from the widget response
- `*_flat_records.json`: normalised flat shape suitable for downstream comparison
- `*_manifest.json`: capture metadata and any errors

## Notes

- The TRB page currently exposes the widget UID on the page itself.
- The proven live source is the widget bootstrap feed on `cdn.storelocatorwidgets.com/json/{uid}-Swedish`.
- The CLI environment may still hit upstream DNS or network restrictions even when the browser request works, which is why this manual capture path exists.
