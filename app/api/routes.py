from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from app.analysis.service import AnalysisService
from app.api.dependencies import (
    get_analysis_service,
    get_curation_service,
    get_export_service,
    get_facility_view_service,
    get_ingestion_service,
    get_provider_status_service,
    get_registry,
    get_repository,
    require_api_key,
)
from app.api.schemas import GapAnalysisRequest, ProviderStatusResponse, RunCreateRequest
from app.core.config import get_settings
from app.core.exceptions import ActiveRunError, RecordNotFound
from app.ingestion.service import IngestionService
from app.services.curation import CurationService
from app.services.export_service import ExportService
from app.services.facility_view import FacilityViewService
from app.services.provider_status import ProviderStatusService
from app.services.provider_registry import ProviderRegistry
from app.storage.db import check_db_connection
from app.storage.repository import Repository


public_router = APIRouter()
api_router = APIRouter(dependencies=[Depends(require_api_key)])


MAP_HTML = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>STXTOFR Map</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    />
    <style>
      :root {
        color-scheme: light;
        --bg: #f7f8f5;
        --panel: #ffffff;
        --panel-accent: #edf3ee;
        --text: #18211f;
        --muted: #61706b;
        --accent: #176b5b;
        --accent-strong: #0f4c43;
        --border: #d6ded9;
        --source: #7b8c87;
        --effective: #0d5d56;
        --override: #c45f2d;
        --manual: #2e6da4;
      }
      body {
        margin: 0;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        color: var(--text);
        background: var(--bg);
      }
      .layout {
        display: grid;
        grid-template-columns: 340px 1fr;
        min-height: 100vh;
      }
      .sidebar {
        padding: 18px;
        border-right: 1px solid var(--border);
        background: var(--panel);
      }
      .sidebar h1 {
        margin: 0 0 16px;
        font-size: 22px;
        line-height: 1.15;
      }
      .sidebar p,
      .sidebar label,
      .sidebar small {
        color: var(--muted);
        line-height: 1.4;
      }
      .sidebar section {
        margin-top: 16px;
      }
      .sidebar input,
      .sidebar select,
      .sidebar button {
        width: 100%;
        box-sizing: border-box;
        margin-top: 8px;
        padding: 10px 11px;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: #fbfcfa;
        font: inherit;
      }
      .sidebar button {
        background: var(--accent);
        color: white;
        cursor: pointer;
        font-weight: 700;
      }
      .sidebar button:hover {
        background: var(--accent-strong);
      }
      .toggles {
        display: grid;
        gap: 10px;
        margin-top: 12px;
      }
      .toggle {
        display: flex;
        gap: 10px;
        align-items: center;
        padding: 10px;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: var(--panel-accent);
      }
      .toggle input {
        width: auto;
        margin: 0;
      }
      .legend {
        padding: 12px;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: var(--panel-accent);
      }
      .legend-row {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 6px 0;
      }
      .swatch {
        width: 16px;
        height: 16px;
        border-radius: 999px;
      }
      #map {
        width: 100%;
        height: 100vh;
      }
      .leaflet-popup-content h3 {
        margin: 0 0 6px;
        font-size: 16px;
      }
      .leaflet-popup-content p {
        margin: 4px 0;
      }
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 4px;
        margin-top: 6px;
      }
      .chip {
        border: 1px solid #cdd8d2;
        border-radius: 999px;
        padding: 2px 7px;
        background: #f6faf7;
        color: #263c37;
        font-size: 12px;
        white-space: normal;
        word-break: break-word;
      }
      @media (max-width: 900px) {
        .layout {
          grid-template-columns: 1fr;
        }
        .sidebar {
          border-right: 0;
          border-bottom: 1px solid var(--border);
        }
        #map {
          height: 70vh;
        }
      }
    </style>
  </head>
  <body>
    <div class="layout">
      <aside class="sidebar">
        <h1 id="title">Roadside Facilities</h1>

        <section>
          <label for="need" id="label-need">Need</label>
          <select id="need">
            <option value="">Everything</option>
          </select>

          <label for="provider" id="label-provider">Provider</label>
          <select id="provider">
            <option value="">All providers</option>
          </select>

          <label for="category" id="label-category">Source type</label>
          <select id="category">
            <option value="">All source types</option>
          </select>

          <label for="city" id="label-city">City</label>
          <input id="city" type="text" placeholder="Stockholm" />

          <button id="apply-filters">Apply</button>
        </section>

        <section class="toggles">
          <label class="toggle" id="toggle-label-source"><input id="toggle-source" type="checkbox" checked /> Show imported source layer</label>
          <label class="toggle" id="toggle-label-effective"><input id="toggle-effective" type="checkbox" checked /> Show effective human-adjusted layer</label>
        </section>

        <section class="legend">
          <strong>Map legend</strong>
          <div class="legend-row"><span class="swatch" style="background:var(--source)"></span><small id="legend-source">Imported source point</small></div>
          <div class="legend-row"><span class="swatch" style="background:var(--effective)"></span><small id="legend-effective">Effective final point with no human change</small></div>
          <div class="legend-row"><span class="swatch" style="background:var(--override)"></span><small id="legend-override">Effective point changed by human review</small></div>
          <div class="legend-row"><span class="swatch" style="background:var(--manual)"></span><small id="legend-manual">Manual-only facility added in review</small></div>
        </section>

        <section>
          <small id="summary">Waiting for data...</small>
        </section>
      </aside>

      <main id="map"></main>
    </div>

    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <script>
      const TRANSLATIONS = {
        title: "Roadside Facilities",
        need: "Need",
        provider: "Provider",
        category: "Source type",
        city: "City",
        apply: "Apply",
        show_source: "Show imported source layer",
        show_effective: "Show effective human-adjusted layer",
        legend_source: "Imported source point",
        legend_effective: "Effective final point with no human change",
        legend_override: "Effective point changed by human review",
        legend_manual: "Manual-only facility added in review",
        waiting: "Waiting for data...",
        layer: "Layer:",
        status: "Status:",
        provider_label: "Provider:",
        type: "Type:",
        city_label: "City:",
        address: "Address:",
        available_here: "Available here:",
        notes: "Notes:",
        everything: "Everything",
        all_source_types: "All source types",
      };

      const map = L.map("map").setView([62.0, 15.0], 5);
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 18,
        attribution: "&copy; OpenStreetMap contributors",
      }).addTo(map);

      const sourceLayer = L.layerGroup().addTo(map);
      const effectiveLayer = L.layerGroup().addTo(map);
      const providerSelect = document.getElementById("provider");
      const needSelect = document.getElementById("need");
      const categorySelect = document.getElementById("category");
      const cityInput = document.getElementById("city");
      const summary = document.getElementById("summary");
      const toggleSource = document.getElementById("toggle-source");
      const toggleEffective = document.getElementById("toggle-effective");

      function buildUrl(path, params) {
        const url = new URL(path, window.location.origin);
        Object.entries(params).forEach(([key, value]) => {
          if (value) {
            url.searchParams.set(key, value);
          }
        });
        return url.toString();
      }

      function escapeHtml(value) {
        return String(value ?? "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#039;");
      }

      async function loadProviders() {
        const response = await fetch("/providers");
        const providers = await response.json();
        for (const provider of providers) {
          const option = document.createElement("option");
          option.value = provider.provider;
          option.textContent = provider.provider;
          providerSelect.appendChild(option);
        }
      }

      function populateSelect(select, allLabel, options) {
        const selected = select.value;
        select.innerHTML = "";
        const allOption = document.createElement("option");
        allOption.value = "";
        allOption.textContent = allLabel;
        select.appendChild(allOption);
        for (const option of options || []) {
          const node = document.createElement("option");
          node.value = option.id;
          node.textContent = option.count == null ? option.label : `${option.label} (${option.count})`;
          select.appendChild(node);
        }
        if ([...select.options].some((option) => option.value === selected)) {
          select.value = selected;
        }
      }

      function markerColor(row, layerName) {
        if (layerName === "source") return "#7b8c87";
        if (row.change_status === "manual") return "#295f92";
        if (row.change_status === "overridden") return "#b8612e";
        return "#0d5d56";
      }

      function addMarker(layer, row, layerName) {
        if (row.latitude == null || row.longitude == null) return null;
        const facets = row.service_facet_labels || [];
        const chips = facets.length
          ? `<div class="chips">${facets.map((label) => `<span class="chip">${escapeHtml(label)}</span>`).join("")}</div>`
          : "";

        const sourceValuesArray = (Array.isArray(row.source_value_labels) && row.source_value_labels.length)
          ? row.source_value_labels.filter(Boolean)
          : [...(row.services || []), ...(row.fuel_types || []), ...(row.parking_features || [])].filter(Boolean);
        const visibleValues = sourceValuesArray.slice(0, 12);
        let sourceChipsHtml = visibleValues.map((label) => `<span class="chip">${escapeHtml(label)}</span>`).join("");
        if (sourceValuesArray.length > visibleValues.length) {
          const moreCount = sourceValuesArray.length - visibleValues.length;
          const moreTitle = escapeHtml(sourceValuesArray.slice(visibleValues.length).join(", "));
          sourceChipsHtml += `<span class="chip" title="${moreTitle}">+${moreCount} more</span>`;
        }
        const marker = L.circleMarker([row.latitude, row.longitude], {
          radius: layerName === "source" ? 6 : 8,
          color: markerColor(row, layerName),
          fillColor: markerColor(row, layerName),
          fillOpacity: layerName === "source" ? 0.45 : 0.8,
          weight: layerName === "source" ? 1 : 2,
        });
        marker.bindPopup(`
          <h3>${escapeHtml(row.facility_name)}</h3>
          <p><strong>${escapeHtml(TRANSLATIONS.layer)}</strong> ${escapeHtml(layerName)}</p>
          <p><strong>${escapeHtml(TRANSLATIONS.status)}</strong> ${escapeHtml(row.change_status || "source")}</p>
          <p><strong>${escapeHtml(TRANSLATIONS.provider_label)}</strong> ${escapeHtml(row.provider_name)}</p>
          <p><strong>${escapeHtml(TRANSLATIONS.type)}</strong> ${escapeHtml(row.category_label || row.category)}</p>
          ${chips}
          <p><strong>${escapeHtml(TRANSLATIONS.city_label)}</strong> ${escapeHtml(row.city || "")}</p>
          <p><strong>${escapeHtml(TRANSLATIONS.address)}</strong> ${escapeHtml(row.formatted_address || "")}</p>
          <div><strong>${escapeHtml(TRANSLATIONS.available_here)}</strong>
            ${sourceChipsHtml ? `<div class="chips">${sourceChipsHtml}</div>` : `<span> — </span>`}
          </div>
          <p><strong>${escapeHtml(TRANSLATIONS.notes)}</strong> ${escapeHtml(row.notes || "")}</p>
        `);
        marker.addTo(layer);
        return [row.latitude, row.longitude];
      }

      function syncLayerVisibility() {
        if (toggleSource.checked) {
          map.addLayer(sourceLayer);
        } else {
          map.removeLayer(sourceLayer);
        }
        if (toggleEffective.checked) {
          map.addLayer(effectiveLayer);
        } else {
          map.removeLayer(effectiveLayer);
        }
      }

      async function loadMap() {
        const filters = {
          provider: providerSelect.value,
          need: needSelect.value,
          category: categorySelect.value,
          city: cityInput.value.trim(),
        };
        const response = await fetch(buildUrl("/map/data", filters));
        const payload = await response.json();
        populateSelect(needSelect, TRANSLATIONS.everything, payload.meta?.need_options || []);
        populateSelect(categorySelect, TRANSLATIONS.all_source_types, payload.meta?.category_options || []);

        sourceLayer.clearLayers();
        effectiveLayer.clearLayers();

        const bounds = [];
        for (const row of payload.source) {
          const point = addMarker(sourceLayer, row, "source");
          if (point) bounds.push(point);
        }
        for (const row of payload.effective) {
          const point = addMarker(effectiveLayer, row, "effective");
          if (point) bounds.push(point);
        }

        if (bounds.length) {
          map.fitBounds(bounds, { padding: [24, 24] });
        } else {
          map.setView([62.0, 15.0], 5);
        }

        summary.textContent =
          `${payload.summary.source_count} imported rows, ${payload.summary.effective_count} effective rows, ` +
          `${payload.summary.changed_count} human-adjusted rows, ${payload.summary.manual_count} manual-only rows.`;
        syncLayerVisibility();
      }

      document.getElementById("apply-filters").addEventListener("click", loadMap);
      needSelect.addEventListener("change", loadMap);
      providerSelect.addEventListener("change", loadMap);
      categorySelect.addEventListener("change", loadMap);
      toggleSource.addEventListener("change", syncLayerVisibility);
      toggleEffective.addEventListener("change", syncLayerVisibility);

      // Initialize sidebar/legend strings from TRANSLATIONS so they can be localized.
      document.getElementById("title").textContent = TRANSLATIONS.title;
      document.getElementById("label-need").textContent = TRANSLATIONS.need;
      document.getElementById("label-provider").textContent = TRANSLATIONS.provider;
      document.getElementById("label-category").textContent = TRANSLATIONS.category;
      document.getElementById("label-city").textContent = TRANSLATIONS.city;
      document.getElementById("apply-filters").textContent = TRANSLATIONS.apply;
      // toggles: the label element contains the checkbox then text; set the text node after the checkbox
      const toggleLabelSource = document.getElementById("toggle-label-source");
      if (toggleLabelSource) toggleLabelSource.childNodes[1].textContent = TRANSLATIONS.show_source;
      const toggleLabelEffective = document.getElementById("toggle-label-effective");
      if (toggleLabelEffective) toggleLabelEffective.childNodes[1].textContent = TRANSLATIONS.show_effective;
      document.getElementById("legend-source").textContent = TRANSLATIONS.legend_source;
      document.getElementById("legend-effective").textContent = TRANSLATIONS.legend_effective;
      document.getElementById("legend-override").textContent = TRANSLATIONS.legend_override;
      document.getElementById("legend-manual").textContent = TRANSLATIONS.legend_manual;
      document.getElementById("summary").textContent = TRANSLATIONS.waiting;

      loadProviders().then(loadMap);
    </script>
  </body>
</html>
"""


@public_router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def build_readiness_payload() -> tuple[dict[str, object], int]:
    settings = get_settings()
    payload: dict[str, object] = {
        "status": "ok",
        "repository_backend": settings.repository_backend,
        "baserow_backend": settings.baserow_backend,
    }
    status_code = 200
    if settings.repository_backend == "memory":
        payload["checks"] = {
            "repository": {
                "status": "ok",
                "detail": "in-memory repository configured",
            }
        }
        return payload, status_code

    db_ok, db_error = check_db_connection()
    payload["checks"] = {
        "database": {
            "status": "ok" if db_ok else "error",
            "detail": "database connection available" if db_ok else db_error,
        }
    }
    if not db_ok:
        payload["status"] = "degraded"
        status_code = 503
    return payload, status_code


@public_router.get("/ready")
def readiness() -> JSONResponse:
    payload, status_code = build_readiness_payload()
    return JSONResponse(status_code=status_code, content=payload)


@public_router.get("/map", response_class=HTMLResponse)
def map_view() -> HTMLResponse:
    return HTMLResponse(content=MAP_HTML)


@public_router.get("/map/data")
def map_data(
    provider: str | None = Query(default=None),
    category: str | None = Query(default=None),
    city: str | None = Query(default=None),
    need: str | None = None,
    facility_view_service: FacilityViewService = Depends(get_facility_view_service),
) -> dict[str, object]:
    return facility_view_service.build_map_snapshot(provider=provider, category=category, city=city, need=need)


@api_router.get("/providers")
def list_providers(registry: ProviderRegistry = Depends(get_registry)) -> list[dict[str, object]]:
    return [provider.describe() for provider in registry.list()]


@api_router.get("/providers/{provider}/status", response_model=ProviderStatusResponse)
def provider_status(
    provider: str,
    registry: ProviderRegistry = Depends(get_registry),
    provider_status_service: ProviderStatusService = Depends(get_provider_status_service),
) -> ProviderStatusResponse:
    adapter = registry.get(provider)
    return ProviderStatusResponse(**provider_status_service.build_status(adapter))


@api_router.post("/runs")
def create_runs(
    payload: RunCreateRequest,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    providers = payload.providers or service.registry.names()
    runs = []
    for name in providers:
        try:
            runs.append(service.run_provider(name, mode=payload.mode, dry_run=payload.dry_run))
        except ActiveRunError as exc:
            runs.append({"error": str(exc), "skipped": True, "provider": name})
    return {"runs": runs}


@api_router.post("/runs/{provider}")
def create_provider_run(
    provider: str,
    payload: RunCreateRequest,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    try:
        run = service.run_provider(provider, mode=payload.mode, dry_run=payload.dry_run)
    except ActiveRunError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"run": run}


@api_router.get("/runs")
def list_runs(
    provider: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    repository: Repository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_runs(provider=provider, mode=mode, status=status, limit=limit, offset=offset)


@api_router.get("/runs/{run_id}")
def get_run(run_id: int, repository: Repository = Depends(get_repository)) -> dict[str, object]:
    run = repository.get_run_detail(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@api_router.post("/reprocess/{record_id}")
def reprocess_record(
    record_id: int,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    try:
        return service.reprocess_raw_payload(record_id)
    except RecordNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@api_router.get("/facilities")
def list_facilities(
    provider: str | None = Query(default=None),
    category: str | None = Query(default=None),
    city: str | None = Query(default=None),
    need: str | None = None,
    verified: bool | None = Query(default=None),
    view: str = Query(default="source"),
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    facility_view_service: FacilityViewService = Depends(get_facility_view_service),
) -> list[dict[str, object]]:
    if view not in {"source", "effective"}:
        raise HTTPException(status_code=400, detail="view must be 'source' or 'effective'")
    return facility_view_service.list_facilities(
        view=view,
        provider=provider,
        category=category,
        city=city,
        need=need,
        verified=verified,
        limit=limit,
        offset=offset,
    )


@api_router.get("/facilities/{facility_id}")
def get_facility(
    facility_id: int,
    view: str = Query(default="source"),
    facility_view_service: FacilityViewService = Depends(get_facility_view_service),
    repository: Repository = Depends(get_repository),
) -> dict[str, object]:
    if view == "source":
        facility = repository.get_facility(facility_id)
    elif view == "effective":
        facility = next(
            (
                row
                for row in facility_view_service.list_facilities(view="effective")
                if row.get("source_facility_id") == facility_id
            ),
            None,
        )
    else:
        raise HTTPException(status_code=400, detail="view must be 'source' or 'effective'")
    if not facility:
        raise HTTPException(status_code=404, detail="facility not found")
    return facility


@api_router.get("/gaps")
def list_gaps(
    region: str | None = Query(default=None),
    category: str | None = Query(default=None),
    stale_only: bool = Query(default=False),
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    repository: Repository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_gaps(region=region, category=category, stale_only=stale_only, limit=limit, offset=offset)


@api_router.get("/issues")
def list_issues(
    provider: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    run_id: int | None = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    repository: Repository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_normalization_issues(provider=provider, severity=severity, run_id=run_id, limit=limit, offset=offset)


@api_router.post("/analysis/gaps")
def run_gap_analysis(
    payload: GapAnalysisRequest,
    service: AnalysisService = Depends(get_analysis_service),
) -> dict[str, object]:
    findings = service.run_gap_analysis(
        region=payload.region,
        category=payload.category,
        stale_only=payload.stale_only,
    )
    return {"findings": findings}


@api_router.post("/curation/push")
def push_curation_rows(
    provider: str | None = Query(default=None),
    service: CurationService = Depends(get_curation_service),
) -> dict[str, object]:
    result = service.push_to_baserow(provider=provider)
    if result["status"] == "failed":
        raise HTTPException(status_code=503, detail=result)
    return result


@api_router.post("/curation/bootstrap")
def bootstrap_curation_schema(service: CurationService = Depends(get_curation_service)) -> dict[str, object]:
    try:
        return service.bootstrap_baserow_schema()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@api_router.post("/curation/pull")
def pull_curation_rows(service: CurationService = Depends(get_curation_service)) -> dict[str, object]:
    result = service.pull_from_baserow()
    if result["status"] == "failed":
        raise HTTPException(status_code=503, detail=result)
    return result


@api_router.get("/curation/syncs")
def list_curation_syncs(repository: Repository = Depends(get_repository)) -> list[dict[str, object]]:
    return repository.list_curation_syncs()


@api_router.post("/exports/facilities")
def export_facilities(service: ExportService = Depends(get_export_service)) -> dict[str, object]:
    return service.build_facility_bundle()


@api_router.get("/exports")
def list_exports(repository: Repository = Depends(get_repository)) -> list[dict[str, object]]:
    return repository.list_export_builds()
