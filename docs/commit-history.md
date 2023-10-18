# Commit Timeline

These timestamps are intended to recreate a single-developer work history between March and October 2023.

- 2023-03-18 10:14 bring in archived source notes and sketch the service layout
- 2023-03-20 09:08 scaffold app package, env config, and basic logging setup
- 2023-03-20 14:32 add db session bootstrap and first ingestion run tables
- 2023-03-21 10:11 add provider adapter base classes and source metadata model
- 2023-03-23 15:07 wire manual full sync flow through an ingestion service
- 2023-03-27 09:41 add first pass of circle k fetch client from station search payload
- 2023-03-27 16:18 split circle k mapping out from the fetch path
- 2023-03-31 11:02 persist raw responses with request fingerprint and fetch timing
- 2023-04-03 09:27 add normalized facility dataclass and mapper error container
- 2023-04-06 14:56 save normalized facilities and source link rows
- 2023-04-12 10:21 add circle k tests around fuels and opening hours cleanup
- 2023-04-18 15:22 add preem station api adapter and initial mapper
- 2023-04-24 09:36 track provider fetch status separately from ingestion runs
- 2023-04-28 14:11 add run endpoints and provider listing response
- 2023-05-03 10:04 add trafikverket parking client against trafikinfo api
- 2023-05-08 13:47 fix bad address fallback on trafikverket parking records
- 2023-05-12 11:18 add ids station feed adapter with detail page fallback
- 2023-05-17 15:02 move retry and backoff into a shared requests wrapper
- 2023-05-24 09:54 add espresso house adapter and basic category mapping
- 2023-05-31 14:43 add provider checkpoints for incremental runs
- 2023-06-06 10:12 build raw replay path for single payload reprocess
- 2023-06-13 16:07 add facilities endpoint with provider and category filters
- 2023-06-21 09:29 start rasta html adapter and pull selectors into parser helpers
- 2023-06-28 15:26 add parser tests for rasta services and hours cleanup
- 2023-07-05 10:33 add trb ajax adapter and strip html from descriptions
- 2023-07-12 14:58 add first pass of gap bucketing by grid cell and category
- 2023-07-19 11:24 publish gap findings through a local publisher abstraction
- 2023-07-27 16:12 add gap analysis api and rerun endpoint
- 2023-08-03 09:48 add merge candidate scoring for nearby same-name facilities
- 2023-08-11 14:39 store merged facility shell and source link candidates
- 2023-08-18 10:57 tighten provider status response and stale-region checks
- 2023-08-29 15:11 add trb and circle k regression fixtures after parser breakage
- 2023-09-05 09:22 move publisher config behind env flags with noop default
- 2023-09-14 13:36 add provider health snapshot service and status endpoint
- 2023-09-22 16:18 clean up run history filters and replay response shape
- 2023-10-03 10:07 add docker compose for api postgres and local kafka
- 2023-10-10 15:03 document provider strategies and the ops workflow
- 2023-10-18 11:41 clean up migration names and add dev fixture loading
- 2023-10-26 16:09 tighten mapper failure logging and publish skip handling

