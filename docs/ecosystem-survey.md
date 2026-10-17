# Pendo destination ecosystem survey

Survey date: 2026-10-17

This survey asks a narrow question: what public software currently loads visitor
or account metadata *into* Pendo, especially through Singer?

## Results

| Search surface | Result |
| --- | --- |
| GitHub repository search for `target-pendo in:name` | Only `kc-salazar/target-pendo` |
| Broader GitHub repository searches for Pendo + Singer/target/loader/warehouse | No other public Singer Pendo target found |
| GitHub code search for the bulk metadata endpoint | One Segment destination-function example, not a Singer target |
| Meltano Hub | Pendo taps are listed; no Pendo target is listed |
| PyPI `target-pendo` project endpoint | `404`; the distribution name was unclaimed |

Relevant public projects:

- [`singer-io/tap-pendo`](https://github.com/singer-io/tap-pendo) extracts data
  *from* Pendo. It is a tap, not a destination.
- [`segment-services-eng/usertesting`](https://github.com/segment-services-eng/usertesting)
  includes a 2022 Segment function that sends one visitor or account per invocation.
  It is not a general Singer target and does not bulk schedule warehouse records.
- [`pendo-io/pendo-ETL-API-calls`](https://github.com/pendo-io/pendo-ETL-API-calls)
  contains official examples for extracting Pendo data for BI. It is not a loader.

## What is distinct here

`target-pendo` is a general Singer destination, independent of the source tap. Its
performance path combines:

- Pendo's bulk visitor/account metadata endpoint;
- exact byte- and row-bounded request construction;
- one-pass JSON encoding reused by the HTTP client;
- compiled JSON Pointer selectors;
- same-ID coalescing with last-value-wins semantics inside a Singer buffer;
- HTTP/2 connection pooling;
- bounded parallel requests with one global concurrency ceiling across streams;
- `Retry-After` support and transient-error retries;
- automatic binary splitting after Pendo `408` or `413` responses; and
- state emission only after every started request succeeds.

## Boundary

GitHub search cannot reveal private, deleted, or unindexed repositories. The survey
supports the claim that no other *publicly discoverable* Singer Pendo target was
found on the survey date. It does not prove that no private implementation exists.
