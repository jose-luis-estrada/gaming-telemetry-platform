# Incident reports

One report per seeded defect, written as a production incident: what was
expected, what was observed, how it was diagnosed, what changed, what would be
different at 100x scale, and a reconciliation line against
`config/manifest.json`.

| Defect | Report | Evidence collected |
|---|---|---|
| Skew | not written | Straggler observed under a join, AQE non-application explained, salting before and after measured. See ADR-0010. |
| Small files | not written | 102,056 landing files, 3,190 read tasks from the open-cost floor. See ADR-0011. |
| Duplicates | not written | Two flavors resolved; 5 out-of-window duplicates survive by design. See ADR-0006. |
| Late arrivals | not written | Wrong-low then corrected Gold value; one partition rewritten per the Delta log; past-horizon band routed out. See ADR-0008. |
| Schema drift | not written | Drift key count before the drift day is 0, enforced as a hard rule. See ADR-0007. |

Evidence will be re-collected on the KIND lakehouse (ADR-0014) and stored under
`evidence/`, with every claim in a report pointing to a raw log or screenshot.
