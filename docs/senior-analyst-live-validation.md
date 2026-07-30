# Senior Analyst LIVE validation (user-executed only)

No LIVE provider, Uvicorn or acceptance request was run while implementing this
change. After code review, the user may run exactly one permanent command from
the repository root:

```powershell
.\scripts\run-senior-analyst-live-validation.ps1
```

The script checks the required branch, merge base, pushed HEAD and clean
worktree; requires port 8053 to be free; copies and hashes the complete SQLite
bundle (`database`, `-wal`, `-shm` when present); verifies the copied database;
starts the application through its normal Uvicorn startup; and calls:

```text
GET /market-context/mnq?refresh=force&view=consumer&audience=senior_analyst_v1
```

The response contract is `SeniorAnalystPayloadV1` schema `1.0`. The request is
bounded to 1,200 seconds by default. The exact HTTP body byte array and response
headers are saved before validation. SHA-256 is calculated from those bytes.
The embedded provider-accounting checklist belongs to the same request. The
validator reads the saved body directly and does not query or reconstruct data
from the database.

The script terminates the Uvicorn parent and descendants, proves port 8053 is
free, and only then emits the final acceptance report. It invokes no trading,
delivery or order route. A `PARTIAL`/`DEGRADED` payload may pass only when all
missing values are null and explained and none of the forbidden stale,
contradictory or temporally invalid conditions are present.

Before this user-executed command, the only permitted verdict is:

```text
IMPLEMENTAZIONE OFFLINE COMPLETATA - ACCETTAZIONE LIVE PENDENTE
```
