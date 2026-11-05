**Language / Langue :** [English](#english) · [Français](#francais)

<a id="english"></a>

# Endeavour Mining P2P Framework: how it works and how to use it

**Boomi account:** the Endeavour account (ID in `BOOMI_ACCOUNT_ID` in `.env`, never in this file). Folder: `Endeavour Management Services London Limited/edv.framework` (`folderId Rjo4ODM3OTY0`).
**Checked against the platform on 2026-11-05.** The component list and IDs come from a `boomi-component-search` run that day, and all 14 processes were pulled at their current version. Nothing was taken from an older doc or from the local sync-state (see § 10).

> **There are two `edv.framework` folders.** Only the one at the account root counts (`Rjo4ODM3OTY0`, 66 components). `1-Development/edv.framework/Shared_Services/Logging` is **empty**. A `--folder "%edv.framework%"` search returns both.

This document describes the **reusable** framework: the shared subprocesses, the Event Streams topics and the SQL Server schema. It doesn't describe any one business interface (Cost Object, S2C GAN, INVPO…). Those interfaces are built *with* the framework.

**History:**
- **2026-09-14:** first version, on MySQL (hosted) with the Database (Legacy) connector.
- **2026-09-25:** migrated to SQL Server with the Database V2 connector.
- **2026-09-28/29:** "resilience" redesign. Processes no longer write to the database directly; they go through Event Streams.
- **2026-11-03/05:** the Listen consumer writes with a single MERGE, the Update is worked around with a Standard Insert, and the CorrelationId prefix becomes the InterfaceId.
- **2026-11-05:** deleted 30 unused legacy MySQL components (list in `active-development/inventories/edv_framework_deleted_20261105.tsv`).

---

## 1. Why this framework exists

Rule set on 2026-09-16: **every new P2P interface (EAM, Ivalua, GAN or any future system) must be built from `TEMPLATE_Standard_P2P_Parent`**. Never build a standalone Listen/Poll → Map → Send flow with its own local error handling. Adding an interface should come down to "copy the template and add a row to `INTERFACE_REGISTRY`", without rebuilding correlation, logging and error handling each time.

**Resilience principle** (user requirement, 2026-09-28): a short SQL Server outage must lose **no** data. The Database V2 connector doesn't retry a failed connection, so an interface's execution path **never writes to the database directly**. It only publishes to Event Streams, and asynchronous consumers write to SQL Server.

---

## 2. Flow overview

```
TEMPLATE_Standard_P2P_Parent (copy for each interface)
  Start
   → Set Properties "Set Interface Data"
       (InterfaceName/InterfaceId/ObjectType/BusinessKey/SourceSystem/TargetSystem/Environment)
   → [Process Call] Sub_Generate_CorrelationId
        generates CorrelationId + TransactionId, LogPhase=START
        → [Process Call] Sub_Execution_Logger ──► Event Streams  edv.transaction.log  (START event)
   → Try/Catch
       Try:
         → PLACEHOLDER business logic (the interface's connectors/maps; replaces this Notify)
         → LogPhase=SUCCESS
         → [Process Call] Sub_Execution_Logger ──► edv.transaction.log  (SUCCESS event)
         → Stop (continue)
       Catch:
         → LogPhase=ERROR
         → [Process Call] Central_Error_Handler
              Get ERROR_CODE_REGISTRY (inside Try/Catch, safe fallback if the database doesn't answer)
              ├─ Retryable  → returns "Classified-Retryable" (nothing is queued)
              └─ otherwise  → publishes ──► Event Streams  edv.transactions.error → returns "Classified-Queued"
         → [Process Call] Sub_Execution_Logger ──► edv.transaction.log  (ERROR event)
         → Stop (continue=false)

Asynchronous consumers (the only processes that write to SQL Server):
  edv.transaction.log    ──► Scheduled_TransactionLog_Consumer (Listen)  ── MERGE into TRANSACTION_LOG
  edv.transactions.error ──► Sub_ErrorQueue_Consumer (Consume)           ── INSERT into ERROR_QUEUE

Alongside:
  WSS_Retrigger_Handler → Sub_Retrigger_Handler   manual retrigger API
  Scheduled_Purge_Retention                       purge based on retention period
```

If the database is down, rows show up late, but none are lost, **as long as the publish to Event Streams succeeds**.

---

## 3. Component inventory (`edv.framework`, 66 components as of 2026-11-05)

### Processes

| Process | componentId | Version | Role |
|---|---|---|---|
| `TEMPLATE_Standard_P2P_Parent` | `4c3f5a28-eaae-4d08-a973-892d6c3ecda7` | v5 | Skeleton to **copy** for each new interface |
| `Sub_Generate_CorrelationId` | `f79c2fe3-e612-4c12-93a1-ac48455fc191` | v16 | Generates CorrelationId (`InterfaceId-timestamp-unique`) and TransactionId (UUID), then publishes the START event |
| `Sub_Execution_Logger` | `73ce504a-48cf-4f2c-9616-94440e7ce77f` | v4 | Builds the JSON event and publishes it to `edv.transaction.log` |
| `Central_Error_Handler` | `26ca08e9-68db-418d-bfef-b8ce452ce9b1` | v17 | Classifies the error and, if it isn't retryable, publishes to `edv.transactions.error` |
| `Scheduled_TransactionLog_Consumer` | `6e731412-a277-4b1f-83be-3338bd141d52` | v7 | **Current consumer** of `edv.transaction.log`: Listen with transacted acknowledgement, one MERGE per event |
| `Sub_TransactionLog_Consumer` | `8daf258f-72f7-4c33-9e24-6db5b6b0e1a2` | v14 | **Old** consumer (Consume + schedule), replaced on 2026-11-05. See § 9 |
| `Sub_ErrorQueue_Consumer` | `b08bd048-de08-4220-999a-087f82bbd909` | v3 | Reads `edv.transactions.error` (Consume) and INSERTs into `ERROR_QUEUE` |
| `Sub_Retrigger_Handler` | `bbfdb162-52a4-4dc8-ae8a-4da97c96d061` | v7 | Manual retrigger logic |
| `WSS_Retrigger_Handler` | `7974b1f4-fae2-45bd-9c61-be804ed00133` | v1 | Minimal WSS listener: `POST /ws/simple/executeRetrigger` |
| `Scheduled_Purge_Retention` | `b7cd1d66-f934-41a0-806e-7c8359a82207` | v4 | Calls `EDV_INTEGRATION.SP_PURGE_RETENTION` |
| `Util_Seed_InterfaceRegistry` | `df032bae-22f9-427d-945b-971eaed880c3` | v4 | Run once: inserts the 5 Cost Object interfaces. **Pattern to copy** |
| `Util_Seed_UnknownErrorCode` | `8fb458ea-14f5-411b-8f4c-1854df4f1117` | v1 | Run once (already done): creates the `UNKNOWN_CODE` row in `ERROR_CODE_REGISTRY` |
| `Diag_DbCheck` / `Diag_SeedTemplateInterface` | `3489ec68-…` / `308e6443-…` | — | Temporary diagnostic processes, **to delete** (their own descriptions say so) |

### Connections and library

| Component | componentId | Role |
|---|---|---|
| `EDV_INTEGRATION (SQL)` | `2187b6a8-fab6-4832-a7f1-4eb3bc36a2e8` | Database V2 connection to SQL Server, schema `EDV_INTEGRATION` |
| `P2P_Framework_EventStreams_Connection` | `54d36e66-c760-45a9-91b2-aa28f83a43d2` | Event Streams connection (used to publish and to consume) |
| `Email_request` | `94d4ffec-8b57-4640-b74f-4481a06e1f5a` | Added by another developer on 2026-11-04; no process uses it yet |
| `MySQL_JDBC` | `e2ccde21-4884-4bf8-922f-80b826cd9808` | Left over from the MySQL days, no longer referenced, but **still deployed**: it couldn't be deleted and is kept on purpose |

### Database V2 operations (all on `EDV_INTEGRATION (SQL)`)

| Operation | Used by |
|---|---|
| `Send_TransactionLog_Upsert_V2` (MERGE) | `Scheduled_TransactionLog_Consumer` |
| `Send_TransactionLog_Insert_V2`, `Send_TransactionLog_UpdateStatus_V2` | `Sub_TransactionLog_Consumer` (old) |
| `Get_ErrorCodeRegistry_ByCode_V2` | `Central_Error_Handler` |
| `Send_ErrorQueue_Insert_V2` | `Sub_ErrorQueue_Consumer` |
| `Get_TransactionLog_SelectByCorrelation_ForRetrigger_V2`, `Get_ErrorQueue_SelectLatestByTransaction_V2`, `Get_PayloadStore_SelectLatestByTransaction_V2`, `Get_RetryHistory_NextNumber_V2`, `Send_RetryHistory_Insert_V2` | `Sub_Retrigger_Handler` |
| `Send_SP_PurgeRetention_V2` | `Scheduled_Purge_Retention` |
| `Send_InterfaceRegistry_Insert_V2` (+ map `Map_InterfaceRegistrySeed_to_InterfaceRegistry_V2`) | `Util_Seed_InterfaceRegistry`, `Diag_SeedTemplateInterface` |
| `Send_ErrorCodeRegistry_Insert_V2` | `Util_Seed_UnknownErrorCode` |
| `Get_TransactionLog_SelectIdByCorrelation_V2` | **no process** (delete if nobody needs it) |
| `Diag_Get_DbCheck_V2` | `Diag_DbCheck` |

The remaining components are the JSON request and response profiles for these operations (`*_V2_Request_JSON` / `*_V2_Response_JSON`), plus `P2P_TransactionLogEvent_JSON` (the event format), `DatabaseV2_WriteResponse_JSON`, and `RetriggerRequest_JSON`/`RetriggerResponse_JSON` for the WSS API.

### Who uses the framework (as of 2026-11-05)

- **About 22 processes** call `Sub_Generate_CorrelationId` and `Sub_Execution_Logger`: Cost Objects (CO_*), FX_RATES, GL, FCT, INVPO / NON_INVPO (request and ack), INV_RCPT / NON_INVRCPT, INV_OKPAY, PAY_BATCH, PAY_CONFIRMATION, COMMON-CATCH-PROCESS and the two S2C GAN interfaces.
- **Only 7** call `Central_Error_Handler`: COMMON-CATCH-PROCESS, GL_ORCHESTRATOR, INVPO_ACK_LISTEN, INVPO_ORCHESTRATOR, the template and the two S2C GAN interfaces. The others log their errors but don't classify or queue them.

To rebuild this list: `boomi-component-search.sh --related-to <componentId>`.

---

## 4. Event Streams topics

| Topic | Producer | Subscription | Consumer |
|---|---|---|---|
| `edv.transaction.log` | `Produce_P2P_ExecutionLog_Event` (in `Sub_Execution_Logger`) | `p2p-transaction-log-consumer` (Shared) | `Scheduled_TransactionLog_Consumer` via `Listen_TransactionLog_Consumer` (transacted, `maxRetries=5`, `ackTimeout=60`, then dead-letter queue) |
| `edv.transactions.error` | `Produce_P2P_ErrorQueueEvent` (in `Central_Error_Handler`) | `p2p-transactions-error-consumer` (Shared) | `Sub_ErrorQueue_Consumer` via `Consume_P2P_ErrorQueueEvent` (`acknowledgeLater=true`, `maxMessages=10`) |

The old topics `esb.p2p.transaction.log` and `esb.transaction.log` **no longer exist** since 2026-09-29. Everything follows the `edv.*` convention.

The log event (profile `P2P_TransactionLogEvent_JSON`) contains: correlationId, transactionId, interfaceName, interfaceId, logPhase (`START`/`SUCCESS`/`ERROR`), errorCategory, objectType, businessKey, sourceSystem, targetSystem, environment and eventTimestamp.

---

## 5. SQL Server schema (schema `EDV_INTEGRATION`, script `script_create_framework.sql`)

| Table | Role | Key |
|---|---|---|
| `INTERFACE_REGISTRY` | Catalogue of allowed interfaces | PK `INTERFACE_ID` (VARCHAR 20) |
| `TRANSACTION_LOG` | One row per interface run; the heart of the framework | PK `TRANSACTION_ID` (UNIQUEIDENTIFIER), FK `INTERFACE_ID` to `INTERFACE_REGISTRY` |
| `ERROR_QUEUE` | Non-retryable errors waiting for a manual retrigger | FK `TRANSACTION_ID`, FK `ERROR_CODE` to `ERROR_CODE_REGISTRY` |
| `RETRY_HISTORY` | History of retrigger attempts | FK `TRANSACTION_ID` |
| `PAYLOAD_STORE` | Reference to the original payload (`PAYLOAD_URI`), not its content | FK `TRANSACTION_ID` |
| `ERROR_CODE_REGISTRY` | Error code classification (`BUSINESS`/`TECHNICAL`, `RETRYABLE`) | PK `ERROR_CODE` |
| `CONFIG` | Settings (retry, retention, notifications) | PK `CONFIG_KEY` |
| `AUDIT_LOG`, `NOTIFICATION_LOG`, `BUSINESS_EVENT` | Planned, but not wired into the standard path | — |

`TRANSACTION_LOG` columns: `TRANSACTION_ID`, `CORRELATION_ID`, `INTERFACE_ID`, `OBJECT_TYPE`, `BUSINESS_KEY`, `SOURCE_SYSTEM`, `TARGET_SYSTEM`, `STATUS`, `START_DATE`, `END_DATE`, `DURATION_MS`, `ENVIRONMENT`, `CREATED_DATE`.

**The live database doesn't match the script for `ERROR_CODE_REGISTRY`.** The script has `MISSING_VAT_NUMBER`/`INVALID_PO_LINE`. The live table has `CONNECTION_FAILURE`, `HTTP_500`, `HTTP_502`, `HTTP_503`, `INVALID_MAPPING`, `MISSING_DATA`, `TIMEOUT` and `UNKNOWN_CODE` (the last one added by `Util_Seed_UnknownErrorCode`). To pick a test code, query the real table, not the script.

**`CONFIG`:** `MAX_RETRY_COUNT=3`, `AUTO_RETRY_ENABLED=TRUE`, `PAYLOAD_RETENTION_DAYS=90`, `ERROR_RETENTION_DAYS=365`, `RETRIGGER_ENABLED=TRUE`, `NOTIFICATION_TEAMS_ENABLED=TRUE`, `NOTIFICATION_EMAIL_ENABLED=TRUE`.

`SP_PURGE_RETENTION` is **not** in `script_create_framework.sql`. Check that it actually exists in the database before relying on the purge.

---

## 6. How it works, subprocess by subprocess

### `Sub_Generate_CorrelationId`
- Generates the `CorrelationId` with a native Set Properties step, as `InterfaceId-timestamp-unique` (`INTERFACE_ID` prefix, e.g. `S2C_SUP`, since 2026-11-04).
- Generates the `TransactionId` as a UUID in a small Groovy script. This is the one deliberate exception to "no Groovy".
- Sets `LogPhase=START` and calls `Sub_Execution_Logger`. **It no longer writes to the database.**
- The caller must have set the `InterfaceId`/`ObjectType`/`BusinessKey`/`SourceSystem`/`TargetSystem`/`Environment` properties beforehand. They're copied into the event so the consumer has everything it needs to create the row.

### `Sub_Execution_Logger`
Builds the JSON event and publishes it (Produce) to `edv.transaction.log`. The calling process doesn't wait for any answer; the database write happens asynchronously in the consumer.

### `Central_Error_Handler`
**Calling contract: the caller must set the `ErrorCode` process property** before calling this subprocess. It must come from a real connector property (e.g. `HTTP_Status_Code`); never parse a free-text error message. If it's missing, the default is `UNKNOWN_CODE` (`TECHNICAL`, not retryable).

Steps:
1. A Get on `ERROR_CODE_REGISTRY`, protected by a Try/Catch. If the database can't be reached, a safe fallback is used (`TECHNICAL`, not retryable).
2. It sets `ErrorCategory` and `Retryable`.
3. It routes:
   - `Retryable=true`: it only logs and returns **`Classified-Retryable`**. The connector's native retry is assumed to have already run.
   - otherwise: it publishes to `edv.transactions.error` and returns **`Classified-Queued`**.

### `Scheduled_TransactionLog_Consumer`: current consumer of `edv.transaction.log`
- **Start:** Listen with transacted acknowledgement. A message is removed from the topic only if the run ends successfully. On an exception it's redelivered up to 5 times, then goes to the dead-letter queue.
- **Don't** schedule it: Listen takes care of that.
- **Don't** deploy it alongside `Sub_TransactionLog_Consumer`: they share the same subscription.

Steps:
1. It extracts the CorrelationId.
2. A Groovy script converts the event timestamp to `yyyy-MM-dd HH:mm:ss.SSS`.
3. **A single MERGE** (`Send_TransactionLog_Upsert_V2`) handles any event (START, SUCCESS or ERROR), in any arrival order.
4. A Groovy check (`shapeCHK`) **fails the run** if the MERGE returned a SQL error. Without this check, the run would end as COMPLETE (see § 8) and the message would be lost.

### `Sub_TransactionLog_Consumer`: old consumer (Consume)
Previous version, replaced on 2026-11-05.
- It handles inserts first, then close-outs (Branch).
- For a close-out whose row doesn't exist yet: if the event is **less than 15 minutes old**, it raises `CLOSEOUT_ROW_NOT_FOUND` so the event is retried; if it's older, it processes it once.
- **Underlying problem:** with `acknowledgeLater=true`, Consume acknowledges nothing without an Acknowledge step, and that step fails on this runtime (§ 8). So messages come back forever.

### `Sub_ErrorQueue_Consumer`
- Consume on `edv.transactions.error`, then Message, then `Send_ErrorQueue_Insert_V2`, all inside a Try/Catch.
- **Must be scheduled** from the Boomi UI: Atom Management → Scheduling.
- It has the same acknowledgement problem as the old consumer (§ 9).

### `Sub_Retrigger_Handler` / `WSS_Retrigger_Handler`
Manual API: `POST /ws/simple/executeRetrigger` with `{"correlationId": "..."}`. The subprocess:
1. looks up `TRANSACTION_ID` (and the status) in `TRANSACTION_LOG`; returns **`NotFound`** if there's no match;
2. reads the latest `ERROR_QUEUE` row and the latest `PAYLOAD_STORE` row. The lookup uses `TRANSACTION_ID`, which is the real foreign key;
3. computes the next `RETRY_NUMBER` (`Get_RetryHistory_NextNumber_V2`) and inserts a row into `RETRY_HISTORY`;
4. returns **`Retriggered`** with `payload_uri`.

**It doesn't automatically resend the message to the target system.** No generic mechanism allows that; it only exposes what an operator needs. The WSS wrapper connects both return paths (`Retriggered` to shape18, `NotFound` to shape21) to one response.

### `Scheduled_Purge_Retention`
- No-data start, then a `{}` Message, then `Send_SP_PurgeRetention_V2` (`EXEC EDV_INTEGRATION.SP_PURGE_RETENTION`), inside a Try/Catch.
- **Must be scheduled** from the Boomi UI.
- **Not verified with real data.** The operation is a Dynamic Update built in XML, and that kind of Update fails on this account (§ 8, pitfall 4). It will probably need to become a Standard Insert that runs the `EXEC`.

### `Util_Seed_InterfaceRegistry`: pattern to copy, not part of the framework
- Process run once with `boomi-test-execute.sh`, never deployed.
- Chains Message (array of rows), then `Map_InterfaceRegistrySeed_to_InterfaceRegistry_V2` (one document per row), then `Send_InterfaceRegistry_Insert_V2`.
- **`INTERFACE_ID` is a primary key**: running the process twice causes a duplicate key error.

---

## 7. How to add a new interface

1. **Add the row to `INTERFACE_REGISTRY`** (`INTERFACE_ID`, `DOMAIN_NAME`, `INTERFACE_NAME`, `SOURCE_SYSTEM`, `TARGET_SYSTEM`). You can copy the `Util_Seed_InterfaceRegistry` pattern or run the INSERT in SQL. **It must exist before the first real run**: without it, the foreign key rejects the MERGE into `TRANSACTION_LOG`. And because of pitfall 1 (§ 8), that rejection can go unnoticed.
2. **Copy `TEMPLATE_Standard_P2P_Parent`** into the interface's project folder (pull then create under a new name, or duplicate it in the Boomi UI and then pull).
3. In "Set Interface Data": fill in `InterfaceName`, `InterfaceId` (used as the CorrelationId prefix), `ObjectType`, `SourceSystem`, `TargetSystem`, `Environment`, and `BusinessKey` if needed.
4. Replace the placeholder Notify with the real business logic.
5. **Set `ErrorCode` yourself.** Before any point that can fail, or in the Catch before calling `Central_Error_Handler`, give the `ErrorCode` process property a value taken from a real connector property. **The template doesn't do this** (§ 9): if you forget, every error is classified as `UNKNOWN_CODE` (not retryable).
6. Add the interface's own error codes to `ERROR_CODE_REGISTRY` (`ERROR_CATEGORY` ∈ {`BUSINESS`, `TECHNICAL`}, `RETRYABLE`).
7. **Don't touch** the Process Calls to `Sub_Generate_CorrelationId`, `Sub_Execution_Logger` and `Central_Error_Handler`. They're what make the interface framework-compliant.
8. Push, deploy and test. Then check **the row in `TRANSACTION_LOG`**; a COMPLETE run status isn't enough (§ 8).

---

## 8. Known pitfalls (seen in practice)

1. **COMPLETE doesn't mean the write succeeded.** All Database V2 operations run with `returnApplicationErrors=true`. A failed INSERT or UPDATE then comes back as a normal JSON document (`{"errorCode":…}`), and the run ends as COMPLETE. Always check `"Status":"Executed Successfully"` and `"Rows Effected"`, or run a `SELECT` against the database. In a consumer, you need a check that fails the run (see `shapeCHK`).
2. **Database V2 Get parameters:** the `$param` name (and the matching field in the JSON request profile) must be **exactly the SQL column name**, case included (`$ERROR_CODE`, not `$ErrorCode`). Otherwise you get error 405 "Keys Provided in Input Request is not matching the Request Profile".
3. **Dates sent to SQL Server:** use `yyyy-MM-dd HH:mm:ss.SSS`, with a space and **no `T`**. The ISO format with `T` is rejected (error 405). This only applies to dates passed as SQL parameters, not to dates in a JSON payload or a CorrelationId.
4. **A Database V2 Update built in XML doesn't work** (`This operation type is not supported:null`). Workaround: a Standard Insert operation (`customOperationType="CREATE"`) whose query contains the `UPDATE` or `MERGE` with `?` placeholders. The JSON keys must be the **real column names, in UPPER CASE**. A MERGE allows only one `WHEN MATCHED` (error 10714): fold the cases together with `CASE`.
5. **Event Streams, Consume + acknowledgeLater:** messages are never acknowledged without an Acknowledge step. That step needs `consumerName`/`messageId` set by a Groovy script, and **it still fails** on this runtime ("Could not get connection from connection cache", seen on 2026-11-04). The chosen solution is **Listen with transacted acknowledgement** (`Scheduled_TransactionLog_Consumer`).
6. **Listen on the shared cloud runtime:** in the MySQL days, the subscription stayed `type=NONE` because there was no execution worker. The framework now runs on a different atom/environment. **Check that Listen actually attaches** (runs visible in Process Reporting) before treating the chain as in production.
7. **Message step + JSON: write `'{1}'`, not `{1}`.** The single quotes switch `{}` substitution on and off.
8. **`ErrorCode` must come from real business or connector code**, never be guessed from a free-text message.
9. **Changed subprocess:** you must redeploy the parent process. A parent only picks up a new subprocess version when the parent itself is deployed.
10. **Topics with an unconsumed backlog have disappeared on their own before** (seen on 2026-09-28, cause unknown). This is a residual risk to "zero data loss".

---

## 9. Open issues and known defects (as of 2026-11-05)

| # | Problem | Impact | Proposed fix |
|---|---|---|---|
| 1 | `Sub_Retrigger_Handler` still has a "TEMP TEST ONLY" Message that replaces the request with a fixed correlationId (`UNKNOWN_INTERFACE-20260928T094643…`). There's also an orphaned step (`shape30`) | The retrigger API ignores the ID it's sent | Remove `shapeTEST` and `shape30`, connect Start to `shape2`, then **redeploy `WSS_Retrigger_Handler`** (still at v1) |
| 2 | The template's Catch never sets `ErrorCode` | Copied interfaces classify every error as `UNKNOWN_CODE` | Add a Set Properties `ErrorCode` in the Catch (e.g. from `HTTP_Status_Code`) |
| 3 | `Sub_TransactionLog_Consumer` (Consume) and `Scheduled_TransactionLog_Consumer` (Listen) share the `p2p-transaction-log-consumer` subscription | If both are deployed, they compete for messages | Undeploy `Sub_TransactionLog_Consumer` and delete it once the Listen version is validated |
| 4 | `Sub_ErrorQueue_Consumer`: no error check after the INSERT, and Consume with `acknowledgeLater` without an Ack step | Silent loss or duplicates in `ERROR_QUEUE` | Switch it to a transacted Listen with a `shapeCHK`-style check, like the log consumer |
| 5 | Only 7 of the ~22 interfaces call `Central_Error_Handler` | Errors not classified or queued | Add the call to the Catch of the orchestrators concerned |
| 6 | `Scheduled_Purge_Retention` never tested; `SP_PURGE_RETENTION` missing from the script | No purge | Test it; if needed, switch to a Standard Insert that runs the `EXEC`, or to two DELETEs |
| 7 | Leftovers to clean up: `Diag_*`, `Get_TransactionLog_SelectIdByCorrelation_V2` and its 2 profiles, `MySQL_JDBC` (still deployed) | Clutter | Delete them; for `MySQL_JDBC`, undeploy it first |
| 8 | The descriptions of `TEMPLATE_Standard_P2P_Parent` and `Sub_Execution_Logger` still mention MySQL, a "synchronous insert" and a "Listen consumer not built yet" | Misleading documentation on the platform | Update those descriptions |

---

## 10. Don't trust the sync-state or local copies

- **The sync-state:** on 2026-09-24, the IDs in `active-development/.sync-state/` for the framework processes were all invalid on the platform.
- **Local copies:** on 2026-11-05, several XML files in `active-development/process/` were one or two versions behind (e.g. `Central_Error_Handler` at v16 locally against v17 on the platform, changed by another developer).
- **This guide itself** is only a snapshot at a given date.

**Rule:** before wiring or changing anything, re-check the ID with `boomi-component-search.sh --folder Rjo4ODM3OTY0` (or `--name "%Name%"`) and **pull** the component again.

---

## 11. Useful commands

```bash
# Full framework inventory (root folder, not 1-Development)
bash <skill-path>/scripts/boomi-component-search.sh --folder Rjo4ODM3OTY0

# What calls / references a component
bash <skill-path>/scripts/boomi-component-search.sh --related-to <componentId>

# Test a subprocess on its own (it must be deployed on its own first)
bash <skill-path>/scripts/boomi-test-execute.sh --process-id <componentId>

# Test the manual retrigger API
bash <skill-path>/scripts/boomi-wss-test.sh --path /ws/simple/executeRetrigger \
  --data '{"correlationId":"..."}'

# Latest runs of a process, then logs of one run
bash <skill-path>/scripts/boomi-execution-query.sh --process-id <componentId> --limit 5
bash <skill-path>/scripts/boomi-execution-query.sh --execution-id <id> --logs
```


---
---

<a id="francais"></a>

# Framework P2P Endeavour Mining — comment ça marche, comment l'utiliser

**Compte Boomi :** compte Endeavour (ID dans `BOOMI_ACCOUNT_ID` du `.env`, jamais dans ce fichier) — dossier `Endeavour Management Services London Limited/edv.framework` (`folderId Rjo4ODM3OTY0`).
**Vérifié contre la plateforme le 2026-11-05** : la liste des composants et leurs IDs viennent d'une recherche `boomi-component-search` faite ce jour-là ; les 14 process ont été récupérés dans leur version courante. Rien n'a été repris d'un ancien doc ni du sync-state local (voir § 10).

> **Attention, il existe deux dossiers `edv.framework`.** Le seul qui compte est celui à la racine du compte (`Rjo4ODM3OTY0`, 66 composants). `1-Development/edv.framework/Shared_Services/Logging` est **vide**. Une recherche `--folder "%edv.framework%"` renvoie les deux.

Ce document décrit le framework **réutilisable** : les sous-process partagés, les topics Event Streams et le schéma SQL Server. Il ne décrit pas une interface métier en particulier (Cost Object, S2C GAN, INVPO…) ; ces interfaces sont construites *avec* le framework.

**Historique :**
- **2026-09-14** : première version, en MySQL (hébergé) avec le connecteur Database (Legacy).
- **2026-09-25** : migration vers SQL Server avec le connecteur Database V2.
- **2026-09-28/29** : refonte « résilience ». Les process n'écrivent plus directement en base : ils passent par Event Streams.
- **2026-11-03/05** : le consommateur Listen écrit avec un seul MERGE, l'Update est contourné par une Standard Insert, et le préfixe du CorrelationId devient l'InterfaceId.
- **2026-11-05** : suppression des 30 composants legacy MySQL inutilisés (liste dans `active-development/inventories/edv_framework_deleted_20261105.tsv`).

---

## 1. Pourquoi ce framework existe

Règle établie le 2026-09-16 : **toute nouvelle interface P2P (EAM, Ivalua, GAN ou un futur système) doit être construite à partir de `TEMPLATE_Standard_P2P_Parent`**. On ne construit jamais un flow autonome Listen/Poll → Map → Send avec sa propre gestion d'erreur locale. Ajouter une interface doit se résumer à « copier le template et ajouter une ligne dans `INTERFACE_REGISTRY` », sans refaire à chaque fois la corrélation, le logging et la gestion d'erreur.

**Principe de résilience** (exigence utilisateur, 2026-09-28) : une courte panne de SQL Server ne doit faire perdre **aucune** donnée. Le connecteur Database V2 ne relance pas une connexion qui échoue. Le chemin d'exécution d'une interface n'écrit donc **jamais directement en base** : il publie seulement sur Event Streams, et des consommateurs asynchrones écrivent dans SQL Server.

---

## 2. Vue d'ensemble du flux

```
TEMPLATE_Standard_P2P_Parent (à copier pour chaque interface)
  Start
   → Set Properties "Set Interface Data"
       (InterfaceName/InterfaceId/ObjectType/BusinessKey/SourceSystem/TargetSystem/Environment)
   → [Process Call] Sub_Generate_CorrelationId
        génère CorrelationId + TransactionId, LogPhase=START
        → [Process Call] Sub_Execution_Logger ──► Event Streams  edv.transaction.log  (événement START)
   → Try/Catch
       Try:
         → PLACEHOLDER logique métier (connecteurs/maps de l'interface ; remplace ce Notify)
         → LogPhase=SUCCESS
         → [Process Call] Sub_Execution_Logger ──► edv.transaction.log  (événement SUCCESS)
         → Stop (continue)
       Catch:
         → LogPhase=ERROR
         → [Process Call] Central_Error_Handler
              Get ERROR_CODE_REGISTRY (sous Try/Catch, valeur de repli sûre si la base ne répond pas)
              ├─ Retryable  → retour "Classified-Retryable" (rien n'est mis en file)
              └─ sinon      → publie ──► Event Streams  edv.transactions.error → retour "Classified-Queued"
         → [Process Call] Sub_Execution_Logger ──► edv.transaction.log  (événement ERROR)
         → Stop (continue=false)

Consommateurs asynchrones (seuls process qui écrivent dans SQL Server) :
  edv.transaction.log    ──► Scheduled_TransactionLog_Consumer (Listen)  ── MERGE dans TRANSACTION_LOG
  edv.transactions.error ──► Sub_ErrorQueue_Consumer (Consume)           ── INSERT dans ERROR_QUEUE

À côté :
  WSS_Retrigger_Handler → Sub_Retrigger_Handler   API manuelle de retrigger
  Scheduled_Purge_Retention                       purge selon la durée de rétention
```

Si la base est en panne, les lignes apparaissent en retard, mais aucune n'est perdue, **à condition que la publication sur Event Streams réussisse**.

---

## 3. Inventaire des composants (`edv.framework`, 66 composants au 2026-11-05)

### Process

| Process | componentId | Version | Rôle |
|---|---|---|---|
| `TEMPLATE_Standard_P2P_Parent` | `4c3f5a28-eaae-4d08-a973-892d6c3ecda7` | v5 | Squelette à **copier** pour chaque nouvelle interface |
| `Sub_Generate_CorrelationId` | `f79c2fe3-e612-4c12-93a1-ac48455fc191` | v16 | Génère CorrelationId (`InterfaceId-timestamp-unique`) et TransactionId (UUID), puis publie l'événement START |
| `Sub_Execution_Logger` | `73ce504a-48cf-4f2c-9616-94440e7ce77f` | v4 | Construit l'événement JSON et le publie sur `edv.transaction.log` |
| `Central_Error_Handler` | `26ca08e9-68db-418d-bfef-b8ce452ce9b1` | v17 | Classe l'erreur et, si elle n'est pas retryable, publie sur `edv.transactions.error` |
| `Scheduled_TransactionLog_Consumer` | `6e731412-a277-4b1f-83be-3338bd141d52` | v7 | **Consommateur actuel** de `edv.transaction.log` : Listen avec acquittement transactionnel, un MERGE par événement |
| `Sub_TransactionLog_Consumer` | `8daf258f-72f7-4c33-9e24-6db5b6b0e1a2` | v14 | **Ancien** consommateur (Consume + planification), remplacé le 2026-11-05. Voir § 9 |
| `Sub_ErrorQueue_Consumer` | `b08bd048-de08-4220-999a-087f82bbd909` | v3 | Lit `edv.transactions.error` (Consume) et fait l'INSERT dans `ERROR_QUEUE` |
| `Sub_Retrigger_Handler` | `bbfdb162-52a4-4dc8-ae8a-4da97c96d061` | v7 | Logique du retrigger manuel |
| `WSS_Retrigger_Handler` | `7974b1f4-fae2-45bd-9c61-be804ed00133` | v1 | Listener WSS minimal : `POST /ws/simple/executeRetrigger` |
| `Scheduled_Purge_Retention` | `b7cd1d66-f934-41a0-806e-7c8359a82207` | v4 | Appelle `EDV_INTEGRATION.SP_PURGE_RETENTION` |
| `Util_Seed_InterfaceRegistry` | `df032bae-22f9-427d-945b-971eaed880c3` | v4 | Exécuté une seule fois : insère les 5 interfaces Cost Object. **Modèle à copier** |
| `Util_Seed_UnknownErrorCode` | `8fb458ea-14f5-411b-8f4c-1854df4f1117` | v1 | Exécuté une seule fois (déjà fait) : crée la ligne `UNKNOWN_CODE` dans `ERROR_CODE_REGISTRY` |
| `Diag_DbCheck` / `Diag_SeedTemplateInterface` | `3489ec68-…` / `308e6443-…` | — | Process de diagnostic temporaires, **à supprimer** (c'est écrit dans leur propre description) |

### Connexions et bibliothèque

| Composant | componentId | Rôle |
|---|---|---|
| `EDV_INTEGRATION (SQL)` | `2187b6a8-fab6-4832-a7f1-4eb3bc36a2e8` | Connexion Database V2 vers SQL Server, schéma `EDV_INTEGRATION` |
| `P2P_Framework_EventStreams_Connection` | `54d36e66-c760-45a9-91b2-aa28f83a43d2` | Connexion Event Streams (sert à publier et à consommer) |
| `Email_request` | `94d4ffec-8b57-4640-b74f-4481a06e1f5a` | Ajoutée par un autre développeur le 2026-11-04 ; aucun process ne l'utilise encore |
| `MySQL_JDBC` | `e2ccde21-4884-4bf8-922f-80b826cd9808` | Reste de l'époque MySQL, plus référencée, mais **encore déployée** : elle n'a pas pu être supprimée et est conservée volontairement |

### Opérations Database V2 (toutes sur `EDV_INTEGRATION (SQL)`)

| Opération | Utilisée par |
|---|---|
| `Send_TransactionLog_Upsert_V2` (MERGE) | `Scheduled_TransactionLog_Consumer` |
| `Send_TransactionLog_Insert_V2`, `Send_TransactionLog_UpdateStatus_V2` | `Sub_TransactionLog_Consumer` (ancien) |
| `Get_ErrorCodeRegistry_ByCode_V2` | `Central_Error_Handler` |
| `Send_ErrorQueue_Insert_V2` | `Sub_ErrorQueue_Consumer` |
| `Get_TransactionLog_SelectByCorrelation_ForRetrigger_V2`, `Get_ErrorQueue_SelectLatestByTransaction_V2`, `Get_PayloadStore_SelectLatestByTransaction_V2`, `Get_RetryHistory_NextNumber_V2`, `Send_RetryHistory_Insert_V2` | `Sub_Retrigger_Handler` |
| `Send_SP_PurgeRetention_V2` | `Scheduled_Purge_Retention` |
| `Send_InterfaceRegistry_Insert_V2` (+ map `Map_InterfaceRegistrySeed_to_InterfaceRegistry_V2`) | `Util_Seed_InterfaceRegistry`, `Diag_SeedTemplateInterface` |
| `Send_ErrorCodeRegistry_Insert_V2` | `Util_Seed_UnknownErrorCode` |
| `Get_TransactionLog_SelectIdByCorrelation_V2` | **aucun process** (à supprimer si personne n'en a l'usage) |
| `Diag_Get_DbCheck_V2` | `Diag_DbCheck` |

Les autres composants sont les profils JSON de requête et de réponse de ces opérations (`*_V2_Request_JSON` / `*_V2_Response_JSON`), plus `P2P_TransactionLogEvent_JSON` (le format de l'événement), `DatabaseV2_WriteResponse_JSON`, et `RetriggerRequest_JSON`/`RetriggerResponse_JSON` pour l'API WSS.

### Qui utilise le framework (au 2026-11-05)

- **Environ 22 process** appellent `Sub_Generate_CorrelationId` et `Sub_Execution_Logger` : Cost Objects (CO_*), FX_RATES, GL, FCT, INVPO / NON_INVPO (requête et ack), INV_RCPT / NON_INVRCPT, INV_OKPAY, PAY_BATCH, PAY_CONFIRMATION, COMMON-CATCH-PROCESS et les deux interfaces S2C GAN.
- **Seulement 7** appellent `Central_Error_Handler` : COMMON-CATCH-PROCESS, GL_ORCHESTRATOR, INVPO_ACK_LISTEN, INVPO_ORCHESTRATOR, le template et les deux interfaces S2C GAN. Les autres journalisent leurs erreurs, mais ne les classent pas et ne les mettent pas en file.

Pour retrouver cette liste : `boomi-component-search.sh --related-to <componentId>`.

---

## 4. Topics Event Streams

| Topic | Producteur | Subscription | Consommateur |
|---|---|---|---|
| `edv.transaction.log` | `Produce_P2P_ExecutionLog_Event` (dans `Sub_Execution_Logger`) | `p2p-transaction-log-consumer` (Shared) | `Scheduled_TransactionLog_Consumer` via `Listen_TransactionLog_Consumer` (transacted, `maxRetries=5`, `ackTimeout=60`, puis dead-letter queue) |
| `edv.transactions.error` | `Produce_P2P_ErrorQueueEvent` (dans `Central_Error_Handler`) | `p2p-transactions-error-consumer` (Shared) | `Sub_ErrorQueue_Consumer` via `Consume_P2P_ErrorQueueEvent` (`acknowledgeLater=true`, `maxMessages=10`) |

Les anciens topics `esb.p2p.transaction.log` et `esb.transaction.log` **n'existent plus** depuis le 2026-09-29. Tout passe par la convention `edv.*`.

L'événement de log (profil `P2P_TransactionLogEvent_JSON`) contient : correlationId, transactionId, interfaceName, interfaceId, logPhase (`START`/`SUCCESS`/`ERROR`), errorCategory, objectType, businessKey, sourceSystem, targetSystem, environment et eventTimestamp.

---

## 5. Schéma SQL Server (schéma `EDV_INTEGRATION`, script `script_create_framework.sql`)

| Table | Rôle | Clé |
|---|---|---|
| `INTERFACE_REGISTRY` | Catalogue des interfaces autorisées | PK `INTERFACE_ID` (VARCHAR 20) |
| `TRANSACTION_LOG` | Une ligne par exécution d'interface ; c'est le cœur du framework | PK `TRANSACTION_ID` (UNIQUEIDENTIFIER), FK `INTERFACE_ID` vers `INTERFACE_REGISTRY` |
| `ERROR_QUEUE` | Erreurs non retryables, en attente d'un retrigger manuel | FK `TRANSACTION_ID`, FK `ERROR_CODE` vers `ERROR_CODE_REGISTRY` |
| `RETRY_HISTORY` | Historique des tentatives de retrigger | FK `TRANSACTION_ID` |
| `PAYLOAD_STORE` | Référence au payload d'origine (`PAYLOAD_URI`), pas son contenu | FK `TRANSACTION_ID` |
| `ERROR_CODE_REGISTRY` | Classement des codes d'erreur (`BUSINESS`/`TECHNICAL`, `RETRYABLE`) | PK `ERROR_CODE` |
| `CONFIG` | Paramètres (retry, rétention, notifications) | PK `CONFIG_KEY` |
| `AUDIT_LOG`, `NOTIFICATION_LOG`, `BUSINESS_EVENT` | Prévues, mais pas branchées sur le chemin standard | — |

Colonnes de `TRANSACTION_LOG` : `TRANSACTION_ID`, `CORRELATION_ID`, `INTERFACE_ID`, `OBJECT_TYPE`, `BUSINESS_KEY`, `SOURCE_SYSTEM`, `TARGET_SYSTEM`, `STATUS`, `START_DATE`, `END_DATE`, `DURATION_MS`, `ENVIRONMENT`, `CREATED_DATE`.

**La base réelle ne correspond pas au script pour `ERROR_CODE_REGISTRY`.** Le script contient `MISSING_VAT_NUMBER`/`INVALID_PO_LINE`. La table en production contient `CONNECTION_FAILURE`, `HTTP_500`, `HTTP_502`, `HTTP_503`, `INVALID_MAPPING`, `MISSING_DATA`, `TIMEOUT` et `UNKNOWN_CODE` (ce dernier ajouté par `Util_Seed_UnknownErrorCode`). Pour choisir un code de test, interroge la vraie table, pas le script.

**`CONFIG`** : `MAX_RETRY_COUNT=3`, `AUTO_RETRY_ENABLED=TRUE`, `PAYLOAD_RETENTION_DAYS=90`, `ERROR_RETENTION_DAYS=365`, `RETRIGGER_ENABLED=TRUE`, `NOTIFICATION_TEAMS_ENABLED=TRUE`, `NOTIFICATION_EMAIL_ENABLED=TRUE`.

`SP_PURGE_RETENTION` n'est **pas** dans `script_create_framework.sql`. Vérifie qu'elle existe bien en base avant de compter sur la purge.

---

## 6. Comment ça marche, sous-process par sous-process

### `Sub_Generate_CorrelationId`
- Génère le `CorrelationId` avec un Set Properties natif, sous la forme `InterfaceId-timestamp-unique` (préfixe `INTERFACE_ID`, par ex. `S2C_SUP`, depuis le 2026-11-04).
- Génère le `TransactionId` en UUID dans un petit script Groovy. C'est la seule exception volontaire au « zéro Groovy ».
- Pose `LogPhase=START` et appelle `Sub_Execution_Logger`. **Il n'écrit plus en base.**
- L'appelant doit avoir posé avant les propriétés `InterfaceId`/`ObjectType`/`BusinessKey`/`SourceSystem`/`TargetSystem`/`Environment`. Elles sont recopiées dans l'événement pour que le consommateur ait tout ce qu'il faut pour créer la ligne.

### `Sub_Execution_Logger`
Construit l'événement JSON et le publie (Produce) sur `edv.transaction.log`. Le process appelant n'attend aucune réponse ; l'écriture en base se fait de façon asynchrone par le consommateur.

### `Central_Error_Handler`
**Contrat d'appel : l'appelant doit poser la propriété de process `ErrorCode`** avant d'appeler ce sous-process. Elle doit venir d'une vraie propriété du connecteur (par ex. `HTTP_Status_Code`) ; on ne parse jamais un message d'erreur libre. Si elle est absente, la valeur par défaut est `UNKNOWN_CODE` (`TECHNICAL`, non retryable).

Déroulement :
1. Un Get sur `ERROR_CODE_REGISTRY`, protégé par un Try/Catch. Si la base est injoignable, une valeur de repli sûre est utilisée (`TECHNICAL`, non retryable).
2. Il pose `ErrorCategory` et `Retryable`.
3. Il route :
   - `Retryable=true` : il journalise seulement et renvoie **`Classified-Retryable`**. On considère que le retry natif du connecteur a déjà eu lieu.
   - sinon : il publie sur `edv.transactions.error` et renvoie **`Classified-Queued`**.

### `Scheduled_TransactionLog_Consumer` — consommateur actuel de `edv.transaction.log`
- **Démarrage :** Listen avec acquittement transactionnel. Un message n'est retiré du topic que si l'exécution se termine en succès. En cas d'exception, il est relivré jusqu'à 5 fois, puis part dans la dead-letter queue.
- **Ne pas** le planifier : Listen s'en charge.
- **Ne pas** le déployer en même temps que `Sub_TransactionLog_Consumer` : ils partagent la même subscription.

Déroulement :
1. Il extrait le CorrelationId.
2. Un script Groovy convertit le timestamp de l'événement au format `yyyy-MM-dd HH:mm:ss.SSS`.
3. **Un seul MERGE** (`Send_TransactionLog_Upsert_V2`) traite n'importe quel événement (START, SUCCESS ou ERROR), dans n'importe quel ordre d'arrivée.
4. Un contrôle Groovy (`shapeCHK`) **fait échouer l'exécution** si le MERGE a renvoyé une erreur SQL. Sans ce contrôle, l'exécution finirait en COMPLETE (voir § 8) et le message serait perdu.

### `Sub_TransactionLog_Consumer` — ancien consommateur (Consume)
Version précédente, remplacée le 2026-11-05.
- Elle traite d'abord les insertions, puis les clôtures (Branch).
- Pour une clôture dont la ligne n'existe pas encore : si l'événement a **moins de 15 min**, elle lève `CLOSEOUT_ROW_NOT_FOUND` pour qu'il soit retraité ; s'il est plus ancien, elle le traite une seule fois.
- **Problème de fond :** avec `acknowledgeLater=true`, Consume n'acquitte rien sans étape Acknowledge, et l'Acknowledge échoue sur ce runtime (§ 8). Les messages reviennent donc sans fin.

### `Sub_ErrorQueue_Consumer`
- Consume sur `edv.transactions.error`, puis Message, puis `Send_ErrorQueue_Insert_V2`, le tout dans un Try/Catch.
- **Doit être planifié** depuis l'interface Boomi : Atom Management → Scheduling.
- Il a le même problème d'acquittement que l'ancien consommateur (§ 9).

### `Sub_Retrigger_Handler` / `WSS_Retrigger_Handler`
API manuelle : `POST /ws/simple/executeRetrigger` avec `{"correlationId": "..."}`. Le sous-process :
1. retrouve `TRANSACTION_ID` (et le statut) dans `TRANSACTION_LOG` ; renvoie **`NotFound`** s'il ne le trouve pas ;
2. lit la dernière ligne de `ERROR_QUEUE` et celle de `PAYLOAD_STORE`. La recherche se fait par `TRANSACTION_ID`, qui est la vraie clé étrangère ;
3. calcule le `RETRY_NUMBER` suivant (`Get_RetryHistory_NextNumber_V2`) et insère une ligne dans `RETRY_HISTORY` ;
4. renvoie **`Retriggered`** avec `payload_uri`.

**Il ne renvoie pas automatiquement le message au système cible.** Aucun mécanisme générique ne le permet ; il expose seulement les informations utiles à un opérateur. Le wrapper WSS relie les deux retours (`Retriggered` vers shape18, `NotFound` vers shape21) à une même réponse.

### `Scheduled_Purge_Retention`
- Start sans données, puis un Message `{}`, puis `Send_SP_PurgeRetention_V2` (`EXEC EDV_INTEGRATION.SP_PURGE_RETENTION`), sous Try/Catch.
- **Doit être planifié** depuis l'interface Boomi.
- **Pas vérifié avec de vraies données.** L'opération est un Dynamic Update construit en XML, et ce type d'Update échoue sur ce compte (§ 8, piège n°4). Il faudra probablement passer par une Standard Insert qui exécute l'`EXEC`.

### `Util_Seed_InterfaceRegistry` — modèle à copier, pas un composant du framework
- Process lancé une seule fois avec `boomi-test-execute.sh`, jamais déployé.
- Enchaîne Message (tableau de lignes), puis `Map_InterfaceRegistrySeed_to_InterfaceRegistry_V2` (un document par ligne), puis `Send_InterfaceRegistry_Insert_V2`.
- **`INTERFACE_ID` est une clé primaire** : relancer le process en double provoque une erreur de clé dupliquée.

---

## 7. Comment ajouter une nouvelle interface

1. **Ajouter la ligne dans `INTERFACE_REGISTRY`** (`INTERFACE_ID`, `DOMAIN_NAME`, `INTERFACE_NAME`, `SOURCE_SYSTEM`, `TARGET_SYSTEM`). Tu peux copier le modèle `Util_Seed_InterfaceRegistry` ou faire l'INSERT en SQL. **Elle doit exister avant le premier vrai run** : sans elle, le MERGE dans `TRANSACTION_LOG` est rejeté par la clé étrangère. Et à cause du piège n°1 (§ 8), ce rejet peut passer inaperçu.
2. **Copier `TEMPLATE_Standard_P2P_Parent`** dans le dossier du projet de l'interface (pull puis create sous un nouveau nom, ou duplication dans l'interface Boomi suivie d'un pull).
3. Dans « Set Interface Data » : renseigner `InterfaceName`, `InterfaceId` (qui sert de préfixe au CorrelationId), `ObjectType`, `SourceSystem`, `TargetSystem`, `Environment`, et `BusinessKey` si besoin.
4. Remplacer le Notify placeholder par la vraie logique métier.
5. **Poser `ErrorCode` toi-même.** Avant tout point qui peut échouer, ou dans le Catch avant l'appel à `Central_Error_Handler`, donne une valeur à la propriété de process `ErrorCode` à partir d'une vraie propriété du connecteur. **Le template ne le fait pas** (§ 9) : si tu l'oublies, toutes les erreurs seront classées `UNKNOWN_CODE` (non retryable).
6. Ajouter dans `ERROR_CODE_REGISTRY` les codes d'erreur propres à l'interface (`ERROR_CATEGORY` ∈ {`BUSINESS`, `TECHNICAL`}, `RETRYABLE`).
7. **Ne pas toucher** aux appels (Process Call) vers `Sub_Generate_CorrelationId`, `Sub_Execution_Logger` et `Central_Error_Handler`. C'est ce qui rend l'interface conforme au framework.
8. Faire le push, déployer et tester. Vérifie ensuite **la ligne dans `TRANSACTION_LOG`** ; le statut COMPLETE de l'exécution ne suffit pas (§ 8).

---

## 8. Pièges connus (constatés en pratique)

1. **COMPLETE ne veut pas dire que l'écriture a réussi.** Toutes les opérations Database V2 tournent avec `returnApplicationErrors=true`. Un INSERT ou un UPDATE en échec revient alors comme un document JSON normal (`{"errorCode":…}`), et l'exécution se termine en COMPLETE. Vérifie toujours `"Status":"Executed Successfully"` et `"Rows Effected"`, ou fais un `SELECT` en base. Dans un consommateur, il faut un contrôle qui fait échouer l'exécution (voir `shapeCHK`).
2. **Paramètres d'un Get Database V2 :** le nom `$param` (et le champ correspondant dans le profil JSON de requête) doit être **exactement le nom de la colonne SQL**, casse comprise (`$ERROR_CODE`, pas `$ErrorCode`). Sinon on obtient l'erreur 405 « Keys Provided in Input Request is not matching the Request Profile ».
3. **Dates envoyées à SQL Server :** utiliser `yyyy-MM-dd HH:mm:ss.SSS`, avec une espace et **sans `T`**. Le format ISO avec `T` est rejeté (erreur 405). Cela ne concerne que les dates passées en paramètre SQL, pas celles mises dans un payload JSON ou un CorrelationId.
4. **Un Update Database V2 construit en XML ne fonctionne pas** (`This operation type is not supported:null`). Contournement : une opération Standard Insert (`customOperationType="CREATE"`) dont la requête contient l'`UPDATE` ou le `MERGE` avec des `?`. Les clés JSON doivent être les **vrais noms de colonnes, en MAJUSCULES**. Un seul `WHEN MATCHED` est autorisé par MERGE (erreur 10714) : il faut regrouper les cas avec `CASE`.
5. **Event Streams, Consume + acknowledgeLater :** les messages ne sont jamais acquittés sans étape Acknowledge. Cette étape doit recevoir `consumerName`/`messageId` posés par un script Groovy, et **elle échoue quand même** sur ce runtime (« Could not get connection from connection cache », constaté le 2026-11-04). La solution retenue est **Listen avec acquittement transactionnel** (`Scheduled_TransactionLog_Consumer`).
6. **Listen sur le runtime cloud partagé :** à l'époque MySQL, la subscription restait `type=NONE`, faute de worker d'exécution. Le framework tourne maintenant sur un autre atom/environnement. **Vérifie que Listen s'attache bien** (exécutions visibles dans Process Reporting) avant de considérer que la chaîne est en production.
7. **Message step + JSON : écrire `'{1}'` et pas `{1}`.** Les guillemets simples activent ou désactivent la substitution `{}`.
8. **`ErrorCode` doit venir du vrai code métier ou connecteur**, jamais être deviné à partir d'un message libre.
9. **Sous-process modifié :** il faut redéployer le process parent. Un parent n'intègre une nouvelle version de sous-process qu'au moment de son propre déploiement.
10. **Les topics dont le backlog n'est pas consommé ont déjà disparu d'eux-mêmes** (observé le 2026-09-28, cause inconnue). C'est un risque résiduel pour le « zéro perte de données ».

---

## 9. Points ouverts et défauts connus (au 2026-11-05)

| # | Problème | Impact | Correctif proposé |
|---|---|---|---|
| 1 | `Sub_Retrigger_Handler` garde un Message « TEMP TEST ONLY » qui remplace la requête par un correlationId fixe (`UNKNOWN_INTERFACE-20260928T094643…`). Il y a aussi une étape orpheline (`shape30`) | L'API de retrigger ignore l'ID qu'on lui envoie | Supprimer `shapeTEST` et `shape30`, relier Start à `shape2`, puis **redéployer `WSS_Retrigger_Handler`** (encore en v1) |
| 2 | Le Catch du template ne pose jamais `ErrorCode` | Les interfaces copiées classent toutes leurs erreurs en `UNKNOWN_CODE` | Ajouter dans le Catch un Set Properties `ErrorCode` (par ex. depuis `HTTP_Status_Code`) |
| 3 | `Sub_TransactionLog_Consumer` (Consume) et `Scheduled_TransactionLog_Consumer` (Listen) partagent la subscription `p2p-transaction-log-consumer` | S'ils sont déployés tous les deux, ils se disputent les messages | Annuler le déploiement de `Sub_TransactionLog_Consumer` et le supprimer une fois le Listen validé |
| 4 | `Sub_ErrorQueue_Consumer` : aucun contrôle d'erreur après l'INSERT, et Consume avec `acknowledgeLater` sans étape Ack | Perte silencieuse ou doublons dans `ERROR_QUEUE` | Le passer en Listen transactionnel avec un contrôle de type `shapeCHK`, comme le consommateur de log |
| 5 | Seulement 7 des ~22 interfaces appellent `Central_Error_Handler` | Erreurs non classées et non mises en file | Ajouter l'appel dans le Catch des orchestrateurs concernés |
| 6 | `Scheduled_Purge_Retention` jamais testé ; `SP_PURGE_RETENTION` absent du script | Pas de purge | Tester ; si besoin, passer à une Standard Insert qui exécute l'`EXEC` ou à deux DELETE |
| 7 | Restes à nettoyer : `Diag_*`, `Get_TransactionLog_SelectIdByCorrelation_V2` et ses 2 profils, `MySQL_JDBC` (encore déployée) | Encombrement | Supprimer ; pour `MySQL_JDBC`, annuler son déploiement d'abord |
| 8 | Les descriptions de `TEMPLATE_Standard_P2P_Parent` et `Sub_Execution_Logger` parlent encore de MySQL, d'un « insert synchrone » et d'un « consommateur Listen pas encore construit » | Documentation sur la plateforme trompeuse | Mettre ces descriptions à jour |

---

## 10. Ne pas faire confiance au sync-state ni aux copies locales

- **Le sync-state :** le 2026-09-24, les IDs de `active-development/.sync-state/` pour les process du framework étaient tous invalides sur la plateforme.
- **Les copies locales :** le 2026-11-05, plusieurs XML de `active-development/process/` étaient en retard d'une ou deux versions (par ex. `Central_Error_Handler` en v16 en local contre v17 sur la plateforme, modifié par un autre développeur).
- **Ce guide lui-même** n'est qu'une photo à une date donnée.

**Règle :** avant de câbler ou de modifier quoi que ce soit, revérifie l'ID avec `boomi-component-search.sh --folder Rjo4ODM3OTY0` (ou `--name "%Nom%"`) et **refais un pull** du composant.

---

## 11. Commandes utiles

```bash
# Inventaire complet du framework (dossier racine, pas 1-Development)
bash <skill-path>/scripts/boomi-component-search.sh --folder Rjo4ODM3OTY0

# Qui appelle / référence un composant
bash <skill-path>/scripts/boomi-component-search.sh --related-to <componentId>

# Tester un sous-process isolément (il doit d'abord être déployé seul)
bash <skill-path>/scripts/boomi-test-execute.sh --process-id <componentId>

# Tester l'API de retrigger manuel
bash <skill-path>/scripts/boomi-wss-test.sh --path /ws/simple/executeRetrigger \
  --data '{"correlationId":"..."}'

# Dernières exécutions d'un process, puis logs d'une exécution
bash <skill-path>/scripts/boomi-execution-query.sh --process-id <componentId> --limit 5
bash <skill-path>/scripts/boomi-execution-query.sh --execution-id <id> --logs
```
