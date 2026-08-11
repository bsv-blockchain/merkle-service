# Tasks: oversize-callback-recovery

## 1. Failure classification

- [x] 1.1 Add `oversizeDeliveryError` (carrying `bodyBytes`, `limitBytes`,
      `statusCode`) with `errOversizeDelivery` / `asOversizeDeliveryError`,
      documented against the 2026-08-11 dev-ovh-1 incident
- [x] 1.2 `deliverCallback`: map a `413 Request Entity Too Large` response to
      the oversize class, ahead of the `isNonRetryable4xx` check
- [x] 1.3 Remove 413 from `isNonRetryable4xx` so no future caller can
      silently restore "413 == permanent == DLQ == stranded block"; comment
      the hazard inline

## 2. Routing

- [x] 2.1 `scheduleRetryOrDLQ`: oversize branch evaluated BEFORE the
      permanent check — retry while budget remains, DLQ with a distinct
      `BLOCK STRANDED:` ERROR once exhausted
- [x] 2.2 Never call `recordCallbackURLFailure` on the oversize path (a
      single registered arcade URL would be auto-disabled inside one block,
      killing all callback delivery)
- [x] 2.3 Extract the retry tail into `scheduleRetry` so the oversize branch
      and the normal fall-through share one backoff ladder and one
      durability contract

## 3. Pre-flight size gate

- [x] 3.1 `CallbackConfig.MaxBodyBytes` (yaml `maxBodyBytes`, mapstructure
      `maxbodybytes`), viper default **0 = disabled**, env
      `CALLBACK_MAX_BODY_BYTES`
- [x] 3.2 `deliverCallback`: after the body is finished (cache hit included),
      refuse to POST when `MaxBodyBytes > 0` and the body exceeds it; same
      oversize class, `statusCode` 0
- [x] 3.3 Unconditional WARN above `bodyWarnBytes` (8 MiB const, half
      arcade's original default) naming block, subtree and size
- [x] 3.4 Document both in `config.yaml`, including that hex DOUBLES the
      blob and that dev-ovh-1 runs 128 MiB since
      teranode-argocd-deployments#211
- [x] 3.5 Surface `maxBodyBytes` / `bodyWarnBytes` in the delivery init log
- [x] 3.6 Bound the STUMP body cache by total bytes (`bodyCacheMaxBytes`,
      64 MiB) as well as entry count. The existing 64-entry bound assumed
      ~545 KB bodies; with arcade's cap now 128 MiB a count-only bound admits
      64 x 128 MiB ~= 8 GiB resident and OOM-kills the delivery pod, escalating
      "one block's STUMPs are oversized" into "callback delivery is dead". An
      over-budget single body is still cached (alone), since refusing it would
      restore the per-subscriber re-fetch/re-hex/re-marshal work the cache
      exists to remove

## 4. Observability

- [x] 4.1 `metrics.OutcomeOversize` — one per rejected attempt
- [x] 4.2 `metrics.OutcomeOversizeStranded` — retries exhausted → DLQ; the
      page-worthy "this block cannot be finalized" signal, deliberately
      separate from `OutcomeDLQ`
- [x] 4.3 `logOversize`: single ERROR naming callback URL, type, block hash,
      subtree index, body size, limit, status, stump ref and retry count

## 5. Tests

- [x] 5.1 `internal/callback/delivery_oversize_test.go`:
      `TestDeliverCallback_413IsNotPermanent`,
      `TestIsNonRetryable4xx_413IsExcluded`,
      `TestProcessDelivery_413DoesNotGoStraightToDLQ` (the regression test:
      0 DLQ, 1 retry republish, retryCount incremented, breaker untouched),
      `TestProcessDelivery_413ExhaustedRetriesStrandsLoudlyWithoutTrippingBreaker`,
      `TestDeliverCallback_413LogsBlockIdentityAndSize`,
      `TestDeliverCallback_PreflightMaxBodyBytesRefusesThePost`,
      `TestDeliverCallback_PreflightDisabledByDefault`,
      `TestHandleMessage_413RepublishesBeforeAck`,
      `TestDeliverCallback_BodyWarnThresholdLogs`,
      `TestStoreBody_ByteBudgetEvicts`,
      `TestStoreBody_EntryBudgetStillEvicts`,
      `TestStoreBody_OversizedSingleBodyIsStillCached`,
      `TestStoreBody_DuplicateKeyDoesNotDoubleCount`
- [x] 5.2 `internal/config/config_test.go`:
      `TestLoad_CallbackMaxBodyBytesDefaultsToDisabled`,
      `TestLoad_CallbackMaxBodyBytesEnvOverride`; add
      `CALLBACK_MAX_BODY_BYTES` to `clearConfigEnv`

## 6. OpenSpec

- [x] 6.1 Update `openspec/specs/unified-callback-topic/spec.md` with the
      oversize class and the qualified DLQ requirement
- [x] 6.2 Add this change folder (proposal / design / tasks / spec delta)

## 7. Validation

- [x] 7.1 `go build ./...`, `go vet ./...`, `go test ./...`
- [x] 7.2 `make lint` (golangci-lint + lint-logfields)
- [x] 7.3 Pre-existing `internal/store` failures (Aerospike `merkle`
      namespace absent in the dev environment) confirmed identical on
      `origin/main`

## 8. Follow-up (NOT in this PR)

- [ ] 8.1 Set `CALLBACK_MAX_BODY_BYTES` in `deploy/k8s/callback-delivery.yaml`
      to match whatever `ARCADE_CALLBACK_MAX_BODY_BYTES` the target
      environment runs (dev-ovh-1: `134217728`)
- [ ] 8.2 Alert on `merkle_callback_messages_total{outcome="oversize_stranded"} > 0`
      (page) and `{outcome="oversize"}` rate (warn)
- [ ] 8.3 Claim-check STUMP delivery — needs a coordinated arcade change; see
      `design.md` for the four-step sketch
- [ ] 8.4 Remove the dead `CALLBACK_STUMP_CACHE_MODE` /
      `CALLBACK_STUMP_CACHE_LRU_SIZE` / `CALLBACK_STUMP_CACHE_TTL_SEC` entries
      from `deploy/k8s/*.yaml` and `deploy/k8s/README.md`, or re-wire them —
      nothing under `internal/` or `cmd/` reads them today
