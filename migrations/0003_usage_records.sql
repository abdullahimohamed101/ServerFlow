-- Usage records (Phase 12): one row per finished inference request, written by the usage consumer from the
-- terminal lifecycle events (inference.request.completed / .failed) on Kafka. Spec section 19.
--
-- The consumer is idempotent: event_id (derived from request_id, event type and attempt) is the primary key and
-- request_id is unique, so a redelivered or replayed event, or even two terminal events for one request, records
-- usage once. There is deliberately NO foreign key to tenants or api_keys: consuming must never depend on tenant
-- rows or their ordering, and an empty tenant_id means authentication was off.
--
-- Token counts are NULL when unknown (unknown is not zero). tokens_source says how good they are: 'usage' is the
-- worker's own usage object, 'chunks' counts streamed content chunks (an approximation of output tokens only),
-- 'estimate' means nothing was reported and only estimated_cost_tokens is meaningful. Reports group by it.
-- occurred_at is when the request finished on the gateway, so a replay does not move usage into "today".
CREATE TABLE usage_records (
    event_id              text        PRIMARY KEY
        CONSTRAINT usage_records_event_id_format CHECK (event_id ~ '^evt_[0-9a-f]{32}$'),
    request_id            text        NOT NULL UNIQUE
        CONSTRAINT usage_records_request_id_format CHECK (request_id ~ '^req_'),
    tenant_id             text        NOT NULL DEFAULT '',
    api_key_id            text        NOT NULL DEFAULT '',
    model                 text        NOT NULL DEFAULT '' CONSTRAINT usage_records_model_len CHECK (char_length(model) <= 128),
    worker_id             text        NOT NULL DEFAULT '' CONSTRAINT usage_records_worker_len CHECK (char_length(worker_id) <= 64),
    outcome               text        NOT NULL CONSTRAINT usage_records_outcome_known CHECK (outcome IN ('completed', 'failed')),
    failure_class         text        NOT NULL DEFAULT '',
    http_status           integer     NOT NULL CONSTRAINT usage_records_status_range CHECK (http_status BETWEEN 100 AND 599),
    input_tokens          bigint      CONSTRAINT usage_records_input_nonneg CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens         bigint      CONSTRAINT usage_records_output_nonneg CHECK (output_tokens IS NULL OR output_tokens >= 0),
    tokens_source         text        NOT NULL CONSTRAINT usage_records_tokens_source_known CHECK (tokens_source IN ('usage', 'chunks', 'estimate')),
    estimated_cost_tokens bigint      NOT NULL DEFAULT 0 CONSTRAINT usage_records_estimate_nonneg CHECK (estimated_cost_tokens >= 0),
    attempts              integer     NOT NULL DEFAULT 0 CONSTRAINT usage_records_attempts_nonneg CHECK (attempts >= 0),
    ttft_ms               bigint      NOT NULL DEFAULT 0 CONSTRAINT usage_records_ttft_nonneg CHECK (ttft_ms >= 0),
    duration_ms           bigint      NOT NULL DEFAULT 0 CONSTRAINT usage_records_duration_nonneg CHECK (duration_ms >= 0),
    occurred_at           timestamptz NOT NULL,
    recorded_at           timestamptz NOT NULL DEFAULT now(),
    kafka_partition       integer,
    kafka_offset          bigint,
    CONSTRAINT usage_records_failure_consistent CHECK ((outcome = 'failed') = (failure_class <> ''))
);

CREATE INDEX usage_records_tenant_time_idx ON usage_records (tenant_id, occurred_at);
CREATE INDEX usage_records_model_time_idx ON usage_records (model, occurred_at);

-- Coordinates of records the consumer could not use (garbage, a newer schema_version, oversize). The payload is
-- never stored. The unique key makes recording one again harmless.
CREATE TABLE usage_rejected_events (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic           text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    reason          text        NOT NULL CONSTRAINT usage_rejected_reason_known CHECK (reason IN ('decode', 'invalid', 'version', 'oversize', 'conflict')),
    rejected_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT usage_rejected_events_coordinates UNIQUE (topic, kafka_partition, kafka_offset)
);

-- Summaries (spec section 21) are a view over the one table, so there is no second write path to drift. The hour
-- is in UTC. Rows with different tokens_source are never summed together as if they were equal quality.
CREATE VIEW usage_hourly AS
SELECT tenant_id,
       model,
       tokens_source,
       date_trunc('hour', occurred_at, 'UTC') AS hour,
       count(*)                                                    AS requests,
       count(*) FILTER (WHERE outcome = 'failed')                  AS failures,
       sum(input_tokens)                                           AS input_tokens,
       sum(output_tokens)                                          AS output_tokens,
       sum(estimated_cost_tokens)                                  AS estimated_cost_tokens
FROM usage_records
GROUP BY tenant_id, model, tokens_source, date_trunc('hour', occurred_at, 'UTC');
