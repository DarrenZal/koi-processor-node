-- Schema snapshot of the four EXISTING tables api/discourse_links.py reads,
-- plus koi_migrations and koi_memory_chunks (appended at the end).
--
-- Source: pg_dump --schema-only --no-owner --no-privileges
--         -t public.session_discourse_moves -t public.document_ingestion_log
--         -t public.koi_memories -t public.document_field_membership personal_koi
--         (read-only), taken 2026-10-19 from the live personal_koi schema.
-- Edits:  comments and SET lines dropped; the episode_id foreign key to
--         knowledge_episodes removed, since that table is not part of the
--         snapshot. Nothing else changed.
-- Use:    tests/test_discourse_links.py and scripts/discourse_links_demo.py load
--         it into a throwaway database they create and drop. It is not a
--         migration.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE public.koi_memories (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    rid character varying(500) NOT NULL,
    cid character varying(500),
    version integer DEFAULT 1,
    previous_version_id uuid,
    event_type character varying(20) NOT NULL,
    source_sensor character varying(200) NOT NULL,
    agent_id uuid,
    content jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,
    superseded_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    is_private boolean DEFAULT false,
    access_source character varying(100),
    published_at timestamp with time zone,
    published_confidence double precision DEFAULT 1.0,
    content_hash character varying(64),
    last_seen_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    jena_uri character varying,
    jena_graph character varying,
    jena_sync_status character varying DEFAULT 'pending'::character varying,
    jena_synced_at timestamp without time zone,
    chunk_rid character varying,
    chunk_index integer,
    total_chunks integer,
    free_chunks character varying,
    chunks_created integer DEFAULT 0,
    jena_chunk_uri character varying,
    source_content_rid character varying,
    is_chunk boolean DEFAULT false,
    parent_document_rid character varying,
    tenant_id character varying(100),
    CONSTRAINT koi_memories_event_type_check CHECK (((event_type)::text = ANY ((ARRAY['NEW'::character varying, 'UPDATE'::character varying, 'FORGET'::character varying])::text[]))),
    CONSTRAINT koi_memories_published_confidence_check CHECK (((published_confidence >= (0)::double precision) AND (published_confidence <= (1)::double precision))),
    CONSTRAINT koi_memories_tenant_id_not_blank CHECK (((tenant_id IS NULL) OR (length(btrim((tenant_id)::text)) > 0)))
);

COMMENT ON TABLE public.koi_memories IS 'Stores processed documents with extracted metadata (one per document, not chunks)';

COMMENT ON COLUMN public.koi_memories.rid IS 'Resource Identifier - unique identifier for content';

COMMENT ON COLUMN public.koi_memories.cid IS 'Content Identifier - IPFS/content-addressed hash';

COMMENT ON COLUMN public.koi_memories.version IS 'Version number for this RID';

COMMENT ON COLUMN public.koi_memories.previous_version_id IS 'Link to previous version for audit trail';

COMMENT ON COLUMN public.koi_memories.superseded_at IS 'Timestamp when this version was replaced by newer version';

COMMENT ON COLUMN public.koi_memories.published_at IS 'Original publication date of the content';

COMMENT ON COLUMN public.koi_memories.published_confidence IS 'Confidence score for extracted publication date (0-1)';

COMMENT ON COLUMN public.koi_memories.content_hash IS 'SHA-256 hash of content for deduplication';

COMMENT ON COLUMN public.koi_memories.last_seen_at IS 'Last time this content was observed by sensors';

COMMENT ON COLUMN public.koi_memories.jena_uri IS 'URI in Apache Jena triplestore';

COMMENT ON COLUMN public.koi_memories.jena_graph IS 'Graph name in Jena';

COMMENT ON COLUMN public.koi_memories.jena_sync_status IS 'Synchronization status with Jena';

COMMENT ON COLUMN public.koi_memories.jena_synced_at IS 'Last synchronization timestamp with Jena';

COMMENT ON COLUMN public.koi_memories.chunk_rid IS 'Reference to chunk if this is chunked content';

COMMENT ON COLUMN public.koi_memories.chunk_index IS 'Index of this chunk within the document';

COMMENT ON COLUMN public.koi_memories.total_chunks IS 'Total number of chunks for this document';

COMMENT ON COLUMN public.koi_memories.source_content_rid IS 'Reference to original source content';

COMMENT ON COLUMN public.koi_memories.is_chunk IS 'Whether this memory is a chunk of a larger document';

COMMENT ON COLUMN public.koi_memories.parent_document_rid IS 'RID of parent document if this is a chunk';

COMMENT ON COLUMN public.koi_memories.tenant_id IS 'Owning tenant for this document, captured at ingest. NULL = pre-tenancy or first-party Regen data. Capture only: no read path filters on this column as of migration 108. Set once and sticky (COALESCE on conflict) — a later ingest of the same rid must not silently re-attribute an existing document.';

CREATE TABLE public.document_field_membership (
    document_rid text NOT NULL,
    field_id text NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    added_by text
);

CREATE TABLE public.document_ingestion_log (
    document_rid text NOT NULL,
    source_path text,
    source_url text,
    title text,
    content_hash text NOT NULL,
    char_count integer,
    chunk_count integer,
    window_count integer,
    group_id text DEFAULT 'personal'::text,
    tier text,
    rag_chunked_at timestamp with time zone,
    deep_extracted_at timestamp with time zone,
    deep_extraction_attempts integer DEFAULT 0,
    deep_extraction_last_error text,
    deep_extraction_next_retry_at timestamp with time zone,
    claims_extracted_at timestamp with time zone,
    last_run_id text,
    last_ingested_at timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT document_ingestion_log_tier_check CHECK (((tier IS NULL) OR (tier = ANY (ARRAY['rag'::text, 'standard'::text, 'thorough'::text]))))
);

CREATE TABLE public.session_discourse_moves (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    episode_id uuid,
    session_id text,
    move_type text NOT NULL,
    title text NOT NULL,
    detail text,
    status text,
    resolves_move_id uuid,
    turn_range_start integer,
    turn_range_end integer,
    embedding vector(1024),
    created_at timestamp without time zone DEFAULT now(),
    target_date date,
    source_type text DEFAULT 'session'::text NOT NULL,
    source_rid text,
    superseded_at timestamp with time zone,
    CONSTRAINT chk_discourse_move_type_by_source CHECK ((((source_type = 'session'::text) AND (move_type = ANY (ARRAY['decision'::text, 'problem'::text, 'question'::text, 'resolution'::text, 'observation'::text, 'next_step'::text, 'learning'::text]))) OR ((source_type = 'document'::text) AND (move_type = ANY (ARRAY['thesis'::text, 'premise'::text, 'evidence'::text, 'claim'::text, 'counterpoint'::text, 'open_question'::text, 'definition'::text, 'implication'::text]))))),
    CONSTRAINT chk_discourse_source_identity CHECK ((((source_type = 'session'::text) AND (session_id IS NOT NULL)) OR ((source_type = 'document'::text) AND (source_rid IS NOT NULL)))),
    CONSTRAINT chk_discourse_source_type CHECK ((source_type = ANY (ARRAY['session'::text, 'document'::text]))),
    CONSTRAINT chk_discourse_status_by_source CHECK (((status IS NULL) OR ((source_type = 'session'::text) AND (status = ANY (ARRAY['open'::text, 'resolved'::text, 'superseded'::text, 'deferred'::text, 'made'::text, 'reverted'::text, 'pending'::text, 'done'::text, 'cancelled'::text]))) OR ((source_type = 'document'::text) AND (status = ANY (ARRAY['asserted'::text, 'supported'::text, 'contested'::text, 'speculative'::text, 'open'::text, 'deferred'::text])))))
);

COMMENT ON COLUMN public.session_discourse_moves.turn_range_start IS 'source_type=session: conversation turn index. source_type=document: global RAG chunk index (range start).';

COMMENT ON COLUMN public.session_discourse_moves.turn_range_end IS 'source_type=session: conversation turn index. source_type=document: global RAG chunk index (range end).';

COMMENT ON COLUMN public.session_discourse_moves.source_type IS 'Producer of the move: ''session'' (daily deep-extraction) or ''document'' (unified document-ingest, Phase 3).';

COMMENT ON COLUMN public.session_discourse_moves.source_rid IS 'document:<sha256> RID for document moves; NULL for session moves (keyed by session_id).';

COMMENT ON COLUMN public.session_discourse_moves.superseded_at IS 'Set by POST /history/apply on the predecessor rid''s moves; CLEARED by the same endpoint''s revive/reveal arm and by ingest_document''s ON CONFLICT reveal, which must travel together with koi_memories.superseded_at = NULL (invariant I11 catches the drift). Stamped for pre-existing superseded rows by migration 123 leg E. Chosen over a status-CHECK migration and over a DELETE; chosen over deriving the predicate from koi_memories because that form fails OPEN once the parent row is hard-deleted under history_policy=drop.';

ALTER TABLE ONLY public.document_field_membership
    ADD CONSTRAINT document_field_membership_pkey PRIMARY KEY (document_rid, field_id);

ALTER TABLE ONLY public.document_ingestion_log
    ADD CONSTRAINT document_ingestion_log_pkey PRIMARY KEY (document_rid);

ALTER TABLE ONLY public.koi_memories
    ADD CONSTRAINT koi_memories_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.koi_memories
    ADD CONSTRAINT koi_memories_rid_key UNIQUE (rid);

ALTER TABLE ONLY public.koi_memories
    ADD CONSTRAINT koi_memories_rid_version_key UNIQUE (rid, version);

ALTER TABLE ONLY public.session_discourse_moves
    ADD CONSTRAINT session_discourse_moves_pkey PRIMARY KEY (id);

CREATE INDEX idx_dfm_field_id ON public.document_field_membership USING btree (field_id);

CREATE INDEX idx_discourse_embedding ON public.session_discourse_moves USING hnsw (embedding vector_cosine_ops);

CREATE INDEX idx_discourse_episode ON public.session_discourse_moves USING btree (episode_id);

CREATE INDEX idx_discourse_session ON public.session_discourse_moves USING btree (session_id);

CREATE INDEX idx_discourse_source_rid ON public.session_discourse_moves USING btree (source_rid) WHERE (source_rid IS NOT NULL);

CREATE INDEX idx_discourse_source_type ON public.session_discourse_moves USING btree (source_type);

CREATE INDEX idx_discourse_status ON public.session_discourse_moves USING btree (status) WHERE (status IS NOT NULL);

CREATE INDEX idx_discourse_target_date ON public.session_discourse_moves USING btree (target_date) WHERE (target_date IS NOT NULL);

CREATE INDEX idx_discourse_type ON public.session_discourse_moves USING btree (move_type);

CREATE INDEX idx_doc_ingest_retry ON public.document_ingestion_log USING btree (deep_extraction_next_retry_at) WHERE (deep_extraction_next_retry_at IS NOT NULL);

CREATE INDEX idx_koi_memories_active_tenant ON public.koi_memories USING btree (tenant_id, superseded_at) WHERE ((superseded_at IS NULL) AND (tenant_id IS NOT NULL));

CREATE INDEX idx_koi_memories_agent_id ON public.koi_memories USING btree (agent_id);

CREATE INDEX idx_koi_memories_cid ON public.koi_memories USING btree (cid);

CREATE INDEX idx_koi_memories_content_hash ON public.koi_memories USING btree (content_hash) WHERE (content_hash IS NOT NULL);

CREATE INDEX idx_koi_memories_created_at ON public.koi_memories USING btree (created_at DESC);

CREATE INDEX idx_koi_memories_daily_curator ON public.koi_memories USING btree (published_at DESC, source_sensor, event_type) WHERE ((published_at IS NOT NULL) AND (superseded_at IS NULL));

CREATE INDEX idx_koi_memories_event_type ON public.koi_memories USING btree (event_type);

CREATE INDEX idx_koi_memories_is_chunk ON public.koi_memories USING btree (is_chunk);

CREATE INDEX idx_koi_memories_is_private ON public.koi_memories USING btree (is_private);

CREATE INDEX idx_koi_memories_jena_sync ON public.koi_memories USING btree (jena_sync_status) WHERE ((jena_sync_status)::text = 'pending'::text);

CREATE INDEX idx_koi_memories_parent_doc ON public.koi_memories USING btree (parent_document_rid);

CREATE INDEX idx_koi_memories_published_at ON public.koi_memories USING btree (published_at DESC) WHERE (published_at IS NOT NULL);

CREATE INDEX idx_koi_memories_rid ON public.koi_memories USING btree (rid);

CREATE INDEX idx_koi_memories_source_content ON public.koi_memories USING btree (source_content_rid);

CREATE INDEX idx_koi_memories_source_sensor ON public.koi_memories USING btree (source_sensor);

CREATE INDEX idx_koi_memories_superseded ON public.koi_memories USING btree (superseded_at) WHERE (superseded_at IS NOT NULL);

CREATE INDEX idx_koi_memories_tenant_id ON public.koi_memories USING btree (tenant_id) WHERE (tenant_id IS NOT NULL);

CREATE INDEX idx_koi_memories_version ON public.koi_memories USING btree (rid, version DESC);

CREATE INDEX idx_memories_jena_sync ON public.koi_memories USING btree (jena_sync_status);

CREATE INDEX idx_memories_jena_uri ON public.koi_memories USING btree (jena_uri);

ALTER TABLE ONLY public.koi_memories
    ADD CONSTRAINT koi_memories_previous_version_id_fkey FOREIGN KEY (previous_version_id) REFERENCES public.koi_memories(id);

ALTER TABLE ONLY public.session_discourse_moves
    ADD CONSTRAINT session_discourse_moves_resolves_move_id_fkey FOREIGN KEY (resolves_move_id) REFERENCES public.session_discourse_moves(id);

-- koi_migrations, which migration 133 records itself in: pg_dump --schema-only
-- --no-owner --no-privileges -t public.koi_migrations personal_koi (read-only), 2026-10-19.
CREATE TABLE public.koi_migrations (
    migration_id text NOT NULL,
    applied_at timestamp with time zone DEFAULT now(),
    checksum text NOT NULL
);

ALTER TABLE ONLY public.koi_migrations
    ADD CONSTRAINT koi_migrations_pkey PRIMARY KEY (migration_id);

-- koi_memory_chunks, which the extractor reads its input from (fetch_rag_chunks):
-- pg_dump --schema-only --no-owner --no-privileges -t public.koi_memory_chunks
-- personal_koi (read-only), 2026-10-20. Comments and SET lines dropped.
CREATE TABLE public.koi_memory_chunks (
    id integer NOT NULL,
    chunk_rid character varying NOT NULL,
    document_rid character varying NOT NULL,
    chunk_index integer NOT NULL,
    total_chunks integer NOT NULL,
    content jsonb NOT NULL,
    embedding public.vector(1024),
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp without time zone DEFAULT now(),
    tsv tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, ((COALESCE((content ->> 'context'::text), ''::text) || ' '::text) || COALESCE((content ->> 'text'::text), ''::text)))) STORED,
    jena_chunk_uri character varying,
    embedding_3072 public.vector(3072)
);

CREATE SEQUENCE public.koi_memory_chunks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.koi_memory_chunks_id_seq OWNED BY public.koi_memory_chunks.id;

ALTER TABLE ONLY public.koi_memory_chunks ALTER COLUMN id SET DEFAULT nextval('public.koi_memory_chunks_id_seq'::regclass);

ALTER TABLE ONLY public.koi_memory_chunks
    ADD CONSTRAINT koi_memory_chunks_chunk_rid_key UNIQUE (chunk_rid);

ALTER TABLE ONLY public.koi_memory_chunks
    ADD CONSTRAINT koi_memory_chunks_pkey PRIMARY KEY (id);

CREATE INDEX idx_chunks_created ON public.koi_memory_chunks USING btree (created_at DESC);

CREATE INDEX idx_chunks_document ON public.koi_memory_chunks USING btree (document_rid);

CREATE INDEX idx_chunks_tsv ON public.koi_memory_chunks USING gin (tsv);

CREATE INDEX idx_kmc_null_embed3072 ON public.koi_memory_chunks USING btree (id) WHERE (embedding_3072 IS NULL);

CREATE INDEX idx_koi_memory_chunks_embedding ON public.koi_memory_chunks USING ivfflat (embedding public.vector_cosine_ops) WITH (lists='100');

CREATE INDEX idx_koi_memory_chunks_embedding_3072_hnsw ON public.koi_memory_chunks USING hnsw (((embedding_3072)::public.halfvec(3072)) public.halfvec_cosine_ops) WITH (m='16', ef_construction='64');

ALTER TABLE ONLY public.koi_memory_chunks
    ADD CONSTRAINT fk_document_rid FOREIGN KEY (document_rid) REFERENCES public.koi_memories(rid) ON DELETE CASCADE;

