
CREATE TABLE activity (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID, 
	space_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	source_type VARCHAR, 
	source_id UUID, 
	type VARCHAR NOT NULL, 
	is_hidden BOOLEAN DEFAULT false NOT NULL, 
	payload JSONB, 
	version BIGINT DEFAULT 0 NOT NULL, 
	event_log_id UUID, 
	related_message_id UUID, 
	related_record_id UUID, 
	related_suggestion_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	read_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id)
)

;
CREATE INDEX activity_event_log_id_idx ON activity (event_log_id) WHERE event_log_id IS NOT NULL;
CREATE INDEX activity_target_idx ON activity (target_type, target_id, deleted_at, read_at, type);
CREATE INDEX activity_payload_idx ON activity USING gin (payload jsonb_path_ops);
CREATE INDEX activity_related_record_id_idx ON activity (target_type, target_id, related_record_id);
CREATE INDEX activity_live_user_related_record_id_idx ON activity (related_record_id) WHERE target_type = 'user' AND deleted_at IS NULL;
CREATE INDEX activity_space_id_idx ON activity (space_id);
CREATE INDEX activity_related_message_id_idx ON activity (target_type, target_id, related_message_id);
CREATE INDEX activity_related_suggestion_id_idx ON activity (target_type, target_id, related_suggestion_id);
CREATE UNIQUE INDEX activity_signal_processed_source_target_unique_idx ON activity (source_id, target_id) WHERE type = 'signal_processed';
CREATE UNIQUE INDEX ix_activity_client_id ON activity (client_id);

CREATE TABLE agent_run (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	parent_agent_run_id UUID, 
	workflow_run_id UUID, 
	role VARCHAR NOT NULL, 
	payload JSONB NOT NULL, 
	compaction_version INTEGER DEFAULT 1 NOT NULL, 
	run_number INTEGER, 
	logfire_trace_id TEXT, 
	status VARCHAR DEFAULT 'ok' NOT NULL, 
	claim_token UUID, 
	claimed_at TIMESTAMP WITH TIME ZONE, 
	error JSONB, 
	app_changes JSONB, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT agent_run_space_id_id_idx UNIQUE (space_id, id), 
	CONSTRAINT agent_run_workflow_run_id_id_idx UNIQUE (workflow_run_id, id)
)

;
CREATE INDEX agent_run_router_signal_idx ON agent_run ((payload ->> 'signal_id')) WHERE role = 'router';
CREATE INDEX agent_run_processor_signal_idx ON agent_run ((payload ->> 'signal_id')) WHERE role = 'processor' AND workflow_run_id IS NULL;
CREATE INDEX agent_run_parent_agent_run_id_idx ON agent_run (parent_agent_run_id);
CREATE INDEX agent_run_space_id_idx ON agent_run (space_id);

CREATE TABLE assignment (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	record_id UUID NOT NULL, 
	scope VARCHAR, 
	instance_date DATE, 
	status VARCHAR DEFAULT 'confirmed', 
	variable TEXT NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	value JSONB DEFAULT '{}'::jsonb NOT NULL, 
	source TEXT DEFAULT 'user' NOT NULL, 
	version BIGINT DEFAULT 1 NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT assignment_scope_date_check CHECK ((scope = 'series' AND instance_date IS NULL) OR (scope = 'instance' AND instance_date IS NOT NULL) OR scope IS NULL), 
	CONSTRAINT assignment_target_type_check CHECK (target_type IN ('resource', 'calendar', 'channel_address')), 
	CONSTRAINT assignment_source_check CHECK (source IN ('user', 'public_claim', 'default-primary', 'suggestion')), 
	CONSTRAINT assignment_version_check CHECK (version > 0), 
	UNIQUE (client_id)
)

;
CREATE INDEX assignment_record_active_idx ON assignment (record_id, variable, target_type, target_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX assignment_active_semantic_idx ON assignment (record_id, variable, target_type, target_id, coalesce(scope, 'series'), coalesce(instance_date, DATE '0001-01-01')) WHERE deleted_at IS NULL;
CREATE INDEX assignment_target_active_idx ON assignment (target_type, target_id, variable, record_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX assignment_record_calendar_unique_idx ON assignment (record_id) WHERE target_type = 'calendar' AND deleted_at IS NULL;
CREATE INDEX assignment_record_id_idx ON assignment (record_id);

CREATE TABLE assistant_run (
	id UUID DEFAULT uuidv7() NOT NULL, 
	assistant_state_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	run_number INTEGER NOT NULL, 
	compaction_version INTEGER NOT NULL, 
	payload JSONB NOT NULL, 
	logfire_trace_id TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT assistant_run_unique_run_number UNIQUE (assistant_state_id, run_number)
)

;
CREATE INDEX idx_assistant_run_compaction ON assistant_run (assistant_state_id, compaction_version);
CREATE INDEX idx_assistant_run_space_id ON assistant_run (space_id);
CREATE INDEX idx_assistant_run_assistant_state_id ON assistant_run (assistant_state_id);

CREATE TABLE assistant_state (
	id UUID DEFAULT uuidv7() NOT NULL, 
	type VARCHAR NOT NULL, 
	source_type VARCHAR NOT NULL, 
	source_id TEXT NOT NULL, 
	space_id UUID NOT NULL, 
	status VARCHAR NOT NULL, 
	active_workflow_id TEXT, 
	last_event_id UUID, 
	last_run_number INTEGER NOT NULL, 
	compaction_version INTEGER NOT NULL, 
	guardrails_seed INTEGER, 
	private_metadata JSONB, 
	external_calendar_materialization JSONB, 
	fields_loro_snapshot BYTEA NOT NULL, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT assistant_state_type_source_unique UNIQUE (type, source_type, source_id)
)

;
CREATE INDEX assistant_state_public_upload_receipt_created_at_idx ON assistant_state (created_at, id) WHERE type = 'source' AND source_type = 'conversation' AND jsonb_path_exists(private_metadata, '$.calendar_write_events[*] ? (@.google_event_id.type() == "string" && @.google_event_id != "")');
CREATE INDEX idx_assistant_state_status ON assistant_state (status);

CREATE TABLE assistant_stream_event (
	id UUID DEFAULT uuidv7() NOT NULL, 
	assistant_run_id UUID NOT NULL, 
	assistant_state_id UUID NOT NULL, 
	seq INTEGER NOT NULL, 
	event_type TEXT NOT NULL, 
	content JSONB NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	space_id UUID NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT assistant_stream_event_run_seq_unique UNIQUE (assistant_run_id, seq)
)

;
CREATE INDEX assistant_stream_event_run_seq_idx ON assistant_stream_event (assistant_run_id, seq);
CREATE INDEX assistant_stream_event_created_at_idx ON assistant_stream_event (created_at);
CREATE INDEX assistant_stream_event_state_seq_idx ON assistant_stream_event (assistant_state_id, seq);
CREATE INDEX assistant_stream_event_space_id_idx ON assistant_stream_event (space_id);

CREATE TABLE calendar (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	external_account_id UUID NOT NULL, 
	resource_id UUID, 
	provider VARCHAR NOT NULL, 
	provider_calendar_id TEXT NOT NULL, 
	provider_channel_id TEXT, 
	provider_channel_resource_id TEXT, 
	provider_channel_expiration TIMESTAMP WITH TIME ZONE, 
	push_not_supported BOOLEAN DEFAULT false NOT NULL, 
	provider_metadata JSONB, 
	next_sync_token TEXT, 
	initial_sync_progress JSONB, 
	last_synced_at TIMESTAMP WITH TIME ZONE, 
	last_checked_at TIMESTAMP WITH TIME ZONE, 
	should_sync BOOLEAN DEFAULT false NOT NULL, 
	is_primary BOOLEAN DEFAULT false NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	version BIGINT DEFAULT 0 NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT calendar_provider_calendar_idx UNIQUE (user_id, external_account_id, provider_calendar_id), 
	UNIQUE (provider_channel_id)
)

;
CREATE INDEX calendar_user_id_provider_calendar_id_idx ON calendar (user_id, provider_calendar_id);
CREATE INDEX calendar_selected_last_checked_idx ON calendar (coalesce(last_checked_at, created_at), id) WHERE should_sync AND deleted_at IS NULL;
CREATE INDEX calendar_user_id_primary_idx ON calendar (user_id, deleted_at, is_primary);
CREATE INDEX calendar_external_account_id_idx ON calendar (external_account_id);
CREATE INDEX calendar_resource_id_idx ON calendar (resource_id) WHERE resource_id IS NOT NULL;
CREATE INDEX calendar_google_subscribed_live_idx ON calendar (id) WHERE deleted_at IS NULL AND provider_calendar_id LIKE '%%@import.calendar.google.com';

CREATE TABLE calendar_event (
	id UUID DEFAULT uuidv7() NOT NULL, 
	provider VARCHAR NOT NULL, 
	provider_id TEXT NOT NULL, 
	role VARCHAR, 
	indexed_record_id TEXT, 
	is_busy_block BOOLEAN DEFAULT false NOT NULL, 
	user_id UUID NOT NULL, 
	calendar_id UUID NOT NULL, 
	ical_uid TEXT, 
	name TEXT, 
	enrichment JSONB, 
	fields_loro_snapshot BYTEA DEFAULT decode('6c6f726f000000000000000000000000b6a4f80a0003ae0000004c4f524f00000300030110010fc9dce021551019010100000000000901b8b1b0a10d000100060104010000061806736368656d610676616c756573047265667304726f6f7400100104020600040100040202060b02060100060904090009000002006672018f92f3869ea4958819040002007676018f92f3869ea49588190600005a006a000300eb026cd901000000050000000c0019105521e0dcc90f000000000002007676034971908b000000cc0000004c4f524f0004224d186040828c000000f9050002010004726f6f7401000000090400020000001400f702000d00040fc9dce02155101900000000043100f10104030201000200010200010300000002030041000600805200f7100001000306736368656d6107018f92f3869ea495881900030676616c756573140068020104726566120045040100017600f000020000000100000d00210052000400000000000f242c6d01000000050000000d00000fc9dce021551019010000000106008004726f6f7469c58175a400000000000000', 'hex') NOT NULL, 
	start_date DATE, 
	start_datetime TIMESTAMP WITH TIME ZONE, 
	end_date DATE, 
	end_datetime TIMESTAMP WITH TIME ZONE, 
	is_recurring BOOLEAN DEFAULT false NOT NULL, 
	recurring_until DATE, 
	original_start_time TEXT, 
	start_timezone VARCHAR(50), 
	end_timezone VARCHAR(50), 
	provider_metadata JSONB, 
	status VARCHAR DEFAULT 'unknown' NOT NULL, 
	signal_content_snapshot JSONB, 
	signal_content_fingerprint TEXT, 
	signal_content_generation BIGINT, 
	signal_content_emitted_generation BIGINT, 
	signal_content_emitted_signal_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	version BIGINT NOT NULL, 
	last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	last_synced_version BIGINT, 
	dirty_at TIMESTAMP WITH TIME ZONE, 
	dirty_by_user_id UUID, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	linked_record_id UUID, 
	PRIMARY KEY (id), 
	CONSTRAINT calendar_event_status_check CHECK (status IN ('confirmed', 'tentative', 'cancelled', 'unknown')), 
	CONSTRAINT calendar_event_user_id_calendar_id_provider_id_unique UNIQUE (user_id, calendar_id, provider_id)
)

;
CREATE INDEX calendar_event_is_recurring_until_idx ON calendar_event (is_recurring, recurring_until);
CREATE INDEX calendar_event_dirty_organizer_idx ON calendar_event (dirty_at) WHERE dirty_at IS NOT NULL AND role = 'organizer';
CREATE INDEX calendar_event_calendar_id_id_idx ON calendar_event (calendar_id, id);
CREATE INDEX calendar_event_calendar_id_user_id_linked_record_id_idx ON calendar_event (calendar_id, user_id, linked_record_id);
CREATE UNIQUE INDEX calendar_event_organizer_indexed_record_id_unique_idx ON calendar_event (indexed_record_id) WHERE deleted_at IS NULL AND role = 'organizer' AND indexed_record_id IS NOT NULL;
CREATE INDEX calendar_event_linked_record_id_idx ON calendar_event (linked_record_id);
CREATE INDEX calendar_event_user_id_start_date_end_date_idx ON calendar_event (user_id, start_date, end_date) WHERE deleted_at IS NULL AND start_date IS NOT NULL;
CREATE INDEX calendar_event_ical_uid_idx ON calendar_event (ical_uid);
CREATE INDEX calendar_event_retired_linked_idx ON calendar_event (linked_record_id) WHERE linked_record_id IS NOT NULL AND (deleted_at IS NOT NULL OR status = 'cancelled');
CREATE INDEX calendar_event_indexed_record_id_idx ON calendar_event (indexed_record_id);
CREATE INDEX calendar_event_user_id_start_datetime_end_datetime_idx ON calendar_event (user_id, start_datetime, end_datetime) WHERE deleted_at IS NULL AND start_datetime IS NOT NULL;
CREATE INDEX calendar_event_partner_consolidation_candidate_idx ON calendar_event (user_id, provider, ical_uid, id) WHERE deleted_at IS NULL AND status <> 'cancelled' AND linked_record_id IS NOT NULL AND ical_uid IS NOT NULL;
CREATE INDEX calendar_event_cancelled_recurrence_exception_idx ON calendar_event (user_id, calendar_id, original_start_time) WHERE status = 'cancelled' AND original_start_time IS NOT NULL;
CREATE INDEX calendar_event_dirty_by_user_id_idx ON calendar_event (dirty_by_user_id) WHERE dirty_by_user_id IS NOT NULL;

CREATE TABLE calendar_highlight (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	highlight_type VARCHAR NOT NULL, 
	highlight_id UUID NOT NULL, 
	bucket VARCHAR NOT NULL, 
	bucket_start DATE NOT NULL, 
	probability DOUBLE PRECISION NOT NULL, 
	rank INTEGER NOT NULL, 
	reason TEXT, 
	input_fingerprint TEXT NOT NULL, 
	computed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT calendar_highlight_highlight_type_check CHECK (highlight_type = 'record'), 
	CONSTRAINT calendar_highlight_bucket_check CHECK (bucket IN ('day', 'week', 'month')), 
	CONSTRAINT calendar_highlight_space_bucket_start_type_id_key UNIQUE (space_id, bucket, bucket_start, highlight_type, highlight_id)
)

;
CREATE INDEX calendar_highlight_space_bucket_start_idx ON calendar_highlight (space_id, bucket, bucket_start);

CREATE TABLE channel_conversation (
	id UUID DEFAULT uuidv7() NOT NULL, 
	channel_type VARCHAR NOT NULL, 
	external_thread_id TEXT NOT NULL, 
	external_account_id TEXT NOT NULL, 
	workflow_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	participants JSONB NOT NULL, 
	last_synced_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT channel_conversation_channel_thread_account_unique_idx UNIQUE (channel_type, external_thread_id, external_account_id), 
	CONSTRAINT channel_conversation_workflow_id_idx UNIQUE (workflow_id)
)

;
CREATE UNIQUE INDEX channel_conversation_imessage_space_id_unique_idx ON channel_conversation (space_id) WHERE channel_type = 'imessage';

CREATE TABLE channel_event (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	external_conversation_id UUID, 
	type VARCHAR NOT NULL, 
	direction VARCHAR NOT NULL, 
	payload JSONB NOT NULL, 
	source_user_id UUID, 
	external_message_id TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id)
)

;
CREATE INDEX channel_event_type_idx ON channel_event (type);
CREATE INDEX channel_event_space_id_created_at_idx ON channel_event (space_id, created_at);
CREATE UNIQUE INDEX channel_event_external_message_id_unique_idx ON channel_event (external_message_id) WHERE external_message_id IS NOT NULL;
CREATE INDEX channel_event_external_conversation_id_idx ON channel_event (external_conversation_id) WHERE external_conversation_id IS NOT NULL;

CREATE TABLE circuits_source_fence (
	source_commit_id UUID NOT NULL, 
	PRIMARY KEY (source_commit_id)
)

;

CREATE TABLE composer_submission (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	user_id UUID NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	state VARCHAR DEFAULT 'pending' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT composer_submission_state_check CHECK (state IN ('pending', 'awaiting_destination', 'routed', 'failed', 'cancelled')), 
	UNIQUE (client_id)
)

;
CREATE INDEX composer_submission_space_id_user_id_state_created_at_idx ON composer_submission (space_id, user_id, state, created_at DESC);

CREATE TABLE contact_address (
	id UUID DEFAULT uuidv7() NOT NULL, 
	kind VARCHAR(8) NOT NULL, 
	value TEXT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT contact_address_kind_check CHECK (kind IN ('email', 'phone')), 
	CONSTRAINT contact_address_kind_value_unique_idx UNIQUE (kind, value)
)

;

CREATE TABLE conversation (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	title VARCHAR(1000), 
	type VARCHAR DEFAULT 'standard' NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	event_log_id UUID, 
	context JSONB, 
	metadata JSONB, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	created_by_user_id UUID NOT NULL, 
	record_id UUID, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (client_id)
)

;
CREATE INDEX conversation_calendar_unsettled_created_at_idx ON conversation (created_at DESC) WHERE deleted_at IS NULL AND jsonb_extract_path(metadata, 'calendar') IS NOT NULL AND coalesce(jsonb_extract_path_text(metadata, 'calendar', 'completed'), 'false') != 'true' AND coalesce(jsonb_extract_path_text(metadata, 'calendar', 'failed'), 'false') != 'true';
CREATE INDEX conversation_event_log_id_idx ON conversation (event_log_id) WHERE event_log_id IS NOT NULL;
CREATE UNIQUE INDEX conversation_onboarding_singleton_idx ON conversation (space_id) WHERE type = 'onboarding' AND deleted_at IS NULL;
CREATE INDEX conversation_calendar_completed_created_at_idx ON conversation (created_at, id) WHERE jsonb_path_exists(metadata, '$.calendar ? (@.completed == true)'::jsonpath);
CREATE UNIQUE INDEX conversation_record_id_active_unique_idx ON conversation (record_id) WHERE record_id IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE email_inbox (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	external_account_id UUID NOT NULL, 
	last_history_id TEXT, 
	last_synced_at TIMESTAMP WITH TIME ZONE, 
	sync_from_date TIMESTAMP WITH TIME ZONE, 
	sync_enabled BOOLEAN DEFAULT false NOT NULL, 
	watch_expires_at TIMESTAMP WITH TIME ZONE, 
	provider_subscription_id TEXT, 
	invalid_grant_refresh_token_hash TEXT, 
	initial_sync_state JSONB, 
	sync_health JSONB, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (external_account_id)
)

;

CREATE TABLE event_log (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID NOT NULL, 
	space_id UUID, 
	record_id UUID, 
	source_user_id UUID NOT NULL, 
	assistant JSONB, 
	type VARCHAR NOT NULL, 
	content JSONB NOT NULL, 
	workflow_suggestion_address JSONB, 
	timestamp BIGINT NOT NULL, 
	status VARCHAR DEFAULT 'pending' NOT NULL, 
	failure_reason VARCHAR, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	is_side_effect BOOLEAN DEFAULT false NOT NULL, 
	transaction_ids BIGINT[] DEFAULT '{}'::bigint[] NOT NULL, 
	source_transactions JSONB DEFAULT '[]'::jsonb NOT NULL, 
	source_history_generation UUID, 
	PRIMARY KEY (id), 
	CONSTRAINT event_log_space_id_timestamp_idx UNIQUE (space_id, timestamp)
)

;
CREATE INDEX event_log_record_id_idx ON event_log (record_id);
CREATE INDEX event_log_tag_created_payload_id_idx ON event_log (space_id, ((content -> 'payload'::text) ->> 'id'::text)) WHERE (type::text = 'tag_created'::text AND status::text = 'committed'::text);
CREATE INDEX event_log_space_id_type_status_idx ON event_log (space_id, type, status);
CREATE INDEX event_log_record_unsynced_attendee_idx ON event_log (record_id, timestamp) WHERE status = 'committed' AND deleted_at IS NULL AND type IN ('assignment_created', 'assignment_deleted', 'resource_assigned_to_record', 'resource_unassigned_from_record');
CREATE UNIQUE INDEX event_log_client_id_non_failed_unique_idx ON event_log (client_id) WHERE status != 'failed';
CREATE INDEX event_log_source_user_id_idx ON event_log (source_user_id);
CREATE INDEX event_log_legacy_workflow_suggestion_recovery_idx ON event_log (((content -> 'payload'::text) ->> 'request_id'::text), space_id, created_at, id) WHERE status = 'pending' AND workflow_suggestion_address IS NULL AND type IN ('suggestion_created', 'suggestions_created') AND ((content -> 'payload'::text) ->> 'request_id'::text) IS NOT NULL;
CREATE INDEX event_log_resource_share_create_client_id_idx ON event_log (space_id, ((content -> 'payload'::text) ->> 'client_id'::text), created_at, id) WHERE status = 'committed' AND type = 'resource_share_created';
CREATE INDEX event_log_client_id_failed_created_at_idx ON event_log (client_id, created_at, id) WHERE status = 'failed';

CREATE TABLE external_account (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	provider TEXT NOT NULL, 
	provider_account_id TEXT NOT NULL, 
	oauth_app TEXT DEFAULT 'internal' NOT NULL, 
	email VARCHAR(1000) NOT NULL, 
	email_verified BOOLEAN DEFAULT false NOT NULL, 
	access_token BYTEA, 
	refresh_token BYTEA, 
	id_token BYTEA, 
	connection JSONB, 
	expires_at TIMESTAMP WITH TIME ZONE, 
	encrypted_password BYTEA, 
	has_calendar_scope BOOLEAN DEFAULT false NOT NULL, 
	has_email_scope BOOLEAN DEFAULT false NOT NULL, 
	has_email_readonly_scope BOOLEAN DEFAULT false NOT NULL, 
	email_granted_at TIMESTAMP WITH TIME ZONE, 
	has_contacts_scope BOOLEAN DEFAULT false NOT NULL, 
	authentication_enabled BOOLEAN DEFAULT false NOT NULL, 
	disconnect_state TEXT, 
	disconnect_events TEXT, 
	resource_id UUID, 
	calendars_list_next_sync_token TEXT, 
	calendars_list_last_synced_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT external_account_user_provider_account_idx UNIQUE (user_id, provider, provider_account_id, oauth_app)
)

;
CREATE UNIQUE INDEX external_account_auth_enabled_idx ON external_account (provider, provider_account_id, oauth_app) WHERE authentication_enabled = true;
CREATE INDEX external_account_email_recovery_updated_at_idx ON external_account (updated_at) WHERE has_email_scope OR has_email_readonly_scope;
CREATE INDEX external_account_email_recovery_email_granted_at_idx ON external_account (email_granted_at) WHERE has_email_scope OR has_email_readonly_scope;
CREATE INDEX external_account_login_provider_account_id_idx ON external_account (lower(provider_account_id));
CREATE INDEX external_account_login_email_idx ON external_account (lower(email));
CREATE UNIQUE INDEX external_account_resource_id_unique_idx ON external_account (resource_id) WHERE resource_id IS NOT NULL;
CREATE INDEX external_account_email_recovery_created_at_idx ON external_account (created_at) WHERE has_email_scope OR has_email_readonly_scope;

CREATE TABLE external_link (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	provider TEXT NOT NULL, 
	account_hint TEXT, 
	account_id TEXT, 
	account_label TEXT, 
	state TEXT NOT NULL, 
	linked_at TIMESTAMP WITH TIME ZONE, 
	lost_at TIMESTAMP WITH TIME ZONE, 
	lost_reason TEXT, 
	last_item_at TIMESTAMP WITH TIME ZONE, 
	gap_from TIMESTAMP WITH TIME ZONE, 
	gap_to TIMESTAMP WITH TIME ZONE, 
	package TEXT, 
	consent_version TEXT, 
	consented_at TIMESTAMP WITH TIME ZONE, 
	connect_workflow_id UUID, 
	connect_opened_at TIMESTAMP WITH TIME ZONE, 
	connect_closed_at TIMESTAMP WITH TIME ZONE, 
	disconnect_started_at TIMESTAMP WITH TIME ZONE, 
	connect_card_message_id UUID, 
	owner_person_id UUID, 
	resource_id UUID, 
	replaced_push_settings JSONB, 
	push_settings_step JSONB, 
	catch_up JSONB, 
	bridge_accept_due_at TIMESTAMP WITH TIME ZONE, 
	bridge_accept_failed_at TIMESTAMP WITH TIME ZONE, 
	last_event_at TIMESTAMP WITH TIME ZONE, 
	website_read_at TIMESTAMP WITH TIME ZONE, 
	website_read_complete_at TIMESTAMP WITH TIME ZONE, 
	website_login TEXT, 
	website_profile_id UUID, 
	kernel_share_held_until TIMESTAMP WITH TIME ZONE, 
	allow_changes BOOLEAN DEFAULT false NOT NULL, 
	read_chat BOOLEAN DEFAULT true NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT external_link_provider_columns_check CHECK ((package IS NULL AND consent_version IS NULL AND consented_at IS NULL AND connect_workflow_id IS NULL AND connect_opened_at IS NULL AND connect_closed_at IS NULL) OR (package IS NOT NULL AND consent_version IS NOT NULL AND consented_at IS NOT NULL AND account_hint IS NULL AND account_id IS NULL)), 
	CONSTRAINT external_link_app_columns_check CHECK (package IS NOT NULL OR (owner_person_id IS NULL AND last_event_at IS NULL))
)

;
CREATE UNIQUE INDEX external_link_user_owner_person_provider_app_unique_idx ON external_link (user_id, owner_person_id, provider) WHERE package IS NOT NULL AND state IN ('pending', 'linked', 'lost');
CREATE INDEX external_link_resource_id_idx ON external_link (resource_id) WHERE resource_id IS NOT NULL;
CREATE INDEX external_link_connect_workflow_id_idx ON external_link (connect_workflow_id) WHERE connect_workflow_id IS NOT NULL;
CREATE INDEX external_link_owner_person_id_provider_app_idx ON external_link (owner_person_id, provider) WHERE package IS NOT NULL;
CREATE UNIQUE INDEX external_link_user_id_provider_chat_unique_idx ON external_link (user_id, provider) WHERE package IS NULL AND state <> 'unlinked';
CREATE INDEX external_link_provider_account_hint_active_idx ON external_link (provider, account_hint) WHERE state <> 'unlinked';
CREATE INDEX external_link_user_id_idx ON external_link (user_id);
CREATE UNIQUE INDEX external_link_resource_id_linked_unique_idx ON external_link (resource_id) WHERE state = 'linked';
CREATE INDEX external_link_connect_card_message_id_idx ON external_link (connect_card_message_id) WHERE connect_card_message_id IS NOT NULL;
CREATE INDEX external_link_provider_account_id_active_idx ON external_link (provider, account_id) WHERE state <> 'unlinked';
CREATE INDEX external_link_user_provider_label_app_idx ON external_link (user_id, provider, account_label, id) WHERE package IS NOT NULL;

CREATE TABLE external_link_group (
	id UUID DEFAULT uuidv7() NOT NULL, 
	external_link_id UUID NOT NULL, 
	external_id TEXT NOT NULL, 
	kind TEXT NOT NULL, 
	name TEXT NOT NULL, 
	member_count INTEGER, 
	last_activity_at TIMESTAMP WITH TIME ZONE, 
	"on" BOOLEAN DEFAULT false NOT NULL, 
	on_since TIMESTAMP WITH TIME ZONE, 
	resource_id UUID, 
	chat_card_message_id UUID, 
	chat_card_latest_at TIMESTAMP WITH TIME ZONE, 
	schedule_source_id UUID, 
	sport TEXT, 
	season_label TEXT, 
	website_listed_at TIMESTAMP WITH TIME ZONE, 
	schedule_mismatch_at TIMESTAMP WITH TIME ZONE, 
	member_tie TEXT, 
	sharing_turned_on_at TIMESTAMP WITH TIME ZONE, 
	schedule_adopted BOOLEAN DEFAULT false NOT NULL, 
	link_read_mode TEXT, 
	link_read_requested_at TIMESTAMP WITH TIME ZONE, 
	no_one_at TIMESTAMP WITH TIME ZONE, 
	website_posts_push TEXT, 
	member_refs TEXT[], 
	PRIMARY KEY (id), 
	CONSTRAINT external_link_group_external_link_id_external_id_idx UNIQUE (external_link_id, external_id), 
	CONSTRAINT external_link_group_member_tie_check CHECK (member_tie IS NULL OR member_tie IN ('person', 'outside_family')), 
	CONSTRAINT external_link_group_link_read_check CHECK ((link_read_mode IS NULL OR link_read_mode IN ('with_preferences', 'skip')) AND (link_read_requested_at IS NULL OR link_read_mode IS NOT NULL)), 
	CONSTRAINT external_link_group_website_posts_push_check CHECK (website_posts_push IS NULL OR website_posts_push IN ('instant', 'digest', 'off'))
)

;
CREATE INDEX external_link_group_resource_id_idx ON external_link_group (resource_id) WHERE resource_id IS NOT NULL;
CREATE INDEX external_link_group_external_link_id_name_id_idx ON external_link_group (external_link_id, name, id);
CREATE INDEX external_link_group_chat_card_message_id_idx ON external_link_group (chat_card_message_id) WHERE chat_card_message_id IS NOT NULL;
CREATE INDEX external_link_group_activity_order_idx ON external_link_group (external_link_id, last_activity_at DESC NULLS LAST, name, id);
CREATE INDEX external_link_group_schedule_source_id_idx ON external_link_group (schedule_source_id) WHERE schedule_source_id IS NOT NULL;

CREATE TABLE external_link_item (
	id UUID DEFAULT uuidv7() NOT NULL, 
	external_link_id UUID NOT NULL, 
	external_link_group_id UUID, 
	kind TEXT DEFAULT 'message' NOT NULL, 
	external_id TEXT NOT NULL, 
	path TEXT, 
	sender_id TEXT, 
	sender BYTEA, 
	subject BYTEA, 
	refs JSONB, 
	sent_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	text BYTEA NOT NULL, 
	media_kind VARCHAR, 
	media_status VARCHAR, 
	file_id UUID, 
	media_duration_s FLOAT, 
	media_size_bytes BIGINT, 
	reply_to_external_id TEXT, 
	history BOOLEAN NOT NULL, 
	signaled_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	kept_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT external_link_item_external_link_group_id_external_id_idx UNIQUE (external_link_group_id, external_id)
)

;
CREATE UNIQUE INDEX external_link_item_external_link_id_external_id_idx ON external_link_item (external_link_id, external_id) WHERE external_link_group_id IS NULL;
CREATE INDEX external_link_item_unsignaled_group_arrival_idx ON external_link_item (external_link_group_id, created_at, id) WHERE signaled_at IS NULL;
CREATE INDEX external_link_item_external_link_id_idx ON external_link_item (external_link_id);
CREATE INDEX external_link_item_external_link_group_id_sent_at_id_idx ON external_link_item (external_link_group_id, sent_at, id);
CREATE INDEX external_link_item_unkept_created_at_idx ON external_link_item (created_at) WHERE kept_at IS NULL;
CREATE INDEX external_link_item_kept_external_link_id_sender_id_idx ON external_link_item (external_link_id, sender_id) WHERE kept_at IS NOT NULL;
CREATE INDEX external_link_item_group_stamp_sent_at_id_idx ON external_link_item (external_link_group_id, signaled_at, sent_at, id);
CREATE INDEX external_link_item_file_id_idx ON external_link_item (file_id) WHERE file_id IS NOT NULL;

CREATE TABLE feedback (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	user_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	is_positive BOOLEAN NOT NULL, 
	subject_key TEXT DEFAULT '__message__' NOT NULL, 
	note TEXT, 
	attachment_file_ids JSONB DEFAULT '[]'::jsonb NOT NULL, 
	slack_message_id TEXT, 
	eval_processed_at TIMESTAMP WITH TIME ZONE, 
	eval_coverage JSONB, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT feedback_user_target_subject_unique_idx UNIQUE (user_id, space_id, target_type, target_id, subject_key), 
	UNIQUE (client_id)
)

;
CREATE UNIQUE INDEX feedback_email_feedback_target_unique_idx ON feedback (target_id) WHERE target_type = 'email_message' AND subject_key = 'email_feedback';
CREATE INDEX feedback_user_id_idx ON feedback (user_id);
CREATE INDEX feedback_target_type_target_id_idx ON feedback (target_type, target_id);
CREATE INDEX feedback_eval_pending_idx ON feedback (created_at) WHERE eval_processed_at IS NULL AND is_positive = false AND (target_type = 'suggestion' OR (target_type = 'email_message' AND subject_key = 'email_feedback'));
CREATE INDEX feedback_otto_user_feedback_pending_idx ON feedback (created_at) WHERE eval_processed_at IS NULL AND is_positive = false AND target_type = 'message' AND subject_key = 'otto_user_feedback';

CREATE TABLE file (
	id UUID DEFAULT uuidv7() NOT NULL, 
	scope_type VARCHAR NOT NULL, 
	user_id UUID, 
	channel_event_id UUID, 
	external_link_item_id UUID, 
	file_type VARCHAR NOT NULL, 
	mime_type TEXT NOT NULL, 
	name TEXT NOT NULL, 
	description TEXT, 
	size INTEGER NOT NULL, 
	parent_type VARCHAR, 
	parent_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	uploaded_at TIMESTAMP WITH TIME ZONE, 
	upload_url_expires_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	content_hash TEXT NOT NULL, 
	file_metadata JSONB, 
	status VARCHAR NOT NULL, 
	scan_verdict VARCHAR, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT file_scope_binding_check CHECK (((scope_type = 'user' AND user_id IS NOT NULL AND channel_event_id IS NULL AND external_link_item_id IS NULL) OR (scope_type = 'channel_event' AND channel_event_id IS NOT NULL AND user_id IS NULL AND external_link_item_id IS NULL) OR (scope_type = 'chat_message' AND external_link_item_id IS NOT NULL AND user_id IS NULL AND channel_event_id IS NULL)))
)

;
CREATE INDEX file_channel_event_id_idx ON file (channel_event_id) WHERE channel_event_id IS NOT NULL;
CREATE INDEX file_parent_type_parent_id_idx ON file (parent_type, parent_id);
CREATE UNIQUE INDEX file_external_link_item_id_content_hash_idx ON file (external_link_item_id, content_hash) WHERE deleted_at IS NULL AND scope_type = 'chat_message';
CREATE UNIQUE INDEX ux_file_channel_dedup ON file (channel_event_id, content_hash) WHERE deleted_at IS NULL AND scope_type = 'channel_event';
CREATE INDEX file_unattached_upload_sweep_idx ON file (updated_at, id) WHERE scope_type = 'user' AND status = 'uploading' AND deleted_at IS NULL AND parent_id IS NULL;
CREATE INDEX file_external_link_item_id_idx ON file (external_link_item_id) WHERE external_link_item_id IS NOT NULL;
CREATE UNIQUE INDEX ux_file_user_dedup ON file (user_id, content_hash) WHERE deleted_at IS NULL AND scope_type = 'user';
CREATE INDEX file_status_idx ON file (status);

CREATE TABLE file_share (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	file_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_space_id UUID NOT NULL, 
	target_id UUID NOT NULL, 
	message_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT file_share_unique_idx UNIQUE (user_id, file_id, target_type, target_space_id, target_id, message_id)
)

;
CREATE INDEX file_share_target_type_target_space_id_target_id_idx ON file_share (target_type, target_space_id, target_id);

CREATE TABLE generated_image (
	id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	image_type VARCHAR NOT NULL, 
	s3_path VARCHAR(512), 
	payload JSONB NOT NULL, 
	status VARCHAR NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id)
)

;
CREATE INDEX generated_image_user_type_created_idx ON generated_image (user_id, image_type, created_at);

CREATE TABLE graph_edge (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	from_type VARCHAR NOT NULL, 
	from_id UUID NOT NULL, 
	to_type VARCHAR NOT NULL, 
	to_id UUID NOT NULL, 
	edge_type VARCHAR(64) NOT NULL, 
	scope_type VARCHAR, 
	scope_id UUID, 
	fields_loro_snapshot BYTEA NOT NULL, 
	version BIGINT NOT NULL, 
	from_space_id UUID NOT NULL, 
	to_space_id UUID NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	valid_from DATE, 
	valid_until DATE, 
	PRIMARY KEY (id), 
	CONSTRAINT graph_edge_scope_pair_check CHECK ((scope_type IS NULL) = (scope_id IS NULL)), 
	CONSTRAINT graph_edge_validity_check CHECK (valid_from IS NULL OR valid_until IS NULL OR valid_from <= valid_until), 
	CONSTRAINT graph_edge_active_edge_type_check CHECK (deleted_at IS NOT NULL OR edge_type IN ('accepted_as', 'attached_to', 'attends_school', 'caregiver_of', 'child_of', 'client_of', 'focuses_on', 'generated', 'has_attendee', 'has_subject', 'has_teacher', 'holds_time_for', 'in_family_circle_of', 'lives_at', 'located_at', 'mentions', 'merged_into', 'parent_of', 'participates_in', 'partner_of', 'patient_of', 'pet_of', 'posted_to', 'provides_service_at', 'references', 'relates_to', 'shared_with', 'source_audio', 'tagged_with', 'targets', 'triggered_by', 'updated', 'works_at')), 
	UNIQUE (client_id)
)

;
CREATE INDEX graph_edge_scope_from_active_idx ON graph_edge (scope_type, scope_id, from_type, from_id, edge_type) WHERE deleted_at IS NULL AND scope_type IS NOT NULL;
CREATE INDEX graph_edge_from_space_idx ON graph_edge (from_space_id, deleted_at);
CREATE INDEX graph_edge_from_idx ON graph_edge (from_type, from_id, edge_type);
CREATE INDEX graph_edge_scope_to_active_idx ON graph_edge (scope_type, scope_id, to_type, to_id, edge_type) WHERE deleted_at IS NULL AND scope_type IS NOT NULL;
CREATE INDEX graph_edge_to_idx ON graph_edge (to_type, to_id, edge_type);
CREATE UNIQUE INDEX graph_edge_scoped_tuple_active_idx ON graph_edge (scope_type, scope_id, from_type, from_id, edge_type, to_type, to_id) WHERE deleted_at IS NULL AND scope_type IS NOT NULL;
CREATE UNIQUE INDEX graph_edge_unscoped_tuple_active_idx ON graph_edge (from_type, from_id, edge_type, to_type, to_id) WHERE deleted_at IS NULL AND scope_type IS NULL;
CREATE INDEX graph_edge_to_space_idx ON graph_edge (to_space_id, deleted_at);
CREATE INDEX graph_edge_type_created_idx ON graph_edge (edge_type, created_at);

CREATE TABLE invite (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	token UUID DEFAULT uuidv7() NOT NULL, 
	invite_type VARCHAR NOT NULL, 
	email VARCHAR(1000), 
	resource_type VARCHAR NOT NULL, 
	resource_id UUID NOT NULL, 
	contact_address_id UUID, 
	permission VARCHAR NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	exchanged_by_user_id UUID, 
	exchanged_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	expires_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	UNIQUE (token)
)

;
CREATE INDEX invite_active_link_target_idx ON invite (resource_type, resource_id) WHERE deleted_at IS NULL AND invite_type = 'link';
CREATE INDEX invite_contact_address_id_idx ON invite (contact_address_id);
CREATE UNIQUE INDEX invite_email_unique_idx ON invite (space_id, invite_type, resource_type, resource_id, permission, email) WHERE deleted_at IS NULL AND invite_type = 'email';
CREATE UNIQUE INDEX invite_link_unique_idx ON invite (space_id, invite_type, resource_type, resource_id, permission) WHERE deleted_at IS NULL AND invite_type = 'link';

CREATE TABLE message (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	user_id UUID NOT NULL, 
	type VARCHAR NOT NULL, 
	content JSONB NOT NULL, 
	context JSONB, 
	parent_id UUID, 
	reply_to_message_id UUID, 
	agent_run_id UUID, 
	event_log_id UUID, 
	source_channel_event_id UUID, 
	composer_submission_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	status VARCHAR, 
	routing_pending BOOLEAN DEFAULT false NOT NULL, 
	timestamp BIGINT NOT NULL, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT message_composer_submission_id_unique UNIQUE (composer_submission_id), 
	CONSTRAINT message_composer_submission_target_check CHECK (target_type <> 'composer_submission' OR (composer_submission_id IS NOT NULL AND target_id = composer_submission_id)), 
	CONSTRAINT message_reply_to_not_self_check CHECK (reply_to_message_id IS NULL OR reply_to_message_id <> id), 
	UNIQUE (client_id)
)

;
CREATE INDEX message_reply_to_message_id_idx ON message (reply_to_message_id) WHERE reply_to_message_id IS NOT NULL;
CREATE INDEX message_event_log_id_idx ON message (event_log_id) WHERE event_log_id IS NOT NULL;
CREATE INDEX message_status_space_id_idx ON message (status, space_id);
CREATE INDEX message_target_skip_card_created_at_id_idx ON message (target_id, created_at, id) WHERE deleted_at IS NULL AND ((content -> 'attachments') @> '[{"type": "app_link_connect", "kind": "intro"}]'::jsonb OR (content -> 'attachments') @> '[{"type": "account_connect", "kind": "password"}]'::jsonb);
CREATE INDEX message_target_live_created_at_id_idx ON message (target_type, target_id, created_at, id) WHERE deleted_at IS NULL;
CREATE INDEX message_target_parent_id_type_idx ON message (target_type, target_id, type, deleted_at, parent_id);
CREATE INDEX message_agent_run_id_idx ON message (agent_run_id);

CREATE TABLE monitored_url_run (
	id UUID DEFAULT uuidv7() NOT NULL, 
	source_id UUID NOT NULL, 
	scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL, 
	previous_successful_run_id UUID, 
	processor_kind VARCHAR NOT NULL, 
	processor_version VARCHAR(128) NOT NULL, 
	status VARCHAR DEFAULT 'pending' NOT NULL, 
	result VARCHAR, 
	http_status INTEGER, 
	resolved_url TEXT, 
	response_etag TEXT, 
	response_last_modified TEXT, 
	raw_sha256 VARCHAR(64), 
	raw_s3_key TEXT, 
	raw_size_bytes BIGINT, 
	raw_media_type TEXT, 
	snapshot_sha256 VARCHAR(64), 
	snapshot_s3_key TEXT, 
	snapshot_size_bytes BIGINT, 
	diff_sha256 VARCHAR(64), 
	diff_s3_key TEXT, 
	diff_size_bytes BIGINT, 
	error_code VARCHAR(128), 
	error_details JSONB, 
	admitted_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	started_at TIMESTAMP WITH TIME ZONE, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT monitored_url_run_source_cycle_unique UNIQUE (source_id, scheduled_for), 
	CONSTRAINT monitored_url_run_source_id_id_unique UNIQUE (source_id, id), 
	CONSTRAINT monitored_url_run_processor_kind_check CHECK (processor_kind IN ('calendar_ics')), 
	CONSTRAINT monitored_url_run_processor_version_check CHECK (btrim(processor_version) <> ''), 
	CONSTRAINT monitored_url_run_status_check CHECK (status IN ('pending', 'running', 'succeeded', 'failed')), 
	CONSTRAINT monitored_url_run_result_check CHECK (result IS NULL OR result IN ('baseline', 'not_modified', 'unchanged', 'changed')), 
	CONSTRAINT monitored_url_run_http_status_check CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599), 
	CONSTRAINT monitored_url_run_raw_sha256_check CHECK (raw_sha256 IS NULL OR raw_sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT monitored_url_run_snapshot_sha256_check CHECK (snapshot_sha256 IS NULL OR snapshot_sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT monitored_url_run_diff_sha256_check CHECK (diff_sha256 IS NULL OR diff_sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT monitored_url_run_raw_size_check CHECK (raw_size_bytes IS NULL OR raw_size_bytes BETWEEN 0 AND 1000000), 
	CONSTRAINT monitored_url_run_snapshot_size_check CHECK (snapshot_size_bytes IS NULL OR snapshot_size_bytes BETWEEN 0 AND 8388608), 
	CONSTRAINT monitored_url_run_diff_size_check CHECK (diff_size_bytes IS NULL OR diff_size_bytes BETWEEN 0 AND 8388608), 
	CONSTRAINT monitored_url_run_raw_artifact_check CHECK ((raw_sha256 IS NULL) = (raw_s3_key IS NULL) AND (raw_sha256 IS NULL) = (raw_size_bytes IS NULL) AND (raw_sha256 IS NULL) = (raw_media_type IS NULL)), 
	CONSTRAINT monitored_url_run_snapshot_artifact_check CHECK ((snapshot_sha256 IS NULL) = (snapshot_s3_key IS NULL) AND (snapshot_sha256 IS NULL) = (snapshot_size_bytes IS NULL)), 
	CONSTRAINT monitored_url_run_diff_artifact_check CHECK ((diff_sha256 IS NULL) = (diff_s3_key IS NULL) AND (diff_sha256 IS NULL) = (diff_size_bytes IS NULL)), 
	CONSTRAINT monitored_url_run_lifecycle_check CHECK ((status = 'pending' AND started_at IS NULL AND completed_at IS NULL AND result IS NULL AND error_code IS NULL) OR (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL AND result IS NULL AND error_code IS NULL) OR (status = 'succeeded' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NOT NULL AND error_code IS NULL AND snapshot_sha256 IS NOT NULL) OR (status = 'failed' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NULL AND error_code IS NOT NULL AND snapshot_sha256 IS NULL AND diff_sha256 IS NULL)), 
	CONSTRAINT monitored_url_run_result_artifact_check CHECK (status <> 'succeeded' OR (result = 'baseline' AND raw_sha256 IS NOT NULL AND diff_sha256 IS NULL AND previous_successful_run_id IS NULL) OR (result = 'not_modified' AND http_status = 304 AND raw_sha256 IS NULL AND diff_sha256 IS NULL AND previous_successful_run_id IS NOT NULL) OR (result = 'unchanged' AND raw_sha256 IS NOT NULL AND diff_sha256 IS NULL AND previous_successful_run_id IS NOT NULL) OR (result = 'changed' AND raw_sha256 IS NOT NULL AND diff_sha256 IS NOT NULL AND previous_successful_run_id IS NOT NULL))
)

;
CREATE INDEX monitored_url_run_stale_running_idx ON monitored_url_run (started_at, id) WHERE status = 'running';
CREATE INDEX monitored_url_run_source_history_idx ON monitored_url_run (source_id, scheduled_for DESC);

CREATE TABLE monitored_url_source (
	id UUID DEFAULT uuidv7() NOT NULL, 
	registration_idempotency_key UUID NOT NULL, 
	canonical_url TEXT NOT NULL, 
	canonical_url_sha256 VARCHAR(64) NOT NULL, 
	canonicalization_version VARCHAR(128) NOT NULL, 
	fetch_scope_type VARCHAR DEFAULT 'public' NOT NULL, 
	processor_kind VARCHAR DEFAULT 'calendar_ics' NOT NULL, 
	processor_version VARCHAR(128) NOT NULL, 
	output_schema VARCHAR(128) DEFAULT 'school_calendar.events' NOT NULL, 
	output_schema_version VARCHAR(128) NOT NULL, 
	status VARCHAR DEFAULT 'draft' NOT NULL, 
	poll_interval_seconds INTEGER DEFAULT 21600 NOT NULL, 
	next_poll_at TIMESTAMP WITH TIME ZONE, 
	http_etag TEXT, 
	http_last_modified TEXT, 
	last_successful_run_id UUID, 
	last_successful_read_at TIMESTAMP WITH TIME ZONE, 
	first_failed_read_at TIMESTAMP WITH TIME ZONE, 
	consecutive_failures INTEGER DEFAULT 0 NOT NULL, 
	first_empty_read_at TIMESTAMP WITH TIME ZONE, 
	consecutive_empty_reads INTEGER DEFAULT 0 NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	disabled_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT monitored_url_source_registration_key_unique_idx UNIQUE (registration_idempotency_key), 
	CONSTRAINT monitored_url_source_url_check CHECK (btrim(canonical_url) <> ''), 
	CONSTRAINT monitored_url_source_url_sha256_check CHECK (canonical_url_sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT monitored_url_source_canonicalization_version_check CHECK (btrim(canonicalization_version) <> ''), 
	CONSTRAINT monitored_url_source_scope_check CHECK (fetch_scope_type IN ('public')), 
	CONSTRAINT monitored_url_source_processor_kind_check CHECK (processor_kind IN ('calendar_ics')), 
	CONSTRAINT monitored_url_source_processor_version_check CHECK (btrim(processor_version) <> ''), 
	CONSTRAINT monitored_url_source_output_schema_check CHECK (output_schema = 'school_calendar.events'), 
	CONSTRAINT monitored_url_source_output_schema_version_check CHECK (btrim(output_schema_version) <> ''), 
	CONSTRAINT monitored_url_source_status_check CHECK (status IN ('draft', 'active', 'paused', 'disabled')), 
	CONSTRAINT monitored_url_source_cadence_check CHECK (poll_interval_seconds >= 900), 
	CONSTRAINT monitored_url_source_failures_check CHECK (consecutive_failures >= 0), 
	CONSTRAINT monitored_url_source_empty_reads_check CHECK (consecutive_empty_reads >= 0 AND (consecutive_empty_reads = 0) = (first_empty_read_at IS NULL)), 
	CONSTRAINT monitored_url_source_lifecycle_check CHECK ((status = 'draft' AND next_poll_at IS NULL AND disabled_at IS NULL) OR (status = 'active' AND next_poll_at IS NOT NULL AND disabled_at IS NULL) OR (status = 'paused' AND next_poll_at IS NULL AND disabled_at IS NULL) OR (status = 'disabled' AND next_poll_at IS NULL AND disabled_at IS NOT NULL))
)

;
CREATE INDEX monitored_url_source_due_idx ON monitored_url_source (next_poll_at, id) WHERE status = 'active';

CREATE TABLE monitored_url_subscription (
	id UUID DEFAULT uuidv7() NOT NULL, 
	registration_idempotency_key UUID NOT NULL, 
	source_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	subject_type VARCHAR NOT NULL, 
	subject_id UUID NOT NULL, 
	consumer_adapter VARCHAR NOT NULL, 
	execution_user_id UUID NOT NULL, 
	status VARCHAR DEFAULT 'active' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	disabled_at TIMESTAMP WITH TIME ZONE, 
	disabled_via VARCHAR, 
	PRIMARY KEY (id), 
	CONSTRAINT monitored_url_subscription_registration_key_unique UNIQUE (registration_idempotency_key), 
	CONSTRAINT monitored_url_subscription_target_unique UNIQUE (source_id, space_id, subject_type, subject_id, consumer_adapter), 
	CONSTRAINT monitored_url_subscription_subject_type_check CHECK (subject_type IN ('resource')), 
	CONSTRAINT monitored_url_subscription_adapter_check CHECK (consumer_adapter IN ('school_calendar')), 
	CONSTRAINT monitored_url_subscription_status_check CHECK (status IN ('active', 'disabled')), 
	CONSTRAINT monitored_url_subscription_disabled_via_check CHECK (disabled_via IN ('chat', 'operator', 'app_group_switch')), 
	CONSTRAINT monitored_url_subscription_lifecycle_check CHECK ((status = 'active' AND disabled_at IS NULL AND disabled_via IS NULL) OR (status = 'disabled' AND disabled_at IS NOT NULL AND (disabled_via IS NULL OR disabled_via IN ('chat', 'operator', 'app_group_switch'))))
)

;
CREATE INDEX monitored_url_subscription_space_subject_active_idx ON monitored_url_subscription (space_id, subject_id) WHERE status = 'active';
CREATE INDEX monitored_url_subscription_active_source_idx ON monitored_url_subscription (source_id, id) WHERE status = 'active';

CREATE TABLE monty_runtime_checkpoint (
	id UUID DEFAULT uuidv7() NOT NULL, 
	workflow_id UUID NOT NULL, 
	runtime_state_id UUID NOT NULL, 
	generation BIGINT NOT NULL, 
	parent_checkpoint_id UUID, 
	through_sequence BIGINT NOT NULL, 
	context_summary_revision BIGINT NOT NULL, 
	runtime_schema_version INTEGER NOT NULL, 
	monty_version TEXT NOT NULL, 
	harness_version TEXT NOT NULL, 
	runtime_abi_sha256 BYTEA NOT NULL, 
	status VARCHAR DEFAULT 'ready' NOT NULL, 
	snapshot_blob_key TEXT, 
	snapshot_sha256 BYTEA NOT NULL, 
	snapshot_bytes BIGINT NOT NULL, 
	manifest JSONB NOT NULL, 
	invalid_reason TEXT, 
	created_by_workflow_run_id UUID NOT NULL, 
	created_by_agent_run_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	ready_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	invalidated_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT monty_runtime_checkpoint_runtime_id_id_unique_idx UNIQUE (runtime_state_id, id), 
	CONSTRAINT monty_runtime_checkpoint_runtime_generation_unique_idx UNIQUE (runtime_state_id, generation), 
	CONSTRAINT monty_runtime_checkpoint_snapshot_blob_key_unique_idx UNIQUE (snapshot_blob_key), 
	CONSTRAINT monty_runtime_checkpoint_generation_check CHECK (generation >= 1), 
	CONSTRAINT monty_runtime_checkpoint_through_sequence_check CHECK (through_sequence >= 0), 
	CONSTRAINT monty_runtime_checkpoint_context_summary_revision_check CHECK (context_summary_revision >= 0), 
	CONSTRAINT monty_runtime_checkpoint_schema_version_check CHECK (runtime_schema_version >= 1), 
	CONSTRAINT monty_runtime_checkpoint_monty_version_check CHECK (char_length(monty_version) BETWEEN 1 AND 128), 
	CONSTRAINT monty_runtime_checkpoint_harness_version_check CHECK (char_length(harness_version) BETWEEN 1 AND 128), 
	CONSTRAINT monty_runtime_checkpoint_runtime_abi_check CHECK (octet_length(runtime_abi_sha256) = 32), 
	CONSTRAINT monty_runtime_checkpoint_status_check CHECK (status IN ('ready', 'invalid', 'deleted')), 
	CONSTRAINT monty_runtime_checkpoint_blob_key_check CHECK (char_length(snapshot_blob_key) BETWEEN 1 AND 1024), 
	CONSTRAINT monty_runtime_checkpoint_snapshot_sha_check CHECK (octet_length(snapshot_sha256) = 32), 
	CONSTRAINT monty_runtime_checkpoint_snapshot_bytes_check CHECK (snapshot_bytes BETWEEN 1 AND 67108864), 
	CONSTRAINT monty_runtime_checkpoint_manifest_bytes_check CHECK (octet_length(manifest::text) <= 65536), 
	CONSTRAINT monty_runtime_checkpoint_invalidated_at_check CHECK (status <> 'invalid' OR invalidated_at IS NOT NULL), 
	CONSTRAINT monty_runtime_checkpoint_invalidated_status_check CHECK (invalidated_at IS NULL OR status IN ('invalid', 'deleted')), 
	CONSTRAINT monty_runtime_checkpoint_invalid_reason_check CHECK (status <> 'invalid' OR invalid_reason IS NOT NULL), 
	CONSTRAINT monty_runtime_checkpoint_deleted_at_check CHECK ((status = 'deleted') = (deleted_at IS NOT NULL))
)

;
CREATE INDEX monty_runtime_checkpoint_parent_checkpoint_id_idx ON monty_runtime_checkpoint (parent_checkpoint_id) WHERE parent_checkpoint_id IS NOT NULL;
CREATE INDEX monty_runtime_checkpoint_created_by_agent_run_id_idx ON monty_runtime_checkpoint (created_by_agent_run_id) WHERE created_by_agent_run_id IS NOT NULL;
CREATE INDEX monty_runtime_checkpoint_workflow_run_idx ON monty_runtime_checkpoint (workflow_id, created_by_workflow_run_id);
CREATE INDEX monty_runtime_checkpoint_restore_idx ON monty_runtime_checkpoint (runtime_state_id, generation DESC) WHERE status = 'ready';

CREATE TABLE native_sync_authority_fence (
	user_id UUID NOT NULL, 
	generation VARCHAR(64) NOT NULL, 
	source_commit_id UUID NOT NULL, 
	PRIMARY KEY (user_id)
)

;

CREATE TABLE partner_calendar_event_mirror (
	id UUID DEFAULT uuidv7() NOT NULL, 
	origin_standing_grant_id UUID NOT NULL, 
	origin_calendar_event_id UUID NOT NULL, 
	calendar_event_id UUID NOT NULL, 
	recipient_user_id UUID NOT NULL, 
	activation_epoch INTEGER NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id)
)

;
CREATE INDEX partner_calendar_event_mirror_recipient_active_idx ON partner_calendar_event_mirror (recipient_user_id) WHERE deleted_at IS NULL;
CREATE INDEX partner_calendar_event_mirror_origin_idx ON partner_calendar_event_mirror (origin_calendar_event_id);
CREATE UNIQUE INDEX partner_calendar_event_mirror_grant_origin_uidx ON partner_calendar_event_mirror (origin_standing_grant_id, origin_calendar_event_id);

CREATE TABLE record (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	name TEXT NOT NULL, 
	fields_loro_snapshot BYTEA NOT NULL, 
	space_id UUID NOT NULL, 
	parent_id UUID, 
	type VARCHAR, 
	ical_uid TEXT, 
	app_event_ref TEXT, 
	source_calendar_event_id UUID, 
	superseded_by_record_id UUID, 
	event_log_id UUID, 
	source_suggestion_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	archived_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	removed_at TIMESTAMP WITH TIME ZONE, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	moved_at_timestamp BIGINT, 
	version BIGINT NOT NULL, 
	provider_projection_reindex_pending_version BIGINT, 
	status VARCHAR, 
	recurring_parent_id UUID, 
	original_start_time TEXT, 
	workflow_origin VARCHAR, 
	workflow_created_at TIMESTAMP WITH TIME ZONE, 
	last_composer_activity_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT record_client_id_idx UNIQUE (client_id), 
	CONSTRAINT record_workflow_origin_check CHECK (workflow_origin IS NULL OR workflow_origin IN ('composer', 'background')), 
	CONSTRAINT record_workflow_foundation_pair_check CHECK ((workflow_origin IS NULL) = (workflow_created_at IS NULL)), 
	CONSTRAINT record_not_self_superseded_check CHECK (superseded_by_record_id IS NULL OR superseded_by_record_id <> id), 
	UNIQUE (client_id)
)

;
CREATE UNIQUE INDEX record_space_id_app_event_ref_unique ON record (space_id, app_event_ref) WHERE app_event_ref IS NOT NULL AND superseded_by_record_id IS NULL AND deleted_at IS NULL;
CREATE INDEX record_space_id_parent_id_idx ON record (space_id, parent_id);
CREATE INDEX record_recurring_parent_id_idx ON record (recurring_parent_id) WHERE recurring_parent_id IS NOT NULL;
CREATE INDEX record_pending_provider_reindex_idx ON record (id) WHERE provider_projection_reindex_pending_version IS NOT NULL;
CREATE UNIQUE INDEX record_recurring_parent_id_original_start_time_idx ON record (recurring_parent_id, original_start_time) WHERE deleted_at IS NULL;
CREATE INDEX record_user_workflow_created_active_idx ON record (created_by_user_id, workflow_created_at DESC, id DESC) WHERE deleted_at IS NULL AND removed_at IS NULL AND archived_at IS NULL AND workflow_created_at IS NOT NULL;
CREATE UNIQUE INDEX record_space_id_ical_uid_parent_unique ON record (space_id, ical_uid) WHERE original_start_time IS NULL AND superseded_by_record_id IS NULL;
CREATE INDEX record_event_log_id_idx ON record (event_log_id) WHERE event_log_id IS NOT NULL;
CREATE INDEX record_recurring_parent_id_spelled_date_idx ON record (recurring_parent_id, (left(replace(original_start_time, '-', ''), 8) COLLATE "C")) WHERE recurring_parent_id IS NOT NULL AND superseded_by_record_id IS NULL;
CREATE INDEX record_source_suggestion_id_idx ON record (source_suggestion_id);
CREATE INDEX record_space_id_filter_idx ON record (space_id, deleted_at, removed_at, archived_at);
CREATE UNIQUE INDEX record_space_id_ical_uid_original_start_time_unique ON record (space_id, ical_uid, original_start_time) WHERE original_start_time IS NOT NULL AND superseded_by_record_id IS NULL;
CREATE INDEX record_superseded_by_record_id_idx ON record (superseded_by_record_id) WHERE superseded_by_record_id IS NOT NULL;
CREATE INDEX record_source_calendar_event_id_idx ON record (source_calendar_event_id);
CREATE INDEX record_user_last_composer_activity_active_idx ON record (created_by_user_id, last_composer_activity_at DESC, id DESC) WHERE deleted_at IS NULL AND removed_at IS NULL AND archived_at IS NULL AND last_composer_activity_at IS NOT NULL;

CREATE TABLE record_assignee_index (
	record_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	assignee_id UUID NOT NULL, 
	PRIMARY KEY (record_id), 
	CONSTRAINT record_assignee_index_space_id_record_id_idx UNIQUE (space_id, record_id)
)

;
CREATE INDEX record_assignee_index_assignee_id_space_id_idx ON record_assignee_index (assignee_id, space_id);

CREATE TABLE record_completed_index (
	record_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (record_id), 
	CONSTRAINT record_completed_index_space_id_record_id_idx UNIQUE (space_id, record_id)
)

;
CREATE INDEX record_completed_index_space_id_completed_at_idx ON record_completed_index (space_id, completed_at);

CREATE TABLE record_date_index (
	record_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	field_id TEXT NOT NULL, 
	field_type TEXT DEFAULT 'due_at' NOT NULL, 
	date_value DATE, 
	datetime_value TIMESTAMP WITH TIME ZONE, 
	end_date_value DATE, 
	end_datetime_value TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (record_id, field_id)
)

;
CREATE INDEX record_date_index_date_value_partial_idx ON record_date_index (date_value, field_id, space_id) WHERE date_value IS NOT NULL;
CREATE INDEX record_date_index_date_range_partial_idx ON record_date_index (date_value, end_date_value, field_id, space_id) WHERE date_value IS NOT NULL;
CREATE INDEX record_date_index_field_type_space_id_idx ON record_date_index (field_type, space_id);
CREATE INDEX record_date_index_datetime_range_partial_idx ON record_date_index (datetime_value, end_datetime_value, field_id, space_id) WHERE datetime_value IS NOT NULL;
CREATE INDEX record_date_index_datetime_value_partial_idx ON record_date_index (datetime_value, field_id, space_id) WHERE datetime_value IS NOT NULL;

CREATE TABLE record_recurrence (
	record_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	rrule TEXT NOT NULL, 
	dtstart TIMESTAMP WITH TIME ZONE NOT NULL, 
	recurring_until TIMESTAMP WITH TIME ZONE, 
	timezone VARCHAR(50), 
	exdates TEXT[], 
	duration_seconds INTEGER, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (record_id)
)

;

CREATE TABLE record_share (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	record_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	invite_id UUID, 
	created_by_user_id UUID NOT NULL, 
	access_level VARCHAR DEFAULT 'edit' NOT NULL, 
	origin_type VARCHAR, 
	origin_id UUID, 
	origin_standing_grant_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT record_share_access_level_check CHECK (access_level IN ('read', 'suggest', 'assign', 'edit')), 
	CONSTRAINT record_share_origin_pair_check CHECK ((origin_type IS NULL) = (origin_id IS NULL))
)

;
CREATE INDEX record_share_record_id_idx ON record_share (record_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX record_share_unique_idx ON record_share (space_id, record_id, target_type, target_id, invite_id) WHERE deleted_at IS NULL;
CREATE INDEX record_share_target_type_target_id_idx ON record_share (target_type, target_id);
CREATE INDEX record_share_origin_standing_grant_filter_idx ON record_share (origin_standing_grant_id) WHERE deleted_at IS NULL;

CREATE TABLE resource (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	parent_id UUID, 
	name TEXT NOT NULL, 
	resource_type VARCHAR, 
	category VARCHAR, 
	normalized_name TEXT, 
	user_id UUID, 
	avatar_key VARCHAR(512), 
	avatar_color VARCHAR, 
	metadata JSONB DEFAULT '{}' NOT NULL, 
	superseded_by_resource_id UUID, 
	record_id UUID, 
	feed_token TEXT, 
	feed_enabled BOOLEAN DEFAULT 'false' NOT NULL, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	created_by_user_id UUID NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT resource_id_space_unique UNIQUE (id, space_id), 
	CONSTRAINT resource_record_id_unique UNIQUE (record_id), 
	CONSTRAINT resource_not_self_parent_check CHECK (parent_id <> id), 
	CONSTRAINT resource_not_self_superseded_check CHECK (superseded_by_resource_id IS NULL OR superseded_by_resource_id <> id), 
	UNIQUE (client_id)
)

;
CREATE UNIQUE INDEX resource_feed_token_unique_idx ON resource (feed_token) WHERE feed_token IS NOT NULL;
CREATE INDEX resource_superseded_by_resource_id_idx ON resource (superseded_by_resource_id) WHERE superseded_by_resource_id IS NOT NULL;
CREATE INDEX resource_parent_id_idx ON resource (parent_id) WHERE parent_id IS NOT NULL;
CREATE UNIQUE INDEX resource_active_owner_person_unique_idx ON resource (space_id, user_id) WHERE user_id IS NOT NULL AND resource_type = 'person' AND deleted_at IS NULL AND superseded_by_resource_id IS NULL;
CREATE INDEX resource_normalized_name_lookup_idx ON resource (normalized_name) WHERE deleted_at IS NULL;
CREATE INDEX resource_space_id_idx ON resource (space_id) WHERE deleted_at IS NULL;

CREATE TABLE resource_fact (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	resource_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	fact_type VARCHAR NOT NULL, 
	value TEXT NOT NULL, 
	value_json JSONB, 
	source VARCHAR NOT NULL, 
	confidence FLOAT, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	created_by_user_id UUID NOT NULL, 
	valid_from DATE, 
	valid_until DATE, 
	PRIMARY KEY (id)
)

;
CREATE UNIQUE INDEX resource_fact_resource_type_unique_idx ON resource_fact (resource_id, fact_type) WHERE deleted_at IS NULL;
CREATE INDEX resource_fact_space_id_idx ON resource_fact (space_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX resource_fact_client_id_unique_idx ON resource_fact (client_id);

CREATE TABLE resource_share (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID NOT NULL, 
	resource_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	target_id UUID NOT NULL, 
	invite_id UUID, 
	source VARCHAR DEFAULT 'direct' NOT NULL, 
	origin_standing_grant_id UUID, 
	access_level VARCHAR NOT NULL, 
	version BIGINT NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT resource_share_target_type_check CHECK (target_type IN ('user')), 
	CONSTRAINT resource_share_access_level_check CHECK (access_level IN ('view', 'use', 'edit', 'manage')), 
	CONSTRAINT resource_share_source_check CHECK (source IN ('direct', 'standing')), 
	CONSTRAINT resource_share_standing_origin_check CHECK ((source = 'standing') = (origin_standing_grant_id IS NOT NULL)), 
	CONSTRAINT resource_share_version_check CHECK (version > 0), 
	UNIQUE (client_id)
)

;
CREATE INDEX resource_share_target_lookup_idx ON resource_share (target_type, target_id, resource_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX resource_share_active_provenance_idx ON resource_share (resource_id, target_type, target_id, source, origin_standing_grant_id) NULLS NOT DISTINCT WHERE deleted_at IS NULL;
CREATE INDEX resource_share_resource_lookup_idx ON resource_share (resource_id, access_level, target_id) WHERE deleted_at IS NULL;
CREATE INDEX resource_share_origin_idx ON resource_share (origin_standing_grant_id) WHERE origin_standing_grant_id IS NOT NULL;
CREATE UNIQUE INDEX resource_share_origin_live_uidx ON resource_share (origin_standing_grant_id, resource_id) WHERE origin_standing_grant_id IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE routine (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	key TEXT NOT NULL, 
	name TEXT NOT NULL, 
	description TEXT, 
	status VARCHAR DEFAULT 'active' NOT NULL, 
	created_by_user_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	retired_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT routine_space_id_key_idx UNIQUE (space_id, key), 
	CONSTRAINT routine_key_check CHECK (key ~ '^[a-z][a-z0-9_.-]{0,127}$'), 
	CONSTRAINT routine_name_length_check CHECK (char_length(name) BETWEEN 1 AND 256), 
	CONSTRAINT routine_description_length_check CHECK (description IS NULL OR char_length(description) <= 4096), 
	CONSTRAINT routine_status_check CHECK (status IN ('active', 'retired')), 
	CONSTRAINT routine_retired_at_check CHECK ((status = 'retired') = (retired_at IS NOT NULL))
)

;
CREATE INDEX routine_created_by_user_id_idx ON routine (created_by_user_id) WHERE created_by_user_id IS NOT NULL;

CREATE TABLE routine_installation (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	workflow_id UUID NOT NULL, 
	routine_id UUID NOT NULL, 
	key TEXT NOT NULL, 
	status VARCHAR DEFAULT 'paused' NOT NULL, 
	active_revision_id UUID, 
	version BIGINT DEFAULT 0 NOT NULL, 
	repair_task_record_id UUID, 
	repair_episode_ordinal BIGINT DEFAULT 0 NOT NULL, 
	repair_failure_fingerprint TEXT, 
	repair_expected_active_revision_id UUID, 
	repair_attempt_count INTEGER DEFAULT 0 NOT NULL, 
	repair_attempt_limit INTEGER, 
	repair_opened_at TIMESTAMP WITH TIME ZONE, 
	repair_exhausted_at TIMESTAMP WITH TIME ZONE, 
	last_successful_workflow_run_id UUID, 
	last_failed_workflow_run_id UUID, 
	created_by_user_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	retired_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT routine_installation_workflow_key_idx UNIQUE (workflow_id, key), 
	CONSTRAINT routine_installation_id_routine_id_idx UNIQUE (id, routine_id), 
	CONSTRAINT routine_installation_workflow_id_id_routine_id_idx UNIQUE (workflow_id, id, routine_id), 
	CONSTRAINT routine_installation_key_check CHECK (key ~ '^[a-z][a-z0-9_.-]{0,127}$'), 
	CONSTRAINT routine_installation_status_check CHECK (status IN ('paused', 'active', 'degraded', 'retired')), 
	CONSTRAINT routine_installation_retired_at_check CHECK ((status = 'retired') = (retired_at IS NOT NULL)), 
	CONSTRAINT routine_installation_version_check CHECK (version >= 0), 
	CONSTRAINT routine_installation_repair_episode_ordinal_check CHECK (repair_episode_ordinal >= 0), 
	CONSTRAINT routine_installation_repair_attempt_count_check CHECK (repair_attempt_count >= 0), 
	CONSTRAINT routine_installation_repair_attempt_limit_check CHECK (repair_attempt_limit IS NULL OR repair_attempt_limit BETWEEN 1 AND 32), 
	CONSTRAINT routine_installation_repair_failure_fingerprint_check CHECK (repair_failure_fingerprint IS NULL OR repair_failure_fingerprint ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT routine_installation_degraded_repair_shape_check CHECK (status <> 'degraded' OR (repair_task_record_id IS NOT NULL AND repair_failure_fingerprint IS NOT NULL AND repair_expected_active_revision_id IS NOT NULL AND repair_attempt_limit IS NOT NULL AND repair_opened_at IS NOT NULL)), 
	CONSTRAINT routine_installation_repair_exhausted_shape_check CHECK (repair_exhausted_at IS NULL OR (status = 'degraded' AND repair_attempt_count >= repair_attempt_limit))
)

;
CREATE INDEX routine_installation_space_id_idx ON routine_installation (space_id);
CREATE INDEX routine_installation_routine_id_idx ON routine_installation (routine_id);
CREATE INDEX routine_installation_created_by_user_id_idx ON routine_installation (created_by_user_id) WHERE created_by_user_id IS NOT NULL;
CREATE INDEX routine_installation_repair_task_record_id_idx ON routine_installation (repair_task_record_id);

CREATE TABLE routine_revision (
	id UUID DEFAULT uuidv7() NOT NULL, 
	routine_id UUID NOT NULL, 
	ordinal INTEGER NOT NULL, 
	parent_revision_id UUID, 
	source TEXT NOT NULL, 
	source_sha256 TEXT NOT NULL, 
	runtime_kind VARCHAR DEFAULT 'monty' NOT NULL, 
	runtime_version TEXT NOT NULL, 
	host_api_version TEXT NOT NULL, 
	input_schema JSONB NOT NULL, 
	output_schema JSONB NOT NULL, 
	output_identity_contract JSONB, 
	capability_manifest JSONB NOT NULL, 
	validation_artifact_ref TEXT NOT NULL, 
	created_by_agent_run_id UUID, 
	created_by_user_id UUID, 
	provenance JSONB NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT routine_revision_routine_id_id_idx UNIQUE (routine_id, id), 
	CONSTRAINT routine_revision_routine_ordinal_idx UNIQUE (routine_id, ordinal), 
	CONSTRAINT routine_revision_source_runtime_host_idx UNIQUE (routine_id, source_sha256, runtime_version, host_api_version), 
	CONSTRAINT routine_revision_ordinal_check CHECK (ordinal >= 1), 
	CONSTRAINT routine_revision_source_length_check CHECK (octet_length(source) BETWEEN 1 AND 1048576), 
	CONSTRAINT routine_revision_source_sha256_check CHECK (source_sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT routine_revision_runtime_kind_check CHECK (runtime_kind = 'monty'), 
	CONSTRAINT routine_revision_runtime_version_length_check CHECK (char_length(runtime_version) BETWEEN 1 AND 128), 
	CONSTRAINT routine_revision_host_api_version_length_check CHECK (char_length(host_api_version) BETWEEN 1 AND 128), 
	CONSTRAINT routine_revision_validation_artifact_ref_length_check CHECK (char_length(validation_artifact_ref) BETWEEN 1 AND 2048)
)

;
CREATE INDEX routine_revision_created_by_agent_run_id_idx ON routine_revision (created_by_agent_run_id) WHERE created_by_agent_run_id IS NOT NULL;
CREATE INDEX routine_revision_created_by_user_id_idx ON routine_revision (created_by_user_id) WHERE created_by_user_id IS NOT NULL;

CREATE TABLE signal (
	id UUID DEFAULT uuidv7() NOT NULL, 
	seq BIGINT GENERATED ALWAYS AS IDENTITY, 
	type TEXT NOT NULL, 
	space_id UUID NOT NULL, 
	subject_type VARCHAR NOT NULL, 
	subject_id TEXT NOT NULL, 
	source_workflow_id UUID, 
	source_type VARCHAR, 
	source_id TEXT, 
	root_signal_id UUID, 
	parent_signal_id UUID, 
	depth INTEGER DEFAULT 0 NOT NULL, 
	origin VARCHAR DEFAULT 'external' NOT NULL, 
	payload JSONB NOT NULL, 
	idempotency_key TEXT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	dispatched_at TIMESTAMP WITH TIME ZONE, 
	dispatch_started_at TIMESTAMP WITH TIME ZONE, 
	dispatch_generation INTEGER DEFAULT 0 NOT NULL, 
	dispatch_lane_key TEXT, 
	dispatch_disposition JSONB DEFAULT '{"kind": "pending"}'::jsonb NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT signal_idempotency_key_idx UNIQUE (idempotency_key), 
	CONSTRAINT signal_parent_not_self_check CHECK (parent_signal_id IS NULL OR parent_signal_id <> id), 
	CONSTRAINT signal_dispatch_lane_key_nonempty_check CHECK (dispatch_lane_key IS NULL OR dispatch_lane_key <> '')
)

;
CREATE INDEX signal_dispatch_frontier_idx ON signal (seq) WHERE dispatched_at IS NULL;
CREATE INDEX signal_email_received_source_id_idx ON signal (source_id) WHERE type = 'email.received' AND source_type = 'email_message';
CREATE INDEX signal_pending_lane_seq_idx ON signal (space_id, dispatch_lane_key, seq) WHERE dispatched_at IS NULL AND dispatch_lane_key IS NOT NULL;
CREATE INDEX signal_root_signal_id_idx ON signal (root_signal_id) WHERE root_signal_id IS NOT NULL;
CREATE INDEX signal_terminal_catalog_unavailable_created_idx ON signal (created_at, id) WHERE dispatched_at IS NOT NULL AND dispatch_disposition->>'kind' = 'terminal_quarantined' AND dispatch_disposition->>'reason_code' = 'workflow_catalog_unavailable';
CREATE INDEX signal_dispatch_unstarted_frontier_idx ON signal (seq) WHERE dispatched_at IS NULL AND dispatch_started_at IS NULL;
CREATE INDEX signal_chat_received_pending_dispatch_idx ON signal (dispatch_started_at, seq) WHERE type = 'chat.received' AND dispatched_at IS NULL;
CREATE INDEX signal_message_received_source_id_idx ON signal (source_id) WHERE type = 'message.received' AND source_type = 'message';
CREATE INDEX signal_parent_signal_id_idx ON signal (parent_signal_id) WHERE parent_signal_id IS NOT NULL;
CREATE INDEX signal_source_workflow_id_idx ON signal (source_workflow_id) WHERE source_workflow_id IS NOT NULL;
CREATE INDEX signal_dispatch_stale_frontier_idx ON signal (dispatch_started_at, seq) WHERE dispatched_at IS NULL AND dispatch_started_at IS NOT NULL;
CREATE INDEX signal_external_link_received_pending_dispatch_idx ON signal (dispatch_started_at, seq) WHERE type = 'external_link.received' AND dispatched_at IS NULL;
CREATE INDEX signal_calendar_active_source_seq_idx ON signal (source_id, seq DESC, id DESC) WHERE type IN ('calendar.invite_received', 'calendar.event_created', 'calendar.event_updated') AND source_type = 'calendar_event' AND source_id IS NOT NULL;
CREATE INDEX signal_calendar_delete_source_seq_idx ON signal (source_id, seq, id) WHERE type = 'calendar.event_deleted' AND source_type = 'calendar_event' AND source_id IS NOT NULL;
CREATE INDEX signal_subject_type_id_idx ON signal (subject_type, subject_id);
CREATE INDEX signal_space_id_idx ON signal (space_id);
CREATE INDEX signal_calendar_subscription_found_pending_dispatch_idx ON signal (dispatch_started_at, seq) WHERE type = 'calendar.subscription_found' AND dispatched_at IS NULL;

CREATE TABLE space (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	name VARCHAR(1000) NOT NULL, 
	type VARCHAR, 
	fields_loro_snapshot BYTEA NOT NULL, 
	schema_loro_snapshot BYTEA, 
	parent_id UUID, 
	event_log_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	archived_at TIMESTAMP WITH TIME ZONE, 
	removed_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	moved_at_timestamp BIGINT, 
	version BIGINT NOT NULL, 
	task_lifecycle_writer_version SMALLINT DEFAULT 1 NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT space_client_id_idx UNIQUE (client_id), 
	CONSTRAINT space_task_lifecycle_writer_version_check CHECK (task_lifecycle_writer_version IN (1, 2)), 
	UNIQUE (client_id)
)

;
CREATE INDEX space_filter_idx ON space (deleted_at, removed_at, archived_at);
CREATE INDEX space_parent_id_idx ON space (parent_id);
CREATE INDEX space_created_by_user_id_idx ON space (created_by_user_id);
CREATE INDEX space_event_log_id_idx ON space (event_log_id) WHERE event_log_id IS NOT NULL;

CREATE TABLE space_member (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	user_id UUID NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT space_member_space_id_user_id_unique_idx UNIQUE (space_id, user_id)
)

;
CREATE INDEX space_member_space_id_idx ON space_member (space_id);
CREATE INDEX space_member_user_id_idx ON space_member (user_id);

CREATE TABLE standing_grant (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID NOT NULL, 
	grantor_user_id UUID, 
	grantee_user_id UUID, 
	grantor_resource_id UUID, 
	grantee_resource_id UUID, 
	tier VARCHAR NOT NULL, 
	scope VARCHAR NOT NULL, 
	state VARCHAR NOT NULL, 
	activation_epoch INTEGER DEFAULT 1 NOT NULL, 
	origin_invite_id UUID, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	activated_at TIMESTAMP WITH TIME ZONE, 
	revoked_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT standing_grant_tier_check CHECK (tier IN ('viewer', 'editor')), 
	CONSTRAINT standing_grant_scope_check CHECK (scope IN ('all')), 
	CONSTRAINT standing_grant_state_check CHECK (state IN ('pending', 'active', 'revoked')), 
	CONSTRAINT standing_grant_version_check CHECK (version > 0), 
	CONSTRAINT standing_grant_grantor_identity_check CHECK ((grantor_user_id IS NULL) <> (grantor_resource_id IS NULL)), 
	CONSTRAINT standing_grant_grantee_identity_check CHECK ((grantee_user_id IS NULL) <> (grantee_resource_id IS NULL)), 
	CONSTRAINT standing_grant_active_user_identity_check CHECK (state <> 'active' OR (grantor_user_id IS NOT NULL AND grantee_user_id IS NOT NULL AND grantor_resource_id IS NULL AND grantee_resource_id IS NULL)), 
	CONSTRAINT standing_grant_revoked_at_check CHECK ((state = 'revoked') = (revoked_at IS NOT NULL)), 
	CONSTRAINT standing_grant_activated_at_check CHECK ((state = 'active') = (activated_at IS NOT NULL)), 
	UNIQUE (client_id)
)

;
CREATE UNIQUE INDEX standing_grant_live_pair_idx ON standing_grant (grantor_user_id, grantee_user_id, scope) WHERE state <> 'revoked';
CREATE INDEX standing_grant_grantee_active_all_editor_idx ON standing_grant (grantee_user_id, grantor_user_id) WHERE state = 'active' AND scope = 'all' AND tier = 'editor';
CREATE UNIQUE INDEX standing_grant_live_user_resource_pair_idx ON standing_grant (grantor_user_id, grantee_resource_id, scope) WHERE state <> 'revoked' AND grantor_user_id IS NOT NULL AND grantee_resource_id IS NOT NULL;
CREATE UNIQUE INDEX standing_grant_live_resource_user_pair_idx ON standing_grant (grantor_resource_id, grantee_user_id, scope) WHERE state <> 'revoked' AND grantor_resource_id IS NOT NULL AND grantee_user_id IS NOT NULL;

CREATE TABLE standing_grant_backfill_status (
	standing_grant_id UUID NOT NULL, 
	activation_epoch INTEGER NOT NULL, 
	state VARCHAR NOT NULL, 
	watermark_resource_id UUID, 
	watermark_workflow_id UUID, 
	watermark_calendar_event_id UUID, 
	phase VARCHAR, 
	last_error TEXT, 
	started_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	failed_at TIMESTAMP WITH TIME ZONE, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (standing_grant_id, activation_epoch), 
	CONSTRAINT standing_grant_backfill_status_epoch_check CHECK (activation_epoch > 0), 
	CONSTRAINT standing_grant_backfill_status_state_check CHECK (state IN ('running', 'complete', 'failed')), 
	CONSTRAINT standing_grant_backfill_status_completed_at_check CHECK ((state = 'complete') = (completed_at IS NOT NULL)), 
	CONSTRAINT standing_grant_backfill_status_failed_at_check CHECK ((state = 'failed') = (failed_at IS NOT NULL))
)

;

CREATE TABLE suggestion (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	source_type VARCHAR NOT NULL, 
	source_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	payload JSONB NOT NULL, 
	status VARCHAR NOT NULL, 
	version BIGINT DEFAULT 0 NOT NULL, 
	source_version BIGINT, 
	source_context_type VARCHAR, 
	source_context_id UUID, 
	request_id TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	removed_at TIMESTAMP WITH TIME ZONE, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	workflow_id UUID, 
	key TEXT, 
	condition_id UUID, 
	created_by_run_id UUID, 
	last_revised_by_run_id UUID, 
	acceptance_claim_event_log_id UUID, 
	acceptance_claimed_at TIMESTAMP WITH TIME ZONE, 
	acceptance_claim_token UUID, 
	handled_by_user_id UUID, 
	handled_by_display_name TEXT, 
	handled_outcome_record_id UUID, 
	expires_at TIMESTAMP WITH TIME ZONE, 
	handled_source_subject TEXT, 
	handled_source_sent_at TIMESTAMP WITH TIME ZONE, 
	handled_outcome_kind VARCHAR, 
	handled_outcome_title TEXT, 
	handled_outcome_start_date DATE, 
	handled_outcome_end_date DATE, 
	handled_outcome_start_at TIMESTAMP WITH TIME ZONE, 
	handled_outcome_end_at TIMESTAMP WITH TIME ZONE, 
	handled_outcome_reminder_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT suggestion_source_context_pair_check CHECK ((source_context_type IS NULL) = (source_context_id IS NULL)), 
	CONSTRAINT suggestion_source_context_type_check CHECK (source_context_type IN ('record', 'context_discovery_run', 'event_log', 'suggestion_client_id')), 
	CONSTRAINT suggestion_acceptance_claim_fields_check CHECK ((acceptance_claim_event_log_id IS NULL) = (acceptance_claimed_at IS NULL) AND (acceptance_claim_event_log_id IS NULL) = (acceptance_claim_token IS NULL)), 
	CONSTRAINT suggestion_handled_notification_fields_check CHECK ((status = 'handled_notification') = (handled_by_user_id IS NOT NULL AND handled_by_display_name IS NOT NULL AND handled_outcome_record_id IS NOT NULL AND expires_at IS NOT NULL)), 
	UNIQUE (client_id)
)

;
CREATE INDEX suggestion_space_id_status_idx ON suggestion (space_id, status);
CREATE INDEX suggestion_handled_notification_expires_at_idx ON suggestion (expires_at) WHERE status = 'handled_notification' AND expires_at IS NOT NULL;
CREATE INDEX suggestion_space_id_key_created_at_idx ON suggestion (space_id, key, created_at);
CREATE INDEX suggestion_accepted_space_updated_idx ON suggestion (space_id, updated_at DESC) WHERE status = 'accepted' AND removed_at IS NULL AND deleted_at IS NULL;
CREATE INDEX suggestion_source_version_idx ON suggestion (source_type, source_id, source_version);
CREATE INDEX suggestion_pending_unbound_request_id_idx ON suggestion (request_id) WHERE status = 'pending' AND removed_at IS NULL AND deleted_at IS NULL AND request_id IS NOT NULL AND (workflow_id IS NULL OR key IS NULL);
CREATE INDEX suggestion_request_id_idx ON suggestion (request_id);
CREATE UNIQUE INDEX suggestion_family_setup_space_key_unique_idx ON suggestion (space_id, key) WHERE status = 'pending' AND removed_at IS NULL AND deleted_at IS NULL AND source_type = 'family_setup' AND key IS NOT NULL;
CREATE UNIQUE INDEX suggestion_pending_workflow_id_key_unique_idx ON suggestion (workflow_id, key) WHERE status = 'pending' AND removed_at IS NULL AND deleted_at IS NULL AND workflow_id IS NOT NULL AND key IS NOT NULL;
CREATE INDEX suggestion_pending_workflow_space_created_idx ON suggestion (space_id, created_at, id) WHERE status = 'pending' AND removed_at IS NULL AND deleted_at IS NULL AND workflow_id IS NOT NULL AND key IS NOT NULL AND space_id IS NOT NULL;
CREATE INDEX suggestion_workflow_id_idx ON suggestion (workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX suggestion_legacy_workflow_recovery_idx ON suggestion (request_id, space_id, created_at, id) WHERE status = 'pending' AND removed_at IS NULL AND deleted_at IS NULL AND workflow_id IS NULL AND key IS NULL;
CREATE INDEX suggestion_workflow_created_by_run_idx ON suggestion (workflow_id, created_by_run_id) WHERE created_by_run_id IS NOT NULL;
CREATE INDEX suggestion_workflow_run_cards_created_idx ON suggestion (workflow_id, created_at, id) WHERE created_by_run_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX suggestion_handled_outcome_record_id_idx ON suggestion (handled_outcome_record_id) WHERE handled_outcome_record_id IS NOT NULL;
CREATE INDEX suggestion_workflow_last_revised_by_run_idx ON suggestion (workflow_id, last_revised_by_run_id) WHERE last_revised_by_run_id IS NOT NULL;
CREATE INDEX suggestion_acceptance_claim_idx ON suggestion (acceptance_claimed_at) WHERE acceptance_claim_event_log_id IS NOT NULL;
CREATE INDEX suggestion_source_context_type_id_idx ON suggestion (source_context_type, source_context_id) WHERE deleted_at IS NULL;
CREATE INDEX suggestion_acceptance_claim_event_log_id_idx ON suggestion (acceptance_claim_event_log_id) WHERE acceptance_claim_event_log_id IS NOT NULL;
CREATE INDEX suggestion_source_type_id_idx ON suggestion (source_type, source_id);
CREATE INDEX suggestion_rejected_space_updated_idx ON suggestion (space_id, updated_at DESC) WHERE status = 'rejected' AND removed_at IS NULL AND deleted_at IS NULL;

CREATE TABLE suggestion_seen (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	user_id UUID NOT NULL, 
	suggestion_id UUID NOT NULL, 
	last_seen_suggestion_version BIGINT NOT NULL, 
	last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	acknowledged_at TIMESTAMP WITH TIME ZONE, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT suggestion_seen_user_id_suggestion_id_key UNIQUE (user_id, suggestion_id), 
	UNIQUE (client_id)
)

;
CREATE INDEX suggestion_seen_suggestion_id_idx ON suggestion_seen (suggestion_id);
CREATE INDEX suggestion_seen_user_id_idx ON suggestion_seen (user_id);

CREATE TABLE tag (
	id UUID DEFAULT uuidv7() NOT NULL, 
	client_id UUID DEFAULT uuidv7() NOT NULL, 
	name TEXT NOT NULL, 
	normalized_name TEXT NOT NULL, 
	fields_loro_snapshot BYTEA NOT NULL, 
	space_id UUID NOT NULL, 
	shadows_tag_id UUID, 
	color TEXT, 
	icon TEXT, 
	type VARCHAR, 
	confirmed_at TIMESTAMP WITH TIME ZONE, 
	confirmed_by_user_id UUID, 
	version BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	deleted_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT tag_space_id_normalized_name_unique_idx UNIQUE (space_id, normalized_name), 
	UNIQUE (client_id)
)

;
CREATE INDEX tag_normalized_name_idx ON tag (normalized_name);
CREATE INDEX tag_space_id_idx ON tag (space_id);
CREATE INDEX tag_shadows_tag_id_idx ON tag (shadows_tag_id);

CREATE TABLE tagged_item (
	tag_id UUID NOT NULL, 
	target_id UUID NOT NULL, 
	target_type VARCHAR NOT NULL, 
	confirmed_at TIMESTAMP WITH TIME ZONE, 
	confirmed_by_user_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_by_user_id UUID NOT NULL, 
	PRIMARY KEY (tag_id, target_id, target_type)
)

;
CREATE INDEX tagged_item_target_idx ON tagged_item (target_id, target_type);
CREATE INDEX tagged_item_target_type_idx ON tagged_item (target_type);
CREATE INDEX tagged_item_tag_id_idx ON tagged_item (tag_id);

CREATE TABLE upcoming_summary (
	id UUID DEFAULT uuidv7() NOT NULL, 
	space_id UUID NOT NULL, 
	granularity VARCHAR NOT NULL, 
	anchor_date DATE NOT NULL, 
	status VARCHAR DEFAULT 'pending' NOT NULL, 
	timezone TEXT, 
	version BIGINT NOT NULL, 
	last_content_version BIGINT NOT NULL, 
	summary_content_version BIGINT, 
	payload JSONB, 
	active_workflow_id TEXT, 
	computed_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT upcoming_summary_space_granularity_anchor_date_key UNIQUE (space_id, granularity, anchor_date)
)

;
CREATE INDEX upcoming_summary_space_id_idx ON upcoming_summary (space_id);
CREATE INDEX upcoming_summary_space_id_granularity_anchor_date_idx ON upcoming_summary (space_id, granularity, anchor_date);
CREATE INDEX upcoming_summary_space_id_status_idx ON upcoming_summary (space_id, status);

CREATE TABLE "user" (
	id UUID DEFAULT uuidv7() NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	email VARCHAR(1000), 
	first_name VARCHAR(1000), 
	last_name VARCHAR(1000), 
	avatar_key VARCHAR(512), 
	avatar_color VARCHAR, 
	google_avatar_url VARCHAR(1024), 
	password_hash BYTEA, 
	is_admin BOOLEAN NOT NULL, 
	is_internal BOOLEAN DEFAULT false NOT NULL, 
	is_guest BOOLEAN NOT NULL, 
	is_waitlisted BOOLEAN NOT NULL, 
	is_test_user BOOLEAN NOT NULL, 
	mock_providers BOOLEAN DEFAULT true NOT NULL, 
	minimum_ios_build BIGINT, 
	referral_source VARCHAR(80), 
	private_space_id UUID, 
	onboarding_workflow_id UUID, 
	family_setup_record_id UUID, 
	onboarded_at TIMESTAMP WITH TIME ZONE, 
	last_read_activity_id UUID, 
	timezone VARCHAR(50), 
	default_calendar_id UUID, 
	trace_capture_mode VARCHAR NOT NULL, 
	leave_by_enabled BOOLEAN DEFAULT false NOT NULL, 
	leave_by_feature_on BOOLEAN, 
	weather_heads_ups_enabled BOOLEAN DEFAULT true NOT NULL, 
	version BIGINT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT user_email_idx UNIQUE (email), 
	CONSTRAINT user_private_space_id_idx UNIQUE (private_space_id), 
	CONSTRAINT user_onboarding_workflow_id_idx UNIQUE (onboarding_workflow_id)
)

;
CREATE INDEX user_is_internal_idx ON "user" (id) WHERE is_internal;
CREATE INDEX user_default_calendar_id_idx ON "user" (default_calendar_id) WHERE default_calendar_id IS NOT NULL;
CREATE INDEX user_family_setup_record_id_idx ON "user" (family_setup_record_id);

CREATE TABLE workflow (
	id UUID DEFAULT uuidv7() NOT NULL, 
	type VARCHAR NOT NULL, 
	title TEXT NOT NULL, 
	summary TEXT, 
	routing_hint TEXT, 
	status VARCHAR DEFAULT 'open' NOT NULL, 
	fields_loro_snapshot BYTEA, 
	has_matcher BOOLEAN DEFAULT false NOT NULL, 
	has_handler BOOLEAN DEFAULT false NOT NULL, 
	compaction_version INTEGER DEFAULT 1 NOT NULL, 
	revision BIGINT DEFAULT 1 NOT NULL, 
	descendant_commit_hold_sources JSONB DEFAULT '[]'::jsonb NOT NULL, 
	workflow_run_lease_owner_id UUID, 
	workflow_run_lease_expires_at TIMESTAMP WITH TIME ZONE, 
	repair_failure_type TEXT, 
	repair_last_error JSONB, 
	repair_consecutive_failures INTEGER DEFAULT 0 NOT NULL, 
	repair_suspended_at TIMESTAMP WITH TIME ZONE, 
	created_by_user_id UUID NOT NULL, 
	space_id UUID NOT NULL, 
	record_id UUID, 
	parent_id UUID, 
	source_signal_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	resolved_at TIMESTAMP WITH TIME ZONE, 
	archived_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT workflow_repair_consecutive_failures_check CHECK (repair_consecutive_failures >= 0)
)

;
CREATE INDEX workflow_repair_suspended_idx ON workflow (repair_suspended_at) WHERE repair_suspended_at IS NOT NULL;
CREATE UNIQUE INDEX workflow_record_id_unique_idx ON workflow (record_id) WHERE record_id IS NOT NULL AND archived_at IS NULL;
CREATE INDEX workflow_record_id_idx ON workflow (record_id) WHERE record_id IS NOT NULL;
CREATE INDEX workflow_recent_open_idx ON workflow (space_id, updated_at) WHERE status = 'open';
CREATE INDEX workflow_source_signal_id_idx ON workflow (source_signal_id) WHERE source_signal_id IS NOT NULL;
CREATE INDEX workflow_parent_id_idx ON workflow (parent_id) WHERE parent_id IS NOT NULL;

CREATE TABLE workflow_run (
	id UUID DEFAULT uuidv7() NOT NULL, 
	signal_id UUID NOT NULL, 
	workflow_id UUID NOT NULL, 
	turn_message_id UUID, 
	handler_version TEXT, 
	agent_run_id UUID, 
	routing_context JSONB, 
	resource_signal_context JSONB, 
	arrival_provenance VARCHAR DEFAULT 'router' NOT NULL, 
	status VARCHAR DEFAULT 'ok' NOT NULL, 
	error JSONB, 
	state_before_frontiers TEXT, 
	workflow_runtime_state_id UUID, 
	runtime_input_event_id UUID, 
	note_admissions JSONB, 
	state_revision_before BIGINT, 
	state_revision_after BIGINT, 
	monty_checkpoint_before_id UUID, 
	monty_checkpoint_after_id UUID, 
	interaction_profile VARCHAR, 
	model_profile VARCHAR, 
	routine_installation_id UUID, 
	routine_installation_version BIGINT, 
	routine_id UUID, 
	routine_revision_id UUID, 
	routine_chain_root_workflow_run_id UUID, 
	routine_reentry_ordinal INTEGER, 
	routine_status VARCHAR, 
	routine_output JSONB, 
	routine_output_artifact_ref TEXT, 
	routine_diagnostic JSONB, 
	compaction_version INTEGER DEFAULT 1 NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	stalled_alert_claimed_at TIMESTAMP WITH TIME ZONE, 
	stalled_alert_emitted_at TIMESTAMP WITH TIME ZONE, 
	stalled_alert_attempt_count INTEGER DEFAULT 0 NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT workflow_run_note_admissions_check CHECK (note_admissions IS NULL OR (jsonb_typeof(note_admissions) = 'array' AND jsonb_array_length(note_admissions) BETWEEN 1 AND 2)), 
	CONSTRAINT workflow_run_signal_id_workflow_id_idx UNIQUE (signal_id, workflow_id), 
	CONSTRAINT workflow_run_workflow_installation_id_id_unique_idx UNIQUE (workflow_id, routine_installation_id, id), 
	CONSTRAINT workflow_run_handoff_workflow_owner_unique_idx UNIQUE (workflow_id, id, routine_installation_id, routine_revision_id), 
	CONSTRAINT workflow_run_handoff_routine_owner_unique_idx UNIQUE (id, routine_installation_id, routine_revision_id), 
	CONSTRAINT workflow_run_routine_chain_owner_unique_idx UNIQUE (workflow_id, id, routine_installation_id, routine_id, routine_revision_id), 
	CONSTRAINT workflow_run_runtime_before_revision_check CHECK (state_revision_before IS NULL OR state_revision_before >= 0), 
	CONSTRAINT workflow_run_runtime_after_revision_check CHECK (state_revision_after IS NULL OR (state_revision_before IS NOT NULL AND state_revision_after >= state_revision_before)), 
	CONSTRAINT workflow_run_runtime_pin_shape_check CHECK ((workflow_runtime_state_id IS NULL AND runtime_input_event_id IS NULL AND state_revision_before IS NULL AND state_revision_after IS NULL AND monty_checkpoint_before_id IS NULL AND monty_checkpoint_after_id IS NULL AND interaction_profile IS NULL AND model_profile IS NULL AND routine_installation_id IS NULL AND routine_installation_version IS NULL AND routine_id IS NULL AND routine_revision_id IS NULL) OR (workflow_runtime_state_id IS NOT NULL AND runtime_input_event_id IS NOT NULL AND state_revision_before IS NOT NULL AND interaction_profile IS NOT NULL)), 
	CONSTRAINT workflow_run_routine_pin_shape_check CHECK ((routine_installation_id IS NULL AND routine_installation_version IS NULL AND routine_id IS NULL AND routine_revision_id IS NULL AND routine_chain_root_workflow_run_id IS NULL AND routine_reentry_ordinal IS NULL AND routine_status IS NULL AND routine_output IS NULL AND routine_output_artifact_ref IS NULL AND routine_diagnostic IS NULL) OR (routine_installation_id IS NOT NULL AND routine_installation_version IS NOT NULL AND routine_installation_version >= 0 AND routine_id IS NOT NULL AND routine_revision_id IS NOT NULL AND routine_chain_root_workflow_run_id IS NOT NULL AND routine_reentry_ordinal IS NOT NULL AND routine_reentry_ordinal >= 0 AND routine_reentry_ordinal <= 32 AND routine_status IS NOT NULL AND agent_run_id IS NULL AND model_profile IS NULL AND ((routine_reentry_ordinal = 0) = (routine_chain_root_workflow_run_id = id)) AND ((routine_status = 'running' AND status = 'pending' AND state_revision_after IS NULL AND monty_checkpoint_after_id IS NULL AND routine_output IS NULL AND routine_output_artifact_ref IS NULL AND routine_diagnostic IS NULL AND error IS NULL) OR (routine_status = 'succeeded' AND status = 'ok' AND state_revision_after IS NOT NULL AND monty_checkpoint_after_id IS NOT NULL AND ((routine_output IS NULL) <> (routine_output_artifact_ref IS NULL)) AND (routine_output_artifact_ref IS NULL OR char_length(routine_output_artifact_ref) BETWEEN 1 AND 2048) AND routine_diagnostic IS NULL AND error IS NULL) OR (routine_status = 'deferred' AND status = 'ok' AND state_revision_after IS NOT NULL AND monty_checkpoint_after_id IS NOT NULL AND routine_output IS NULL AND routine_output_artifact_ref IS NULL AND routine_diagnostic IS NOT NULL AND jsonb_typeof(routine_diagnostic) = 'object' AND octet_length(routine_diagnostic::text) <= 16384 AND COALESCE(routine_diagnostic->>'kind' = 'deferred', FALSE) AND error IS NULL) OR (routine_status = 'failed' AND status = 'error' AND state_revision_after IS NULL AND monty_checkpoint_after_id IS NULL AND routine_output IS NULL AND routine_output_artifact_ref IS NULL AND routine_diagnostic IS NOT NULL AND jsonb_typeof(routine_diagnostic) = 'object' AND octet_length(routine_diagnostic::text) <= 16384 AND COALESCE(routine_diagnostic->>'kind' = 'failure', FALSE) AND error IS NOT NULL AND jsonb_typeof(error) = 'object' AND octet_length(error::text) <= 16384))))
)

;
CREATE INDEX workflow_run_workflow_id_created_at_idx ON workflow_run (workflow_id, created_at);
CREATE INDEX workflow_run_agent_run_id_idx ON workflow_run (agent_run_id) WHERE agent_run_id IS NOT NULL;
CREATE INDEX workflow_run_monty_checkpoint_before_id_idx ON workflow_run (monty_checkpoint_before_id) WHERE monty_checkpoint_before_id IS NOT NULL;
CREATE INDEX workflow_run_otto_email_stalled_due_idx ON workflow_run (created_at, id) WHERE status = 'pending' AND stalled_alert_emitted_at IS NULL AND stalled_alert_attempt_count < 2;
CREATE INDEX workflow_run_signal_id_idx ON workflow_run (signal_id);
CREATE UNIQUE INDEX workflow_run_workflow_id_id_unique_idx ON workflow_run (workflow_id, id);
CREATE INDEX workflow_run_monty_checkpoint_after_id_idx ON workflow_run (monty_checkpoint_after_id) WHERE monty_checkpoint_after_id IS NOT NULL;
CREATE UNIQUE INDEX workflow_run_routine_chain_ordinal_idx ON workflow_run (workflow_id, routine_chain_root_workflow_run_id, routine_reentry_ordinal) WHERE routine_chain_root_workflow_run_id IS NOT NULL;

CREATE TABLE workflow_runtime_event (
	id UUID DEFAULT uuidv7() NOT NULL, 
	workflow_id UUID NOT NULL, 
	runtime_state_id UUID NOT NULL, 
	workflow_run_id UUID, 
	sequence BIGINT NOT NULL, 
	dedup_key TEXT NOT NULL, 
	event_kind VARCHAR NOT NULL, 
	interaction_profile VARCHAR NOT NULL, 
	source_type VARCHAR NOT NULL, 
	source_id TEXT NOT NULL, 
	payload_kind VARCHAR NOT NULL, 
	inline_payload JSONB, 
	inline_payload_canonical BYTEA, 
	blob_key TEXT, 
	blob_sha256 BYTEA, 
	blob_bytes BIGINT, 
	target_event_id UUID, 
	causation_event_id UUID, 
	correlation_id UUID NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT workflow_runtime_event_workflow_id_id_unique_idx UNIQUE (workflow_id, id), 
	CONSTRAINT workflow_runtime_event_runtime_state_id_id_unique_idx UNIQUE (runtime_state_id, id), 
	CONSTRAINT workflow_runtime_event_runtime_sequence_unique_idx UNIQUE (runtime_state_id, sequence), 
	CONSTRAINT workflow_runtime_event_runtime_dedup_unique_idx UNIQUE (runtime_state_id, dedup_key), 
	CONSTRAINT workflow_runtime_event_sequence_check CHECK (sequence >= 1), 
	CONSTRAINT workflow_runtime_event_dedup_key_check CHECK (char_length(dedup_key) BETWEEN 1 AND 512), 
	CONSTRAINT workflow_runtime_event_kind_check CHECK (event_kind IN ('input', 'assistant', 'tool_call', 'tool_result', 'record', 'workflow', 'summary', 'system')), 
	CONSTRAINT workflow_runtime_event_profile_check CHECK (interaction_profile IN ('interactive', 'background', 'maintenance')), 
	CONSTRAINT workflow_runtime_event_source_type_check CHECK (source_type IN ('signal', 'record_event', 'workflow_run', 'tool_invocation', 'summary_compaction', 'system')), 
	CONSTRAINT workflow_runtime_event_source_id_check CHECK (char_length(source_id) BETWEEN 1 AND 512), 
	CONSTRAINT workflow_runtime_event_payload_kind_check CHECK (payload_kind IN ('inline', 'blob', 'tombstone')), 
	CONSTRAINT workflow_runtime_event_payload_shape_check CHECK ((payload_kind = 'inline' AND inline_payload IS NOT NULL AND inline_payload_canonical IS NOT NULL AND blob_key IS NULL AND blob_sha256 IS NULL AND blob_bytes IS NULL AND target_event_id IS NULL) OR (payload_kind = 'blob' AND inline_payload IS NULL AND inline_payload_canonical IS NULL AND blob_key IS NOT NULL AND blob_sha256 IS NOT NULL AND blob_bytes IS NOT NULL AND target_event_id IS NULL) OR (payload_kind = 'tombstone' AND inline_payload IS NULL AND inline_payload_canonical IS NULL AND blob_key IS NULL AND blob_sha256 IS NULL AND blob_bytes IS NULL AND target_event_id IS NOT NULL)), 
	CONSTRAINT workflow_runtime_event_inline_payload_bytes_check CHECK (inline_payload_canonical IS NULL OR octet_length(inline_payload_canonical) <= 49024), 
	CONSTRAINT workflow_runtime_event_blob_key_check CHECK (blob_key IS NULL OR char_length(blob_key) BETWEEN 1 AND 1024), 
	CONSTRAINT workflow_runtime_event_blob_sha256_check CHECK (blob_sha256 IS NULL OR octet_length(blob_sha256) = 32), 
	CONSTRAINT workflow_runtime_event_blob_bytes_check CHECK (blob_bytes IS NULL OR blob_bytes BETWEEN 1 AND 67108864)
)

;
CREATE INDEX workflow_runtime_event_correlation_created_idx ON workflow_runtime_event (correlation_id, created_at, id);
CREATE INDEX workflow_runtime_event_workflow_run_sequence_idx ON workflow_runtime_event (workflow_run_id, sequence);

CREATE TABLE workflow_runtime_state (
	id UUID DEFAULT uuidv7() NOT NULL, 
	workflow_id UUID NOT NULL, 
	runtime_schema_version INTEGER DEFAULT 1 NOT NULL, 
	state_revision BIGINT DEFAULT 0 NOT NULL, 
	next_event_sequence BIGINT DEFAULT 1 NOT NULL, 
	processed_through_sequence BIGINT DEFAULT 0 NOT NULL, 
	context_summary JSONB DEFAULT jsonb_build_object('v', 1, 'sections', '[]'::jsonb) NOT NULL, 
	context_summary_revision BIGINT DEFAULT 0 NOT NULL, 
	context_epoch JSONB DEFAULT jsonb_build_object('v', 1, 'sections', '[]'::jsonb) NOT NULL, 
	context_epoch_revision BIGINT DEFAULT 0 NOT NULL, 
	context_covered_through_sequence BIGINT DEFAULT 0 NOT NULL, 
	current_monty_checkpoint_id UUID, 
	pending_continuation_event_id UUID, 
	lifecycle_state VARCHAR DEFAULT 'active' NOT NULL, 
	lifecycle_reason VARCHAR, 
	retention_deadline_at TIMESTAMP WITH TIME ZONE, 
	erasure_requested_at TIMESTAMP WITH TIME ZONE, 
	purged_at TIMESTAMP WITH TIME ZONE, 
	last_active_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	context_last_compacted_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT workflow_runtime_state_workflow_id_id_unique_idx UNIQUE (workflow_id, id), 
	CONSTRAINT workflow_runtime_state_schema_version_check CHECK (runtime_schema_version >= 1), 
	CONSTRAINT workflow_runtime_state_revision_check CHECK (state_revision >= 0), 
	CONSTRAINT workflow_runtime_state_next_sequence_check CHECK (next_event_sequence >= 1), 
	CONSTRAINT workflow_runtime_state_processed_sequence_check CHECK (processed_through_sequence BETWEEN 0 AND next_event_sequence - 1), 
	CONSTRAINT workflow_runtime_state_context_summary_revision_check CHECK (context_summary_revision >= 0), 
	CONSTRAINT workflow_runtime_state_context_epoch_revision_check CHECK (context_epoch_revision BETWEEN 0 AND context_summary_revision), 
	CONSTRAINT workflow_runtime_state_context_coverage_check CHECK (context_covered_through_sequence BETWEEN 0 AND processed_through_sequence), 
	CONSTRAINT workflow_runtime_state_context_summary_bytes_check CHECK (octet_length(context_summary::text) <= 65536), 
	CONSTRAINT workflow_runtime_state_context_epoch_bytes_check CHECK (octet_length(context_epoch::text) <= 65536), 
	CONSTRAINT workflow_runtime_state_lifecycle_state_check CHECK (lifecycle_state IN ('active', 'retained', 'cold', 'erasing', 'erased')), 
	CONSTRAINT workflow_runtime_state_lifecycle_reason_check CHECK (lifecycle_reason IS NULL OR lifecycle_reason IN ('workflow_closed', 'workflow_archived', 'workflow_deleted', 'privacy_request', 'space_deleted', 'retention_expired')), 
	CONSTRAINT workflow_runtime_state_retention_deadline_check CHECK ((lifecycle_state = 'active' AND retention_deadline_at IS NULL) OR (lifecycle_state = 'retained' AND retention_deadline_at IS NOT NULL) OR lifecycle_state IN ('cold', 'erasing', 'erased')), 
	CONSTRAINT workflow_runtime_state_erasure_requested_check CHECK (erasure_requested_at IS NULL OR lifecycle_state IN ('erasing', 'erased')), 
	CONSTRAINT workflow_runtime_state_purged_check CHECK ((lifecycle_state = 'erased') = (purged_at IS NOT NULL)), 
	UNIQUE (workflow_id)
)

;
CREATE INDEX workflow_runtime_state_pending_continuation_event_id_idx ON workflow_runtime_state (pending_continuation_event_id) WHERE pending_continuation_event_id IS NOT NULL;
CREATE INDEX workflow_runtime_state_context_compaction_due_idx ON workflow_runtime_state (COALESCE(context_last_compacted_at, created_at), last_active_at, id);
ALTER TABLE external_link ADD CONSTRAINT external_link_connect_card_message_id_fk FOREIGN KEY(connect_card_message_id) REFERENCES message (id) ON DELETE SET NULL;
ALTER TABLE external_link ADD CONSTRAINT external_link_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id) ON DELETE CASCADE;
ALTER TABLE external_link ADD CONSTRAINT external_link_connect_workflow_id_fk FOREIGN KEY(connect_workflow_id) REFERENCES workflow (id) ON DELETE SET NULL;
ALTER TABLE external_link ADD CONSTRAINT external_link_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE SET NULL;
ALTER TABLE "user" ADD CONSTRAINT user_onboarding_workflow_id_fk FOREIGN KEY(onboarding_workflow_id) REFERENCES workflow (id) ON DELETE SET NULL;
ALTER TABLE "user" ADD CONSTRAINT user_family_setup_record_id_fk FOREIGN KEY(family_setup_record_id) REFERENCES record (id) ON DELETE SET NULL;
ALTER TABLE "user" ADD CONSTRAINT user_default_calendar_id_fk FOREIGN KEY(default_calendar_id) REFERENCES calendar (id) ON DELETE SET NULL;
ALTER TABLE "user" ADD CONSTRAINT user_last_read_activity_id_fk FOREIGN KEY(last_read_activity_id) REFERENCES activity (id);
ALTER TABLE "user" ADD CONSTRAINT user_private_space_id_fk FOREIGN KEY(private_space_id) REFERENCES space (id) ON DELETE SET NULL;
ALTER TABLE activity ADD CONSTRAINT activity_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE activity ADD CONSTRAINT activity_related_record_id_fk FOREIGN KEY(related_record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE activity ADD CONSTRAINT activity_event_log_id_fk FOREIGN KEY(event_log_id) REFERENCES event_log (id);
ALTER TABLE activity ADD CONSTRAINT activity_related_suggestion_id_fk FOREIGN KEY(related_suggestion_id) REFERENCES suggestion (id) ON DELETE CASCADE;
ALTER TABLE activity ADD CONSTRAINT activity_related_message_id_fk FOREIGN KEY(related_message_id) REFERENCES message (id) ON DELETE CASCADE;
ALTER TABLE space ADD CONSTRAINT space_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE space ADD CONSTRAINT space_event_log_id_fk FOREIGN KEY(event_log_id) REFERENCES event_log (id) DEFERRABLE;
ALTER TABLE space ADD CONSTRAINT space_parent_id_fk FOREIGN KEY(parent_id) REFERENCES space (id);
ALTER TABLE event_log ADD CONSTRAINT event_log_source_user_id_fk FOREIGN KEY(source_user_id) REFERENCES "user" (id);
ALTER TABLE event_log ADD CONSTRAINT event_log_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) DEFERRABLE;
ALTER TABLE event_log ADD CONSTRAINT event_log_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) DEFERRABLE;
ALTER TABLE record ADD CONSTRAINT record_parent_id_fk FOREIGN KEY(parent_id) REFERENCES record (id);
ALTER TABLE record ADD CONSTRAINT record_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE record ADD CONSTRAINT record_event_log_id_fk FOREIGN KEY(event_log_id) REFERENCES event_log (id) DEFERRABLE;
ALTER TABLE record ADD CONSTRAINT record_source_calendar_event_id_fk FOREIGN KEY(source_calendar_event_id) REFERENCES calendar_event (id) ON DELETE SET NULL DEFERRABLE;
ALTER TABLE record ADD CONSTRAINT record_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record ADD CONSTRAINT record_recurring_parent_id_fk FOREIGN KEY(recurring_parent_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE record ADD CONSTRAINT record_source_suggestion_id_fk FOREIGN KEY(source_suggestion_id) REFERENCES suggestion (id) DEFERRABLE;
ALTER TABLE record ADD CONSTRAINT record_superseded_by_record_id_fk FOREIGN KEY(superseded_by_record_id) REFERENCES record (id) ON DELETE RESTRICT;
ALTER TABLE suggestion ADD CONSTRAINT suggestion_handled_outcome_record_id_fk FOREIGN KEY(handled_outcome_record_id) REFERENCES record (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_acceptance_claim_event_log_id_fk FOREIGN KEY(acceptance_claim_event_log_id) REFERENCES event_log (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_created_by_run_id_fk FOREIGN KEY(created_by_run_id) REFERENCES workflow_run (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_handled_by_user_id_fk FOREIGN KEY(handled_by_user_id) REFERENCES "user" (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_last_revised_by_run_id_fk FOREIGN KEY(last_revised_by_run_id) REFERENCES workflow_run (id);
ALTER TABLE suggestion ADD CONSTRAINT suggestion_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id);
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_routine_chain_root_owner_fk FOREIGN KEY(workflow_id, routine_chain_root_workflow_run_id, routine_installation_id, routine_id, routine_revision_id) REFERENCES workflow_run (workflow_id, id, routine_installation_id, routine_id, routine_revision_id) ON DELETE CASCADE;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id);
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_monty_checkpoint_after_same_runtime_fk FOREIGN KEY(workflow_runtime_state_id, monty_checkpoint_after_id) REFERENCES monty_runtime_checkpoint (runtime_state_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_workflow_runtime_state_workflow_id_fk FOREIGN KEY(workflow_id, workflow_runtime_state_id) REFERENCES workflow_runtime_state (workflow_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_routine_revision_routine_id_fk FOREIGN KEY(routine_id, routine_revision_id) REFERENCES routine_revision (routine_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_routine_installation_workflow_id_fk FOREIGN KEY(workflow_id, routine_installation_id, routine_id) REFERENCES routine_installation (workflow_id, id, routine_id) ON DELETE RESTRICT;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_agent_run_id_fk FOREIGN KEY(agent_run_id) REFERENCES agent_run (id);
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_signal_id_fk FOREIGN KEY(signal_id) REFERENCES signal (id);
ALTER TABLE workflow_run ADD CONSTRAINT workflow_run_monty_checkpoint_before_same_runtime_fk FOREIGN KEY(workflow_runtime_state_id, monty_checkpoint_before_id) REFERENCES monty_runtime_checkpoint (runtime_state_id, id) ON DELETE RESTRICT;
ALTER TABLE monty_runtime_checkpoint ADD CONSTRAINT monty_runtime_checkpoint_created_by_workflow_run_workflow_id_fk FOREIGN KEY(workflow_id, created_by_workflow_run_id) REFERENCES workflow_run (workflow_id, id) ON DELETE RESTRICT;
ALTER TABLE monty_runtime_checkpoint ADD CONSTRAINT monty_runtime_checkpoint_runtime_state_workflow_id_fk FOREIGN KEY(workflow_id, runtime_state_id) REFERENCES workflow_runtime_state (workflow_id, id) ON DELETE RESTRICT;
ALTER TABLE monty_runtime_checkpoint ADD CONSTRAINT monty_runtime_checkpoint_parent_same_runtime_fk FOREIGN KEY(runtime_state_id, parent_checkpoint_id) REFERENCES monty_runtime_checkpoint (runtime_state_id, id) ON DELETE RESTRICT;
ALTER TABLE monty_runtime_checkpoint ADD CONSTRAINT monty_runtime_checkpoint_created_by_agent_run_id_fk FOREIGN KEY(created_by_agent_run_id) REFERENCES agent_run (id) ON DELETE RESTRICT;
ALTER TABLE agent_run ADD CONSTRAINT agent_run_parent_space_fk FOREIGN KEY(space_id, parent_agent_run_id) REFERENCES agent_run (space_id, id) ON DELETE RESTRICT;
ALTER TABLE agent_run ADD CONSTRAINT agent_run_workflow_run_id_fk FOREIGN KEY(workflow_run_id) REFERENCES workflow_run (id) ON DELETE RESTRICT;
ALTER TABLE agent_run ADD CONSTRAINT agent_run_parent_agent_run_id_fk FOREIGN KEY(parent_agent_run_id) REFERENCES agent_run (id);
ALTER TABLE agent_run ADD CONSTRAINT agent_run_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE workflow_runtime_state ADD CONSTRAINT workflow_runtime_state_current_monty_checkpoint_same_runtime_fk FOREIGN KEY(id, current_monty_checkpoint_id) REFERENCES monty_runtime_checkpoint (runtime_state_id, id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE workflow_runtime_state ADD CONSTRAINT workflow_runtime_state_pending_continuation_event_fk FOREIGN KEY(pending_continuation_event_id) REFERENCES workflow_runtime_event (id) ON DELETE SET NULL;
ALTER TABLE workflow_runtime_state ADD CONSTRAINT workflow_runtime_state_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id) ON DELETE RESTRICT;
ALTER TABLE workflow ADD CONSTRAINT workflow_parent_id_fk FOREIGN KEY(parent_id) REFERENCES workflow (id) ON DELETE RESTRICT;
ALTER TABLE workflow ADD CONSTRAINT workflow_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE workflow ADD CONSTRAINT workflow_source_signal_id_fk FOREIGN KEY(source_signal_id) REFERENCES signal (id);
ALTER TABLE workflow ADD CONSTRAINT workflow_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE SET NULL;
ALTER TABLE workflow ADD CONSTRAINT workflow_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE signal ADD CONSTRAINT signal_source_workflow_id_fk FOREIGN KEY(source_workflow_id) REFERENCES workflow (id);
ALTER TABLE signal ADD CONSTRAINT signal_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE signal ADD CONSTRAINT signal_parent_signal_id_fk FOREIGN KEY(parent_signal_id) REFERENCES signal (id) ON DELETE SET NULL;
ALTER TABLE signal ADD CONSTRAINT signal_root_signal_id_fk FOREIGN KEY(root_signal_id) REFERENCES signal (id);
ALTER TABLE workflow_runtime_event ADD CONSTRAINT workflow_runtime_event_target_same_runtime_fk FOREIGN KEY(runtime_state_id, target_event_id) REFERENCES workflow_runtime_event (runtime_state_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_runtime_event ADD CONSTRAINT workflow_runtime_event_runtime_state_workflow_id_fk FOREIGN KEY(workflow_id, runtime_state_id) REFERENCES workflow_runtime_state (workflow_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_runtime_event ADD CONSTRAINT workflow_runtime_event_causation_same_runtime_fk FOREIGN KEY(runtime_state_id, causation_event_id) REFERENCES workflow_runtime_event (runtime_state_id, id) ON DELETE RESTRICT;
ALTER TABLE workflow_runtime_event ADD CONSTRAINT workflow_runtime_event_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id) ON DELETE RESTRICT;
ALTER TABLE workflow_runtime_event ADD CONSTRAINT workflow_runtime_event_workflow_run_workflow_id_fk FOREIGN KEY(workflow_id, workflow_run_id) REFERENCES workflow_run (workflow_id, id) ON DELETE RESTRICT;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_routine_id_fk FOREIGN KEY(routine_id) REFERENCES routine (id) ON DELETE RESTRICT;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_repair_task_record_id_fk FOREIGN KEY(repair_task_record_id) REFERENCES record (id) ON DELETE SET NULL;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_active_revision_routine_id_fk FOREIGN KEY(routine_id, active_revision_id) REFERENCES routine_revision (routine_id, id) ON DELETE RESTRICT;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_repair_expected_revision_fk FOREIGN KEY(routine_id, repair_expected_active_revision_id) REFERENCES routine_revision (routine_id, id) ON DELETE RESTRICT;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id) ON DELETE SET NULL;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id) ON DELETE CASCADE;
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_last_failed_workflow_run_fk FOREIGN KEY(workflow_id, id, last_failed_workflow_run_id) REFERENCES workflow_run (workflow_id, routine_installation_id, id) ON DELETE SET NULL (last_failed_workflow_run_id);
ALTER TABLE routine_installation ADD CONSTRAINT routine_installation_last_successful_workflow_run_fk FOREIGN KEY(workflow_id, id, last_successful_workflow_run_id) REFERENCES workflow_run (workflow_id, routine_installation_id, id) ON DELETE SET NULL (last_successful_workflow_run_id);
ALTER TABLE routine_revision ADD CONSTRAINT routine_revision_routine_id_fk FOREIGN KEY(routine_id) REFERENCES routine (id) ON DELETE RESTRICT;
ALTER TABLE routine_revision ADD CONSTRAINT routine_revision_parent_routine_id_fk FOREIGN KEY(routine_id, parent_revision_id) REFERENCES routine_revision (routine_id, id) ON DELETE RESTRICT;
ALTER TABLE routine_revision ADD CONSTRAINT routine_revision_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id) ON DELETE SET NULL;
ALTER TABLE routine_revision ADD CONSTRAINT routine_revision_created_by_agent_run_id_fk FOREIGN KEY(created_by_agent_run_id) REFERENCES agent_run (id) ON DELETE SET NULL;
ALTER TABLE routine ADD CONSTRAINT routine_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id) ON DELETE SET NULL;
ALTER TABLE routine ADD CONSTRAINT routine_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE calendar_event ADD CONSTRAINT calendar_event_linked_record_id_fk FOREIGN KEY(linked_record_id) REFERENCES record (id) DEFERRABLE;
ALTER TABLE calendar_event ADD CONSTRAINT calendar_event_dirty_by_user_id_fk FOREIGN KEY(dirty_by_user_id) REFERENCES "user" (id);
ALTER TABLE calendar_event ADD CONSTRAINT calendar_event_calendar_id_fk FOREIGN KEY(calendar_id) REFERENCES calendar (id);
ALTER TABLE calendar_event ADD CONSTRAINT calendar_event_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE calendar ADD CONSTRAINT calendar_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE calendar ADD CONSTRAINT calendar_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE SET NULL;
ALTER TABLE calendar ADD CONSTRAINT calendar_external_account_id_fk FOREIGN KEY(external_account_id) REFERENCES external_account (id);
ALTER TABLE external_account ADD CONSTRAINT external_account_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE SET NULL;
ALTER TABLE external_account ADD CONSTRAINT external_account_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE resource ADD CONSTRAINT resource_superseded_by_resource_id_fk FOREIGN KEY(superseded_by_resource_id) REFERENCES resource (id) ON DELETE RESTRICT;
ALTER TABLE resource ADD CONSTRAINT resource_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE RESTRICT;
ALTER TABLE resource ADD CONSTRAINT resource_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE SET NULL;
ALTER TABLE resource ADD CONSTRAINT resource_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE resource ADD CONSTRAINT resource_parent_id_space_id_fk FOREIGN KEY(parent_id, space_id) REFERENCES resource (id, space_id);
ALTER TABLE resource ADD CONSTRAINT resource_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE message ADD CONSTRAINT message_composer_submission_id_fk FOREIGN KEY(composer_submission_id) REFERENCES composer_submission (id) ON DELETE RESTRICT;
ALTER TABLE message ADD CONSTRAINT message_event_log_id_fk FOREIGN KEY(event_log_id) REFERENCES event_log (id);
ALTER TABLE message ADD CONSTRAINT message_reply_to_message_id_fk FOREIGN KEY(reply_to_message_id) REFERENCES message (id) ON DELETE SET NULL;
ALTER TABLE message ADD CONSTRAINT message_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE message ADD CONSTRAINT message_source_channel_event_id_fk FOREIGN KEY(source_channel_event_id) REFERENCES channel_event (id) ON DELETE SET NULL;
ALTER TABLE message ADD CONSTRAINT message_agent_run_id_fk FOREIGN KEY(agent_run_id) REFERENCES agent_run (id) ON DELETE SET NULL;
ALTER TABLE message ADD FOREIGN KEY(parent_id) REFERENCES message (id);
ALTER TABLE message ADD CONSTRAINT message_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE composer_submission ADD CONSTRAINT composer_submission_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id) ON DELETE CASCADE;
ALTER TABLE composer_submission ADD CONSTRAINT composer_submission_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE channel_event ADD CONSTRAINT channel_event_source_user_id_fk FOREIGN KEY(source_user_id) REFERENCES "user" (id);
ALTER TABLE channel_event ADD CONSTRAINT channel_event_external_conversation_id_fk FOREIGN KEY(external_conversation_id) REFERENCES channel_conversation (id) ON DELETE SET NULL;
ALTER TABLE channel_event ADD CONSTRAINT channel_event_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE channel_conversation ADD CONSTRAINT channel_conversation_workflow_id_fk FOREIGN KEY(workflow_id) REFERENCES workflow (id);
ALTER TABLE channel_conversation ADD CONSTRAINT channel_conversation_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE standing_grant_backfill_status ADD CONSTRAINT standing_grant_backfill_status_standing_grant_id_fk FOREIGN KEY(standing_grant_id) REFERENCES standing_grant (id) ON DELETE CASCADE;
ALTER TABLE standing_grant ADD CONSTRAINT standing_grant_grantee_user_id_fk FOREIGN KEY(grantee_user_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE standing_grant ADD CONSTRAINT standing_grant_grantee_resource_id_fk FOREIGN KEY(grantee_resource_id) REFERENCES resource (id) ON DELETE RESTRICT;
ALTER TABLE standing_grant ADD CONSTRAINT standing_grant_grantor_resource_id_fk FOREIGN KEY(grantor_resource_id) REFERENCES resource (id) ON DELETE RESTRICT;
ALTER TABLE standing_grant ADD CONSTRAINT standing_grant_grantor_user_id_fk FOREIGN KEY(grantor_user_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE standing_grant ADD CONSTRAINT standing_grant_origin_invite_id_fk FOREIGN KEY(origin_invite_id) REFERENCES invite (id) ON DELETE RESTRICT;
ALTER TABLE invite ADD CONSTRAINT invite_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE invite ADD CONSTRAINT invite_exchanged_by_user_id_fk FOREIGN KEY(exchanged_by_user_id) REFERENCES "user" (id);
ALTER TABLE invite ADD CONSTRAINT invite_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE invite ADD CONSTRAINT invite_contact_address_id_fk FOREIGN KEY(contact_address_id) REFERENCES contact_address (id) ON DELETE RESTRICT;
ALTER TABLE file_share ADD CONSTRAINT file_share_message_id_fk FOREIGN KEY(message_id) REFERENCES message (id);
ALTER TABLE file_share ADD CONSTRAINT file_share_target_space_id_fk FOREIGN KEY(target_space_id) REFERENCES space (id);
ALTER TABLE file_share ADD CONSTRAINT file_share_file_id_fk FOREIGN KEY(file_id) REFERENCES file (id);
ALTER TABLE file_share ADD CONSTRAINT file_share_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE file ADD CONSTRAINT file_channel_event_id_fk FOREIGN KEY(channel_event_id) REFERENCES channel_event (id) ON DELETE CASCADE;
ALTER TABLE file ADD CONSTRAINT file_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE file ADD CONSTRAINT file_external_link_item_id_fk FOREIGN KEY(external_link_item_id) REFERENCES external_link_item (id) ON DELETE CASCADE;
ALTER TABLE external_link_item ADD CONSTRAINT external_link_item_file_id_fk FOREIGN KEY(file_id) REFERENCES file (id);
ALTER TABLE external_link_item ADD CONSTRAINT external_link_item_external_link_group_id_fk FOREIGN KEY(external_link_group_id) REFERENCES external_link_group (id) ON DELETE CASCADE;
ALTER TABLE external_link_item ADD CONSTRAINT external_link_item_external_link_id_fk FOREIGN KEY(external_link_id) REFERENCES external_link (id) ON DELETE CASCADE;
ALTER TABLE external_link_group ADD CONSTRAINT external_link_group_schedule_source_id_fk FOREIGN KEY(schedule_source_id) REFERENCES monitored_url_source (id) ON DELETE SET NULL;
ALTER TABLE external_link_group ADD CONSTRAINT external_link_group_chat_card_message_id_fk FOREIGN KEY(chat_card_message_id) REFERENCES message (id) ON DELETE SET NULL;
ALTER TABLE external_link_group ADD CONSTRAINT external_link_group_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE SET NULL;
ALTER TABLE external_link_group ADD CONSTRAINT external_link_group_external_link_id_fk FOREIGN KEY(external_link_id) REFERENCES external_link (id) ON DELETE CASCADE;
ALTER TABLE monitored_url_source ADD CONSTRAINT monitored_url_source_successful_run_same_source_fk FOREIGN KEY(id, last_successful_run_id) REFERENCES monitored_url_run (source_id, id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_run ADD CONSTRAINT monitored_url_run_previous_same_source_fk FOREIGN KEY(source_id, previous_successful_run_id) REFERENCES monitored_url_run (source_id, id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_run ADD CONSTRAINT monitored_url_run_source_id_fk FOREIGN KEY(source_id) REFERENCES monitored_url_source (id) ON DELETE RESTRICT;
ALTER TABLE tagged_item ADD FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE tagged_item ADD FOREIGN KEY(confirmed_by_user_id) REFERENCES "user" (id);
ALTER TABLE tagged_item ADD FOREIGN KEY(tag_id) REFERENCES tag (id);
ALTER TABLE tag ADD FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE tag ADD FOREIGN KEY(confirmed_by_user_id) REFERENCES "user" (id);
ALTER TABLE tag ADD FOREIGN KEY(shadows_tag_id) REFERENCES tag (id) ON DELETE SET NULL;
ALTER TABLE tag ADD FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record_assignee_index ADD CONSTRAINT record_assignee_index_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE record_assignee_index ADD CONSTRAINT record_assignee_index_assignee_id_fk FOREIGN KEY(assignee_id) REFERENCES "user" (id);
ALTER TABLE record_assignee_index ADD CONSTRAINT record_assignee_index_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE resource_fact ADD CONSTRAINT resource_fact_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE RESTRICT;
ALTER TABLE resource_fact ADD CONSTRAINT resource_fact_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE resource_fact ADD CONSTRAINT resource_fact_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE assistant_stream_event ADD FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE assistant_stream_event ADD FOREIGN KEY(assistant_state_id) REFERENCES assistant_state (id) ON DELETE CASCADE;
ALTER TABLE assistant_stream_event ADD FOREIGN KEY(assistant_run_id) REFERENCES assistant_run (id) ON DELETE CASCADE;
ALTER TABLE assistant_run ADD CONSTRAINT assistant_run_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE assistant_run ADD CONSTRAINT assistant_run_assistant_state_fk FOREIGN KEY(assistant_state_id) REFERENCES assistant_state (id);
ALTER TABLE assistant_state ADD CONSTRAINT assistant_state_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE generated_image ADD CONSTRAINT generated_image_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE email_inbox ADD CONSTRAINT email_inbox_external_account_id_fk FOREIGN KEY(external_account_id) REFERENCES external_account (id);
ALTER TABLE email_inbox ADD CONSTRAINT email_inbox_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE suggestion_seen ADD CONSTRAINT suggestion_seen_suggestion_id_fk FOREIGN KEY(suggestion_id) REFERENCES suggestion (id) ON DELETE CASCADE;
ALTER TABLE suggestion_seen ADD CONSTRAINT suggestion_seen_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id) ON DELETE CASCADE;
ALTER TABLE calendar_highlight ADD CONSTRAINT calendar_highlight_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE upcoming_summary ADD CONSTRAINT upcoming_summary_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE feedback ADD CONSTRAINT feedback_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id);
ALTER TABLE feedback ADD CONSTRAINT feedback_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE space_member ADD CONSTRAINT space_member_user_id_fk FOREIGN KEY(user_id) REFERENCES "user" (id) ON DELETE CASCADE;
ALTER TABLE space_member ADD CONSTRAINT space_member_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE CASCADE;
ALTER TABLE record_completed_index ADD CONSTRAINT record_completed_index_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record_completed_index ADD CONSTRAINT record_completed_index_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE partner_calendar_event_mirror ADD CONSTRAINT partner_calendar_event_mirror_standing_grant_id_fk FOREIGN KEY(origin_standing_grant_id) REFERENCES standing_grant (id) ON DELETE RESTRICT;
ALTER TABLE partner_calendar_event_mirror ADD CONSTRAINT partner_calendar_event_mirror_recipient_user_id_fk FOREIGN KEY(recipient_user_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE partner_calendar_event_mirror ADD CONSTRAINT partner_calendar_event_mirror_calendar_event_id_fk FOREIGN KEY(calendar_event_id) REFERENCES calendar_event (id) ON DELETE RESTRICT;
ALTER TABLE partner_calendar_event_mirror ADD CONSTRAINT partner_calendar_event_mirror_origin_calendar_event_id_fk FOREIGN KEY(origin_calendar_event_id) REFERENCES calendar_event (id) ON DELETE RESTRICT;
ALTER TABLE assignment ADD CONSTRAINT assignment_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE assignment ADD CONSTRAINT assignment_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE resource_share ADD CONSTRAINT resource_share_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE resource_share ADD CONSTRAINT resource_share_invite_id_fk FOREIGN KEY(invite_id) REFERENCES invite (id) ON DELETE RESTRICT;
ALTER TABLE resource_share ADD CONSTRAINT resource_share_resource_id_fk FOREIGN KEY(resource_id) REFERENCES resource (id) ON DELETE RESTRICT;
ALTER TABLE resource_share ADD CONSTRAINT resource_share_origin_standing_grant_id_fk FOREIGN KEY(origin_standing_grant_id) REFERENCES standing_grant (id) ON DELETE RESTRICT;
ALTER TABLE resource_share ADD CONSTRAINT resource_share_target_id_fk FOREIGN KEY(target_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_subscription ADD CONSTRAINT monitored_url_subscription_execution_user_id_fk FOREIGN KEY(execution_user_id) REFERENCES "user" (id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_subscription ADD CONSTRAINT monitored_url_subscription_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_subscription ADD CONSTRAINT monitored_url_subscription_source_id_fk FOREIGN KEY(source_id) REFERENCES monitored_url_source (id) ON DELETE RESTRICT;
ALTER TABLE monitored_url_subscription ADD CONSTRAINT monitored_url_subscription_resource_same_space_fk FOREIGN KEY(subject_id, space_id) REFERENCES resource (id, space_id) ON DELETE RESTRICT;
ALTER TABLE record_share ADD CONSTRAINT record_share_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE record_share ADD CONSTRAINT record_share_target_id_fk FOREIGN KEY(target_id) REFERENCES "user" (id);
ALTER TABLE record_share ADD CONSTRAINT record_share_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record_share ADD CONSTRAINT record_share_origin_standing_grant_id_fk FOREIGN KEY(origin_standing_grant_id) REFERENCES standing_grant (id);
ALTER TABLE record_share ADD CONSTRAINT record_share_invite_id_fk FOREIGN KEY(invite_id) REFERENCES invite (id);
ALTER TABLE record_share ADD CONSTRAINT record_share_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id);
ALTER TABLE native_sync_authority_fence ADD FOREIGN KEY(user_id) REFERENCES "user" (id) ON DELETE CASCADE;
ALTER TABLE graph_edge ADD CONSTRAINT graph_edge_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE graph_edge ADD CONSTRAINT graph_edge_to_space_id_fk FOREIGN KEY(to_space_id) REFERENCES space (id);
ALTER TABLE graph_edge ADD CONSTRAINT graph_edge_from_space_id_fk FOREIGN KEY(from_space_id) REFERENCES space (id);
ALTER TABLE record_recurrence ADD CONSTRAINT record_recurrence_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record_recurrence ADD CONSTRAINT record_recurrence_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE record_date_index ADD CONSTRAINT record_date_index_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
ALTER TABLE record_date_index ADD CONSTRAINT record_date_index_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id) ON DELETE CASCADE;
ALTER TABLE conversation ADD CONSTRAINT conversation_record_id_fk FOREIGN KEY(record_id) REFERENCES record (id);
ALTER TABLE conversation ADD CONSTRAINT conversation_created_by_user_id_fk FOREIGN KEY(created_by_user_id) REFERENCES "user" (id);
ALTER TABLE conversation ADD CONSTRAINT conversation_event_log_id_fk FOREIGN KEY(event_log_id) REFERENCES event_log (id);
ALTER TABLE conversation ADD CONSTRAINT conversation_space_id_fk FOREIGN KEY(space_id) REFERENCES space (id);
