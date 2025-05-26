DROP TABLE IF EXISTS archives_archive CASCADE;
DROP TABLE IF EXISTS channels_channellog CASCADE;
DROP TABLE IF EXISTS channels_channel CASCADE;
DROP TABLE IF EXISTS flows_flowstart_contacts CASCADE;
DROP TABLE IF EXISTS flows_flowstart_groups CASCADE;
DROP TABLE IF EXISTS flows_flowstart_calls CASCADE;
DROP TABLE IF EXISTS flows_flowstart CASCADE;
DROP TABLE IF EXISTS flows_flowrun CASCADE;
DROP TABLE IF EXISTS flows_flow CASCADE;
DROP TABLE IF EXISTS msgs_broadcast_contacts CASCADE;
DROP TABLE IF EXISTS msgs_broadcast_groups CASCADE;
DROP TABLE IF EXISTS msgs_broadcastmsgcount CASCADE;
DROP TABLE IF EXISTS msgs_broadcast CASCADE;
DROP TABLE IF EXISTS msgs_label CASCADE;
DROP TABLE IF EXISTS msgs_msg_labels CASCADE;
DROP TABLE IF EXISTS msgs_msg CASCADE;
DROP TABLE IF EXISTS msgs_optin CASCADE;
DROP TABLE IF EXISTS ivr_call CASCADE;
DROP TABLE IF EXISTS contacts_contacturn CASCADE;
DROP TABLE IF EXISTS contacts_contactgroup_contacts CASCADE;
DROP TABLE IF EXISTS contacts_contactgroup CASCADE;
DROP TABLE IF EXISTS contacts_contact CASCADE;
DROP TABLE IF EXISTS auth_user CASCADE;
DROP TABLE IF EXISTS orgs_org CASCADE;

CREATE TABLE orgs_org (
    id serial primary key,
    name character varying(255) NOT NULL,
    is_anon boolean NOT NULL,
    is_active boolean NOT NULL,
    created_on timestamp with time zone NOT NULL
);

CREATE TABLE auth_user (
    id serial primary key,
    username character varying(128) NOT NULL
);

CREATE TABLE channels_channel (
    id serial primary key,
    uuid character varying(36) NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    name character varying(255) NOT NULL
);

CREATE TABLE contacts_contact (
    id serial primary key,
    uuid character varying(36) NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    is_active boolean NOT NULL,
    created_by_id integer NOT NULL,
    created_on timestamp with time zone NOT NULL,
    modified_by_id integer NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    name character varying(128),
    language character varying(3),
    fields jsonb
);

CREATE TABLE contacts_contacturn (
    id serial primary key,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    contact_id integer,
    scheme character varying(128) NOT NULL,
    priority integer NOT NULL,
    path character varying(255) NOT NULL,
    channel_id integer,
    display character varying(255),
    identity character varying(255) NOT NULL,
    auth_tokens jsonb
);

CREATE TABLE contacts_contactgroup (
    id serial primary key,
    uuid uuid NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    name character varying(128) NOT NULL
);

CREATE TABLE contacts_contactgroup_contacts (
    id serial primary key,
    contactgroup_id integer NOT NULL,
    contact_id integer NOT NULL
);

CREATE TABLE flows_flow (
    id serial primary key,
    uuid character varying(36) NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    name character varying(128) NOT NULL
);

CREATE TABLE msgs_broadcast (
    id serial primary key,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    translations jsonb NOT NULL,
    created_on timestamp with time zone NOT NULL,
    schedule_id int NULL,
    is_active boolean NOT NULL
);

CREATE TABLE msgs_broadcast_contacts (
    id serial primary key,
    broadcast_id integer NOT NULL REFERENCES msgs_broadcast(id),
    contact_id integer NOT NULL REFERENCES contacts_contact(id)
);

CREATE TABLE msgs_broadcast_groups (
    id serial primary key,
    broadcast_id integer NOT NULL REFERENCES msgs_broadcast(id),
    contactgroup_id integer NOT NULL REFERENCES contacts_contactgroup(id)
);

CREATE TABLE msgs_broadcastmsgcount (
    id serial primary key,
    count integer NOT NULL,
    broadcast_id integer NOT NULL REFERENCES msgs_broadcast(id)
);

CREATE TABLE msgs_optin (
    id serial PRIMARY KEY,
    uuid uuid NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id) ON DELETE CASCADE,
    name character varying(64)
);

CREATE TABLE msgs_msg (
    id bigserial PRIMARY KEY,
    uuid uuid NOT NULL,
    org_id integer NOT NULL REFERENCES orgs_org(id) ON DELETE CASCADE,
    channel_id integer REFERENCES channels_channel(id) ON DELETE CASCADE,
    contact_id integer NOT NULL REFERENCES contacts_contact(id) ON DELETE CASCADE,
    contact_urn_id integer REFERENCES contacts_contacturn(id) ON DELETE CASCADE,
    broadcast_id integer REFERENCES msgs_broadcast(id) ON DELETE CASCADE,
    flow_id integer REFERENCES flows_flow(id) ON DELETE CASCADE,
    --ticket_id integer REFERENCES tickets_ticket(id) ON DELETE CASCADE,
    created_by_id integer REFERENCES auth_user(id) ON DELETE CASCADE,
    text text NOT NULL,
    attachments character varying(255)[] NULL,
    quick_replies character varying(64)[] NULL,
    optin_id integer REFERENCES msgs_optin(id) ON DELETE CASCADE,
    locale character varying(6) NULL,
    created_on timestamp with time zone NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    sent_on timestamp with time zone,
    msg_type character varying(1) NOT NULL,
    direction character varying(1) NOT NULL,
    status character varying(1) NOT NULL,
    visibility character varying(1) NOT NULL,
    msg_count integer NOT NULL,
    high_priority boolean NULL,
    error_count integer NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    failed_reason character varying(1),
    external_id character varying(255),
    log_uuids uuid[]
);

CREATE TABLE msgs_label (
    id serial primary key,
    uuid character varying(36) NULL,
    name character varying(64)
);

CREATE TABLE msgs_msg_labels (
    id serial primary key,
    msg_id integer NOT NULL REFERENCES msgs_msg(id),
    label_id integer NOT NULL REFERENCES msgs_label(id)
);

CREATE TABLE flows_flowstart (
    id serial primary key,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    created_on timestamp with time zone NOT NULL
);

CREATE TABLE flows_flowstart_contacts (
    id serial primary key,
    flowstart_id integer NOT NULL REFERENCES flows_flowstart(id),
    contact_id integer NOT NULL REFERENCES contacts_contact(id)
);

CREATE TABLE flows_flowstart_groups (
    id serial primary key,
    flowstart_id integer NOT NULL REFERENCES flows_flowstart(id),
    contactgroup_id integer NOT NULL REFERENCES contacts_contactgroup(id)
);

CREATE TABLE flows_flowrun (
    id serial primary key,
    uuid uuid NOT NULL UNIQUE,
    org_id integer NOT NULL REFERENCES orgs_org(id),
    responded boolean NOT NULL,
    contact_id integer NOT NULL REFERENCES contacts_contact(id),
    flow_id integer NOT NULL REFERENCES flows_flow(id),
    start_id integer NULL REFERENCES flows_flowstart(id),
    results text NOT NULL,
    path text NOT NULL,
    path_nodes uuid[] NULL,
    path_times timestamp with time zone[] NULL,
    created_on timestamp with time zone NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    exited_on timestamp with time zone NULL,
    status varchar(1) NOT NULL
);

CREATE TABLE archives_archive (
    id serial primary key,
    archive_type varchar(16) NOT NULL, 
    created_on timestamp with time zone NOT NULL, 
    start_date date NOT NULL, 
    period varchar(1) NOT NULL, 
    record_count integer NOT NULL, 
    size bigint NOT NULL, 
    hash text NOT NULL, 
    url varchar(200) NOT NULL, 
    needs_deletion boolean NOT NULL, 
    deleted_on timestamp with time zone NULL,
    build_time integer NOT NULL, 
    org_id integer NOT NULL,
    rollup_id integer NULL
);

INSERT INTO orgs_org(id, name, is_active, is_anon, created_on) VALUES
(1, 'Org 1', TRUE, FALSE, '2017-11-10 21:11:59.890662+00'),
(2, 'Org 2', TRUE, FALSE, '2017-08-10 21:11:59.890662+00'),
(3, 'Org 3', TRUE, TRUE, '2017-08-10 21:11:59.890662+00'),
(4, 'Org 4', FALSE, TRUE, '2017-08-10 21:11:59.890662+00');

INSERT INTO channels_channel(id, uuid, org_id, name) VALUES
(1, '8c1223c3-bd43-466b-81f1-e7266a9f4465', 1, 'Channel 1'),
(2, '60f2ed5b-05f2-4156-9ff0-e44e90da1b85', 2, 'Channel 2'),
(3, 'b79e0054-068f-4928-a5f4-339d10a7ad5a', 3, 'Channel 3');

INSERT INTO archives_archive(id, org_id, archive_type, created_on, start_date, period, record_count, size, hash, url, needs_deletion, build_time) VALUES 
(NEXTVAL('archives_archive_id_seq'), 3, 'message', '2017-08-10 00:00:00.000000+00', '2017-08-10 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0),
(NEXTVAL('archives_archive_id_seq'), 3, 'message', '2017-09-10 00:00:00.000000+00', '2017-09-10 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0),
(NEXTVAL('archives_archive_id_seq'), 3, 'message', '2017-09-02 00:00:00.000000+00', '2017-09-01 00:00:00.000000+00', 'M', 0, 0, '', '', TRUE, 0),
(NEXTVAL('archives_archive_id_seq'), 2, 'message', '2017-10-08 00:00:00.000000+00', '2017-10-08 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0);

INSERT INTO contacts_contact(id, uuid, org_id, is_active, created_by_id, created_on, modified_by_id, modified_on, name, language) VALUES
(1, 'c7a2dd87-a80e-420b-8431-ca48d422e924', 1, TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', NULL, 'eng'),
(3, '7a6606c7-ff41-4203-aa98-454a10d37209', 1, TRUE, -1, '2015-03-26 10:07:14.054521+00', -1, '2015-03-26 10:07:14.054521+00', NULL, NULL),
(4, '29b45297-15ad-4061-a7d4-e0b33d121541', 1, TRUE, -1, '2015-03-26 13:04:58.699648+00', -1, '2015-03-26 13:04:58.699648+00', NULL, NULL),
(5, '51762bba-01a2-4c4e-b5cd-b182d0405cd4', 1, TRUE, -1, '2015-03-27 07:39:28.955051+00', -1, '2015-03-27 07:39:28.955051+00', 'John Doe', NULL),
(6, '3e814add-e614-41f7-8b5d-a07f670a698f', 2, TRUE, -1, '2015-10-30 19:42:27.001837+00', -1, '2015-10-30 19:42:27.001837+00', 'Ajodinabiff Dane', NULL),
(7, '7051dff0-0a27-49d7-af1f-4494239139e6', 3, TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', 'Joanne Stone', NULL),
(8, 'b46f6e18-95b4-4984-9926-dded047f4eb3', 2, TRUE, -1, '2015-03-27 13:39:43.995812+00', -1, '2015-03-27 13:39:43.995812+00', NULL, 'fre'),
(9, '9195c8b7-6138-4d84-ac56-5192cc3d8ceb', 2, TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', NULL, NULL),
(10, '2b8bd28d-43e0-4c34-a4bb-0f10b11fdb8a', 2, TRUE, -1, '2016-08-22 14:20:05.690311+00', -1, '2016-08-22 14:20:05.690311+00', 'John Arbies', NULL);

INSERT INTO contacts_contacturn(id, org_id, contact_id, scheme, priority, path, display, identity) VALUES
(1, 1, 1, 'tel', 50, '+12067791111', NULL, 'tel:+12067791111'),
(2, 1, 1, 'tel', 50, '+12067792222', NULL, 'tel:+12067792222'),
(4, 1, 3, 'tel', 50, '+12067794444', NULL, 'tel:+12067794444'),
(5, 1, 4, 'tel', 50, '+12067795555', NULL, 'tel:+12067795555'),
(6, 1, 5, 'tel', 50, '+12060000556', NULL, 'tel:+12067796666'),
(7, 2, 6, 'tel', 50, '+12060005577', NULL, 'tel:+12067797777'),
(8, 3, 7, 'tel', 50, '+12067798888', NULL, 'tel:+12067798888'),
(9, 2, 8, 'viber', 90, 'viberpath==', NULL, 'viber:viberpath=='),
(10, 2, 9, 'facebook', 90, 1000001, 'funguy', 'facebook:1000001'),
(11, 2, 10, 'twitterid', 90, 1000001, 'fungal', 'twitterid:1000001');

INSERT INTO contacts_contactgroup(id, uuid, org_id, name) VALUES
(1, '4ea0f313-2f62-4e57-bdf0-232b5191dd57', 2, 'Group 1'),
(2, '4c016340-468d-4675-a974-15cb7a45a5ab', 2, 'Group 2'),
(3, 'e61b5bf7-8ddf-4e05-b0a8-4c46a6b68cff', 2, 'Group 3'),
(4, '529bac39-550a-4d6f-817c-1833f3449007', 2, 'Group 4');

INSERT INTO contacts_contactgroup_contacts(id, contact_id, contactgroup_id) VALUES
(1, 1, 1),
(3, 1, 4),
(4, 3, 4);

INSERT INTO flows_flow(id, uuid, org_id, name) VALUES
(1, '6639286a-9120-45d4-aa39-03ae3942a4a6', 2, 'Flow 1'),
(2, '629db399-a5fb-4fa0-88e6-f479957b63d2', 2, 'Flow 2'),
(3, '3914b88e-625b-4603-bd9f-9319dc331c6b', 2, 'Flow 3'),
(4, 'cfa2371d-2f06-481d-84b2-d974f3803bb0', 2, 'Flow 4');

INSERT INTO msgs_broadcast(id, org_id, translations, created_on, schedule_id, is_active) VALUES
(1, 2, '{"text": {"eng": "hello", "fre": "bonjour"}}', '2017-08-12 22:11:59.890662+02:00', 1, TRUE),
(2, 2, '{"text": {"und": "hola"}}', '2017-08-12 22:11:59.890662+02:00', NULL, TRUE),
(3, 2, '{"text": {"und": "not purged"}}', '2017-08-12 19:11:59.890662+02:00', NULL, TRUE),
(4, 2, '{"text": {"und": "new"}}', '2019-08-12 19:11:59.890662+02:00', NULL, TRUE);

INSERT INTO msgs_msg(id, uuid, org_id, broadcast_id, text, created_on, sent_on, modified_on, direction, status, visibility, msg_type, attachments, channel_id, contact_id, contact_urn_id, flow_id, msg_count, error_count, next_attempt) VALUES
(1, '2f969340-704a-4aa2-a1bd-2f832a21d257', 2, NULL, 'message 1', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'I', 'H', 'V', 'T', NULL, 2, 6, 7, NULL, 1, 0, '2017-08-12 21:11:59.890662+00'),
(2, 'abe87ac1-015c-4803-be29-1e89509fe682', 2, NULL, 'message 2', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'I', 'H', 'D', 'T', NULL, 2, 6, 7, NULL, 1, 0, '2017-08-12 21:11:59.890662+00'),
(3, 'a7e83a22-a6ff-4e18-82d0-19545640ccba', 2, NULL, 'message 3', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'O', 'H', 'V', 'T', '{"image/png:https://foo.bar/image1.png", "image/png:https://foo.bar/image2.png"}', NULL, 6, 7, NULL, 1, 0, '2017-08-12 21:11:59.890662+00'),
(4, '1cad36af-5581-4c8a-81cd-83708398f61e', 2, NULL, 'message 4', '2017-08-13 21:11:59.890662+00', '2017-08-13 21:11:59.890662+00', '2017-08-13 21:11:59.890662+00', 'I', 'H', 'V', 'T', NULL, 2, 6, 7, NULL, 1, 0, '2017-08-13 21:11:59.890662+00'),
(5, 'f557972e-2eb5-42fa-9b87-902116d18787', 3, NULL, 'message 5', '2017-08-11 21:11:59.890662+02:00', '2017-08-11 21:11:59.890662+02:00', '2017-08-11 21:11:59.890662+02:00', 'I', 'H', 'V', 'T', NULL, 3, 7, 8, NULL, 1, 0, '2017-08-11 21:11:59.890662+02:00'),
(6, '579d148c-0ab1-4afb-832f-afb1fe0e19b7', 2, 2, 'message 6', '2017-10-08 21:11:59.890662+00', '2017-10-08 21:11:59.890662+00', '2017-10-08 21:11:59.890662+00', 'I', 'H', 'V', 'T', NULL, 2, 6, 7, NULL, 1, 0, '2017-10-08 21:11:59.890662+00'),
(7, '7aeca469-2593-444e-afe4-4702317534c9', 2, NULL, 'message 7', '2018-01-02 21:11:59.890662+00', '2018-01-02 21:11:59.890662+00', '2018-01-02 21:11:59.890662+00', 'I', 'H', 'X', 'T', NULL, 2, 6, 7, 2, 1, 0, '2018-01-02 21:11:59.890662+00'),
(9, 'e14ab466-0d3b-436d-a0f7-5851fd7d9b7d', 2, NULL, 'message 9', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'O', 'S', 'V', 'T', NULL, NULL, 6, NULL, 3, 1, 0, '2017-08-12 21:11:59.890662+00');

INSERT INTO msgs_label(id, uuid, name) VALUES
(1, '1d9e3188-b74b-4ae0-a166-0de31aedb34a', 'Label 1'),
(2, 'c5a69101-8dc3-444f-8b0b-5ab816e46eec', 'Label 2'),
(3, '9e13d3b6-1ffa-406e-b66b-5cebe6738488', 'Label 3');

INSERT INTO msgs_msg_labels(id, msg_id, label_id) VALUES
(1, 1, 1),
(2, 1, 2),
(3, 2, 2),
(4, 3, 2);

INSERT INTO auth_user(id, username) VALUES 
(1, 'greg@gmail.com');

INSERT INTO flows_flowstart(id, org_id, created_on) VALUES 
(1, 2, NOW());

INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES 
(1, 6);

INSERT INTO flows_flowstart_groups(flowstart_id, contactgroup_id) VALUES 
(1, 1);

INSERT INTO flows_flowrun(id, uuid, org_id, responded, contact_id, flow_id, results, path, path_nodes, path_times, created_on, modified_on, exited_on, status, start_id) VALUES
(1, '4ced1260-9cfe-4b7f-81dd-b637108f15b9', 2, TRUE, 6, 1, '{}', '[]', NULL, NULL, '2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00', 'C', 1),
(2, '7d68469c-0494-498a-bdf3-bac68321fd6d', 2, TRUE, 6, 1, 
'{"agree": {"category": "Strongly agree", "node_uuid": "a0434c54-3e26-4eb0-bafc-46cdeaf435ac", "name": "Do you agree?", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}}',
'[{"uuid": "c3d0b417-db75-417c-8050-33776ec8f620", "node_uuid": "10896d63-8df7-4022-88dd-a9d93edf355b", "arrived_on": "2017-08-12T15:07:24.049815+02:00", "exit_uuid": "2f890507-2ad2-4bd1-92fc-0ca031155fca"}]', 
NULL, NULL, '2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00', 'C', NULL),
(3, 'de782b35-a398-46ed-8550-34c66053841b', 3, TRUE, 7, 2, 
'{"agree": {"category": "Strongly agree", "node_uuid": "084c8cf1-715d-4d0a-b38d-a616ed74e638", "name": "Agree", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}, "confirm_agree": {"category": "Confirmed Strongly agree", "node_uuid": "a0434c54-3e26-4eb0-bafc-46cdeaf435ab", "name": "Do you agree?", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}}',
'[{"uuid": "600ac5b4-4895-4161-ad97-6e2f1bb48bcb", "node_uuid": "accbc6e2-b0df-46cd-9a76-bff0fdf4d753", "arrived_on": "2017-08-12T15:07:24.049815+02:00", "exit_uuid": "8249e2dc-c893-4200-b6d2-398d07a459bc"}]', 
NULL, NULL, '2017-08-10 21:11:59.890662+02:00','2017-08-10 21:11:59.890662+02:00','2017-08-10 21:11:59.890662+02:00', 'C', NULL),
(4, '329a5d24-64fc-479c-8d24-9674c9b46530', 3, TRUE, 7, 2, 
'{"agree": {"category": "Disagree", "node_uuid": "084c8cf1-715d-4d0a-b38d-a616ed74e638", "name": "Agree", "value": "B", "created_on": "2017-10-10T12:25:21.714339+00:00", "input": "B"}}',
'[{"uuid": "babf4fc8-e12c-4bb9-a9dd-61178a118b5a", "node_uuid": "accbc6e2-b0df-46cd-9a76-bff0fdf4d753", "arrived_on": "2017-10-12T15:07:24.049815+02:00", "exit_uuid": "8249e2dc-c893-4200-b6d2-398d07a459bc"}]', 
NULL, NULL, '2017-10-10 21:11:59.890662+02:00','2017-10-10 21:11:59.890662+02:00','2017-10-10 21:11:59.890662+02:00', 'C', NULL),
(5, 'abed67d2-06b8-4749-8bb9-ecda037b673b', 3, TRUE, 7, 2, '{}', '[]', NULL, NULL, '2017-10-10 21:11:59.890663+02:00','2017-10-10 21:11:59.890662+02:00','2017-10-10 21:11:59.890662+02:00', 'C', NULL),
(6, '6262eefe-a6e9-4201-9b76-a7f25e3b7f29', 3, TRUE, 7, 2, '{}', '[]', NULL, NULL, '2017-12-12 21:11:59.890662+02:00','2017-12-12 21:11:59.890662+02:00','2017-12-12 21:11:59.890662+02:00', 'C', NULL),
(7, '6c0d7db9-076b-4edc-ab4b-38576ae394fc', 2, TRUE, 7, 2, '{}', '[]', NULL, NULL, '2017-08-13 13:11:59.890662+02:00','2017-08-14 16:11:59.890662+02:00', NULL, 'W', NULL),
(8, '0c54f7b9-875b-4385-ae85-fb9e84f4b3d6', 2, TRUE, 6, 1,
'{"agree": {"category": "Strongly agree", "node_uuid": "a0434c54-3e26-4eb0-bafc-46cdeaf435ac", "name": "Do you agree?", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}}',
'[]',
'{"d1a55403-83a3-42f1-b24c-6446bb18e6a6","ce1bdc68-5c16-452e-a0ce-52440fc7bb9a","1640b40d-63ed-43b0-a443-097ce8bb8710"}', '{"2017-08-12T15:07:25.049815+02:00","2017-08-12T15:07:26.049815+02:00","2017-08-12T15:07:27.049815+02:00"}', '2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00', 'C', NULL);

-- update run #5 to have a path longer than 500 steps
UPDATE flows_flowrun SET path = s.path FROM (
    SELECT json_agg(CONCAT('{"uuid": "babf4fc8-e12c-4bb9-a9dd-61178a118b5a", "node_uuid": "accbc6e2-b0df-46cd-9a76-bff0fdf4d753", "arrived_on": "2017-10-12T15:07:24.', LPAD(gs.val::text, 6, '0'), '+02:00", "exit_uuid": "8249e2dc-c893-4200-b6d2-398d07a459bc"}')::jsonb) as path FROM generate_series(1, 1000) as gs(val)
) AS s WHERE id = 5;
