CREATE EXTENSION IF NOT EXISTS HSTORE;

DROP TABLE IF EXISTS orgs_org CASCADE;
CREATE TABLE orgs_org (
    id serial primary key,
    name character varying(255) NOT NULL,
    is_anon boolean NOT NULL,
    is_active boolean NOT NULL,
    created_on timestamp with time zone NOT NULL
);

DROP TABLE IF EXISTS channels_channel CASCADE;
CREATE TABLE channels_channel (
    id serial primary key,
    name character varying(255) NOT NULL,
    uuid character varying(36) NOT NULL,
    org_id integer references orgs_org(id) on delete cascade
);

DROP TABLE IF EXISTS contacts_contact CASCADE;
CREATE TABLE contacts_contact (
    id serial primary key,
    is_active boolean NOT NULL,
    created_by_id integer NOT NULL,
    created_on timestamp with time zone NOT NULL,
    modified_by_id integer NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    org_id integer NOT NULL references orgs_org(id) on delete cascade,
    is_blocked boolean NOT NULL,
    name character varying(128),
    language character varying(3),
    uuid character varying(36) NOT NULL,
    is_stopped boolean NOT NULL,
    fields jsonb
);

DROP TABLE IF EXISTS contacts_contacturn CASCADE;
CREATE TABLE contacts_contacturn (
    id serial primary key,
    contact_id integer,
    scheme character varying(128) NOT NULL,
    org_id integer NOT NULL,
    priority integer NOT NULL,
    path character varying(255) NOT NULL,
    channel_id integer,
    auth text,
    display character varying(255),
    identity character varying(255) NOT NULL
);

DROP TABLE IF EXISTS contacts_contactgroup CASCADE;
CREATE TABLE contacts_contactgroup (
    id serial primary key,
    uuid character varying(36) NOT NULL,
    name character varying(128) NOT NULL
);

DROP TABLE IF EXISTS contacts_contactgroup_contacts CASCADE;
CREATE TABLE contacts_contactgroup_contacts (
    id serial primary key,
    contactgroup_id integer NOT NULL,
    contact_id integer NOT NULL
);

DROP TABLE IF EXISTS channels_channellog CASCADE;
DROP TABLE IF EXISTS msgs_msg_labels CASCADE;
DROP TABLE IF EXISTS msgs_msg CASCADE;
CREATE TABLE msgs_msg (
    id serial primary key,
    broadcast_id integer NULL,
    uuid character varying(36) NULL,
    text text NOT NULL,
    high_priority boolean NULL,
    created_on timestamp with time zone NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    sent_on timestamp with time zone,
    queued_on timestamp with time zone,
    direction character varying(1) NOT NULL,
    status character varying(1) NOT NULL,
    visibility character varying(1) NOT NULL,
    msg_type character varying(1),
    msg_count integer NOT NULL,
    error_count integer NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    external_id character varying(255),
    attachments character varying(255)[],
    channel_id integer references channels_channel(id) on delete cascade,
    contact_id integer NOT NULL references contacts_contact(id) on delete cascade,
    contact_urn_id integer NULL references contacts_contacturn(id) on delete cascade,
    org_id integer NOT NULL references orgs_org(id) on delete cascade,
    metadata text,
    topup_id integer,
    delete_reason char(1) NULL,
    response_to_id integer NULL references msgs_msg(id)
);

DROP TABLE IF EXISTS msgs_broadcast_recipients;
DROP TABLE IF EXISTS msgs_broadcast;
CREATE TABLE msgs_broadcast (
    id serial primary key,
    "text" hstore NOT NULL,
    purged BOOLEAN NOT NULL,
    created_on timestamp with time zone NOT NULL,
    schedule_id int NULL,
    org_id integer NOT NULL references orgs_org(id) on delete cascade
);

DROP TABLE IF EXISTS msgs_broadcast_contacts;
CREATE TABLE msgs_broadcast_contacts (
    id serial primary key,
    broadcast_id integer NOT NULL,
    contact_id integer NOT NULL
);

DROP TABLE IF EXISTS msgs_broadcast_groups;
CREATE TABLE msgs_broadcast_groups (
    id serial primary key,
    broadcast_id integer NOT NULL,
    group_id integer NOT NULL
);

DROP TABLE IF EXISTS msgs_broadcast_urns;
CREATE TABLE msgs_broadcast_urns (
    id serial primary key,
    broadcast_id integer NOT NULL,
    contacturn_id integer NOT NULL
);

DROP TABLE IF EXISTS msgs_broadcastmsgcount;
CREATE TABLE msgs_broadcastmsgcount (
    id serial primary key,
    "count" integer NOT NULL,
    broadcast_id integer NOT NULL
);

DROP TABLE IF EXISTS msgs_label CASCADE;
CREATE TABLE msgs_label (
    id serial primary key,
    uuid character varying(36) NULL,
    name character varying(64)
);

CREATE TABLE msgs_msg_labels (
    id serial primary key,
    msg_id integer NOT NULL references msgs_msg(id),
    label_id integer NOT NULL
);

DROP TABLE IF EXISTS flows_flow CASCADE;
CREATE TABLE flows_flow (
    id serial primary key,
    uuid character varying(36) NOT NULL,
    name character varying(128) NOT NULL
);

DROP TABLE IF EXISTS auth_user CASCADE;
CREATE TABLE auth_user (
    id serial primary key,
    username character varying(128) NOT NULL
);

DROP TABLE IF EXISTS api_webhookresult CASCADE;
DROP TABLE IF EXISTS api_webhookevent CASCADE;
DROP TABLE IF EXISTS flows_flowpathrecentrun CASCADE;
DROP TABLE IF EXISTS flows_actionlog CASCADE;
DROP TABLE IF EXISTS flows_flowrun CASCADE;
CREATE TABLE flows_flowrun (
    id serial primary key,
    is_active boolean NOT NULL DEFAULT FALSE,
    uuid character varying(36) NOT NULL,
    responded boolean NOT NULL,
    contact_id integer NOT NULL references contacts_contact(id),
    flow_id integer NOT NULL references flows_flow(id),
    org_id integer NOT NULL references orgs_org(id),
    results text NOT NULL,
    path text NOT NULL,
    events jsonb NOT NULL,
    parent_id integer NULL references flows_flowrun(id),
    created_on timestamp with time zone NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    exited_on timestamp with time zone NULL,
    submitted_by_id integer NULL references auth_user(id),
    exit_type varchar(1) NULL,
    delete_reason char(1) NULL
);

DROP TABLE IF EXISTS archives_archive CASCADE;
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

CREATE TABLE channels_channellog (
    id serial primary key,
    msg_id integer NOT NULL references msgs_msg(id)
);

CREATE TABLE flows_flowpathrecentrun (
    id serial primary key,
    run_id integer NOT NULL references flows_flowrun(id) DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO orgs_org(id, name, is_active, is_anon, created_on) VALUES
(1, 'Org 1', TRUE, FALSE, '2017-11-10 21:11:59.890662+00'),
(2, 'Org 2', TRUE, FALSE, '2017-08-10 21:11:59.890662+00'),
(3, 'Org 3', TRUE, TRUE, '2017-08-10 21:11:59.890662+00'),
(4, 'Org 4', FALSE, TRUE, '2017-08-10 21:11:59.890662+00');

INSERT INTO channels_channel(id, uuid, name, org_id) VALUES
(1, '8c1223c3-bd43-466b-81f1-e7266a9f4465', 'Channel 1', 1),
(2, '60f2ed5b-05f2-4156-9ff0-e44e90da1b85', 'Channel 2', 2),
(3, 'b79e0054-068f-4928-a5f4-339d10a7ad5a', 'Channel 3', 3);

INSERT INTO archives_archive(id, archive_type, created_on, start_date, period, record_count, size, hash, url, needs_deletion, build_time, org_id) VALUES 
(NEXTVAL('archives_archive_id_seq'), 'message', '2017-08-10 00:00:00.000000+00', '2017-08-10 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0, 3),
(NEXTVAL('archives_archive_id_seq'), 'message', '2017-09-10 00:00:00.000000+00', '2017-09-10 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0, 3),
(NEXTVAL('archives_archive_id_seq'), 'message', '2017-09-02 00:00:00.000000+00', '2017-09-01 00:00:00.000000+00', 'M', 0, 0, '', '', TRUE, 0, 3),
(NEXTVAL('archives_archive_id_seq'), 'message', '2017-10-08 00:00:00.000000+00', '2017-10-08 00:00:00.000000+00', 'D', 0, 0, '', '', TRUE, 0, 2);

INSERT INTO contacts_contact(id, is_active, created_by_id, created_on, modified_by_id, modified_on, org_id, is_blocked, name, language, uuid, is_stopped) VALUES
(1,  TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', 1, FALSE, NULL, 'eng', 'c7a2dd87-a80e-420b-8431-ca48d422e924', FALSE),
(3,  TRUE, -1, '2015-03-26 10:07:14.054521+00', -1, '2015-03-26 10:07:14.054521+00', 1, FALSE, NULL, NULL, '7a6606c7-ff41-4203-aa98-454a10d37209', TRUE),
(4,  TRUE, -1, '2015-03-26 13:04:58.699648+00', -1, '2015-03-26 13:04:58.699648+00', 1, TRUE, NULL, NULL, '29b45297-15ad-4061-a7d4-e0b33d121541', FALSE),
(5,  TRUE, -1, '2015-03-27 07:39:28.955051+00', -1, '2015-03-27 07:39:28.955051+00', 1, FALSE, 'John Doe', NULL, '51762bba-01a2-4c4e-b5cd-b182d0405cd4', FALSE),
(6,  TRUE, -1, '2015-10-30 19:42:27.001837+00', -1, '2015-10-30 19:42:27.001837+00', 2, FALSE, 'Ajodinabiff Dane', NULL, '3e814add-e614-41f7-8b5d-a07f670a698f', FALSE),
(7,  TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', 3, FALSE, 'Joanne Stone', NULL, '7051dff0-0a27-49d7-af1f-4494239139e6', FALSE),
(8,  TRUE, -1, '2015-03-27 13:39:43.995812+00', -1, '2015-03-27 13:39:43.995812+00', 2, FALSE, NULL, 'fre', 'b46f6e18-95b4-4984-9926-dded047f4eb3', FALSE),
(9,  TRUE, -1, '2017-11-10 21:11:59.890662+00', -1, '2017-11-10 21:11:59.890662+00', 2, FALSE, NULL, NULL, '9195c8b7-6138-4d84-ac56-5192cc3d8ceb', FALSE),
(10, TRUE, -1, '2016-08-22 14:20:05.690311+00', -1, '2016-08-22 14:20:05.690311+00', 2, FALSE, 'John Arbies', NULL, '2b8bd28d-43e0-4c34-a4bb-0f10b11fdb8a', FALSE);

INSERT INTO contacts_contacturn(id, contact_id, scheme, org_id, priority, path, display, identity) VALUES
(1, 1, 'tel', 1, 50, '+12067791111', NULL, 'tel:+12067791111'),
(2, 1, 'tel', 1, 50, '+12067792222', NULL, 'tel:+12067792222'),
(4, 3, 'tel', 1, 50, '+12067794444', NULL, 'tel:+12067794444'),
(5, 4, 'tel', 1, 50, '+12067795555', NULL, 'tel:+12067795555'),
(6, 5, 'tel', 1, 50, '+12060000556', NULL, 'tel:+12067796666'),
(7, 6, 'tel', 2, 50, '+12060005577', NULL, 'tel:+12067797777'),
(8, 7, 'tel', 3, 50, '+12067798888', NULL, 'tel:+12067798888'),
(9, 8, 'viber', 2, 90, 'viberpath==', NULL, 'viber:viberpath=='),
(10, 9, 'facebook', 2, 90, 1000001, 'funguy', 'facebook:1000001'),
(11, 10, 'twitterid', 2, 90, 1000001, 'fungal', 'twitterid:1000001');

INSERT INTO contacts_contactgroup(id, uuid, name) VALUES
(1, '4ea0f313-2f62-4e57-bdf0-232b5191dd57', 'Group 1'),
(2, '4c016340-468d-4675-a974-15cb7a45a5ab', 'Group 2'),
(3, 'e61b5bf7-8ddf-4e05-b0a8-4c46a6b68cff', 'Group 3'),
(4, '529bac39-550a-4d6f-817c-1833f3449007', 'Group 4');

INSERT INTO contacts_contactgroup_contacts(id, contact_id, contactgroup_id) VALUES
(1, 1, 1),
(3, 1, 4),
(4, 3, 4);

INSERT INTO msgs_broadcast(id, text, created_on, purged, org_id, schedule_id) VALUES
(1, 'eng=>"hello",fre=>"bonjour"'::hstore, '2017-08-12 22:11:59.890662+02:00', TRUE, 2, 1),
(2, 'base=>"hola"'::hstore, '2017-08-12 22:11:59.890662+02:00', TRUE, 2, NULL),
(3, 'base=>"not purged"'::hstore, '2017-08-12 19:11:59.890662+02:00', FALSE, 2, NULL),
(4, 'base=>"new"'::hstore, '2019-08-12 19:11:59.890662+02:00', FALSE, 2, NULL);

INSERT INTO msgs_msg(id, broadcast_id, uuid, text, created_on, sent_on, modified_on, direction, status, visibility, msg_type, attachments, channel_id, contact_id, contact_urn_id, org_id, msg_count, error_count, next_attempt, response_to_id) VALUES
(1, NULL, '2f969340-704a-4aa2-a1bd-2f832a21d257', 'message 1', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'I', 'H', 'V', 'I', NULL, 2, 6, 7, 2, 1, 0, '2017-08-12 21:11:59.890662+00', NULL),
(2, NULL, 'abe87ac1-015c-4803-be29-1e89509fe682', 'message 2', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'I', 'H', 'D', 'I', NULL, 2, 6, 7, 2, 1, 0, '2017-08-12 21:11:59.890662+00', NULL),
(3, NULL, 'a7e83a22-a6ff-4e18-82d0-19545640ccba', 'message 3', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'O', 'H', 'V', 'I', '{"image/png:https://foo.bar/image1.png", "image/png:https://foo.bar/image2.png"}', NULL, 6, 7, 2, 1, 0, '2017-08-12 21:11:59.890662+00', NULL),
(4, NULL, '1cad36af-5581-4c8a-81cd-83708398f61e', 'message 4', '2017-08-13 21:11:59.890662+00', '2017-08-13 21:11:59.890662+00', '2017-08-13 21:11:59.890662+00', 'I', 'H', 'V', 'I', NULL, 2, 6, 7, 2, 1, 0, '2017-08-13 21:11:59.890662+00', NULL),
(5, NULL, 'f557972e-2eb5-42fa-9b87-902116d18787', 'message 5', '2017-08-11 21:11:59.890662+02:00', '2017-08-11 21:11:59.890662+02:00', '2017-08-11 21:11:59.890662+02:00', 'I', 'H', 'V', 'I', NULL, 3, 7, 8, 3, 1, 0, '2017-08-11 21:11:59.890662+02:00', NULL),
(6, 2, '579d148c-0ab1-4afb-832f-afb1fe0e19b7', 'message 6', '2017-10-08 21:11:59.890662+00', '2017-10-08 21:11:59.890662+00', '2017-10-08 21:11:59.890662+00', 'I', 'H', 'V', 'I', NULL, 2, 6, 7, 2, 1, 0, '2017-10-08 21:11:59.890662+00', NULL),
(7, NULL, '7aeca469-2593-444e-afe4-4702317534c9', 'message 7', '2018-01-02 21:11:59.890662+00', '2018-01-02 21:11:59.890662+00', '2018-01-02 21:11:59.890662+00', 'I', 'H', 'V', 'I', NULL, 2, 6, 7, 2, 1, 0, '2018-01-02 21:11:59.890662+00', 2),
(9, NULL, 'e14ab466-0d3b-436d-a0f7-5851fd7d9b7d', 'message 9', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', '2017-08-12 21:11:59.890662+00', 'O', 'S', 'V', 'F', NULL, NULL, 6, NULL, 2, 1, 0, '2017-08-12 21:11:59.890662+00', NULL);

INSERT INTO msgs_label(id, uuid, name) VALUES
(1, '1d9e3188-b74b-4ae0-a166-0de31aedb34a', 'Label 1'),
(2, 'c5a69101-8dc3-444f-8b0b-5ab816e46eec', 'Label 2'),
(3, '9e13d3b6-1ffa-406e-b66b-5cebe6738488', 'Label 3');

INSERT INTO msgs_msg_labels(id, msg_id, label_id) VALUES
(1, 1, 1),
(2, 1, 2),
(3, 2, 2),
(4, 3, 2);

INSERT INTO channels_channellog(id, msg_id) VALUES 
(1, 1),
(2, 2),
(3, 3),
(4, 4),
(5, 5),
(6, 6);

INSERT INTO flows_flow(id, uuid, name) VALUES
(1, '6639286a-9120-45d4-aa39-03ae3942a4a6', 'Flow 1'),
(2, '629db399-a5fb-4fa0-88e6-f479957b63d2', 'Flow 2'),
(3, '3914b88e-625b-4603-bd9f-9319dc331c6b', 'Flow 3'),
(4, 'cfa2371d-2f06-481d-84b2-d974f3803bb0', 'Flow 4');

INSERT INTO auth_user(id, username) VALUES 
(1, 'greg@gmail.com');

INSERT INTO flows_flowrun(id, uuid, responded, contact_id, flow_id, org_id, results, path, events, created_on, modified_on, exited_on, exit_type, parent_id, submitted_by_id) VALUES
(1, '4ced1260-9cfe-4b7f-81dd-b637108f15b9', TRUE, 6, 1, 2, '{}', '[]', '[]', '2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00', 'C', NULL, NULL),
(2, '7d68469c-0494-498a-bdf3-bac68321fd6d', TRUE, 6, 1, 2, 
'{"agree": {"category": "Strongly agree", "node_uuid": "a0434c54-3e26-4eb0-bafc-46cdeaf435ac", "name": "Do you agree?", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}}',
'[{"uuid": "c3d0b417-db75-417c-8050-33776ec8f620", "node_uuid": "10896d63-8df7-4022-88dd-a9d93edf355b", "arrived_on": "2017-08-12T15:07:24.049815+02:00", "exit_uuid": "2f890507-2ad2-4bd1-92fc-0ca031155fca"}]', 
'[{"msg": {"urn": "tel:+12076661212", "text": "hola", "uuid": "cf05c58f-31fb-4ce8-9e65-4ecc9fd47cbe", "channel": {"name": "1223", "uuid": "bbfe2e9c-cf69-4d0a-b42e-00ac3dc0b0b8"}}, "type": "msg_created", "step_uuid": "659cdae5-1f29-4a58-9437-10421f724268", "created_on": "2018-01-22T15:06:47.357682+00:00"}]',
'2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00','2017-08-12 21:11:59.890662+02:00', 'C', NULL, NULL),
(3, 'de782b35-a398-46ed-8550-34c66053841b', TRUE, 7, 2, 3, 
'{"agree": {"category": "Strongly agree", "node_uuid": "084c8cf1-715d-4d0a-b38d-a616ed74e638", "name": "Agree", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}, "confirm_agree": {"category": "Confirmed Strongly agree", "node_uuid": "a0434c54-3e26-4eb0-bafc-46cdeaf435ab", "name": "Do you agree?", "value": "A", "created_on": "2017-05-03T12:25:21.714339+00:00", "input": "A"}}',
'[{"uuid": "600ac5b4-4895-4161-ad97-6e2f1bb48bcb", "node_uuid": "accbc6e2-b0df-46cd-9a76-bff0fdf4d753", "arrived_on": "2017-08-12T15:07:24.049815+02:00", "exit_uuid": "8249e2dc-c893-4200-b6d2-398d07a459bc"}]', 
'[{"msg": {"urn": "tel:+12076661212", "text": "hola", "uuid": "9ea50923-0888-4596-9a9d-4890994934a9", "channel": {"name": "1223", "uuid": "d6597e08-8285-428c-8e7e-97c68adfa073"}}, "type": "msg_created", "step_uuid": "ae067248-df92-41c8-bb29-92506e984259", "created_on": "2018-01-22T15:06:47.357682+00:00"}]',
'2017-08-10 21:11:59.890662+02:00','2017-08-10 21:11:59.890662+02:00','2017-08-10 21:11:59.890662+02:00', 'C', NULL, 1),
(4, 'de782b35-a398-46ed-8550-34c66053841b', TRUE, 7, 2, 3, 
'{"agree": {"category": "Disagree", "node_uuid": "084c8cf1-715d-4d0a-b38d-a616ed74e638", "name": "Agree", "value": "B", "created_on": "2017-10-10T12:25:21.714339+00:00", "input": "B"}}',
'[{"uuid": "babf4fc8-e12c-4bb9-a9dd-61178a118b5a", "node_uuid": "accbc6e2-b0df-46cd-9a76-bff0fdf4d753", "arrived_on": "2017-10-12T15:07:24.049815+02:00", "exit_uuid": "8249e2dc-c893-4200-b6d2-398d07a459bc"}]', 
'[{"msg": {"urn": "tel:+12076661212", "text": "hi hi", "uuid": "543d2c4b-ff0b-4b87-a9a4-b2d6745cf470", "channel": {"name": "1223", "uuid": "d6597e08-8285-428c-8e7e-97c68adfa073"}}, "type": "msg_created", "step_uuid": "3a5014dd-7b14-4b7a-be52-0419c09340a6", "created_on": "2018-10-12T15:06:47.357682+00:00"}]',
'2017-10-10 21:11:59.890662+02:00','2017-10-10 21:11:59.890662+02:00','2017-10-10 21:11:59.890662+02:00', 'C', NULL, NULL),
(6, '6262eefe-a6e9-4201-9b76-a7f25e3b7f29', TRUE, 7, 2, 3, '{}', '[]', '[]', 
'2017-12-12 21:11:59.890662+02:00','2017-12-12 21:11:59.890662+02:00','2017-12-12 21:11:59.890662+02:00', 'C', 4, NULL);

INSERT INTO flows_flowpathrecentrun(id, run_id) VALUES 
(1, 3);