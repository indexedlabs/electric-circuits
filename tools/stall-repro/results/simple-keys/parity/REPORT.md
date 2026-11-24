# Variant S against today's gateway predicates

(a) is today's gateway predicate with no client scope; (b) is the union of the account's S feeds. Counts are (account, row) pairs over 20 accounts.

## Base: the seeded database (with shares) plus the first calendar import

Snapshot `1303:1303:` at start and `1303:1303:` at end, PostgreSQL 18.7 (Debian 18.7-1.pgdg13+5), taken 2026-11-24 16:08:35.847996+00:00.

| Table | Admitted (a) | Admitted (b) | (a) only | (b) only | Why (a) only | Why (b) only | Examples (a) only | Examples (b) only |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| `record` | 22,786 | 22,786 | 0 | 0 | — | — | — | — |
| `record_date_index` | 21,686 | 21,686 | 0 | 0 | — | — | — | — |
| `record_recurrence` | 7,247 | 7,247 | 0 | 0 | — | — | — | — |
| `record_assignee_index` | 26 | 26 | 0 | 0 | — | — | — | — |
| `record_completed_index` | 20 | 20 | 0 | 0 | — | — | — | — |
| `assignment` | 7,847 | 7,847 | 0 | 0 | — | — | — | — |
| `calendar_event` | 5,400 | 5,400 | 0 | 0 | — | — | — | — |
| `graph_edge` | 6,707 | 6,151 | 556 | 0 | edge on a directly shared Record: S as written gives graph_edge no share list: 556 | — | `0060f6ae-d6da-5906-be58-aec73c1dd003`<br>`02b8b5de-1cab-58a1-96db-d0a0401dc4d3`<br>`03a3e6af-1504-56d0-9d08-b5391ea898e7`<br>`04082e08-6558-5b8d-adbe-98ed4925b28f`<br>`04446901-b23b-5da5-b876-793268feecfe` | — |

`graph_edge` under the readings S leaves open: graph_edge with the direct-share Record list: (a) only 0, (b) only 0; graph_edge with generic and relates_to tested against member Spaces only: (a) only 556, (b) only 0; both: (a) only 0, (b) only 0.

Drift check (no accounts; rows against their parents): nothing found.

## Base plus the planted coverage and drift rows

Snapshot `1333:1333:` at start and `1333:1333:` at end, PostgreSQL 18.7 (Debian 18.7-1.pgdg13+5), taken 2026-11-24 16:09:02.874045+00:00.

| Table | Admitted (a) | Admitted (b) | (a) only | (b) only | Why (a) only | Why (b) only | Examples (a) only | Examples (b) only |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| `record` | 23,318 | 23,324 | 0 | 6 | — | partner reads the member feed: deleted Record row: 3; partner reads the member feed: removed Record row: 2; hidden-type Record in the direct-share list: 1 | — | `0f6a92cc-8619-59f3-a86d-09320e47f455`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea`<br>`76496563-0809-5025-9218-9ae2d2e0698b`<br>`8b985d84-0d27-565e-b020-88c4739e5fdc`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3` |
| `record_date_index` | 21,697 | 21,707 | 2 | 12 | Space differs from its Record's: 2 | on a deleted Record: 5; on a hidden-type Record: 2; partner reads the member feed: child of a removed Record: 2; Space differs from its Record's: 2; child of a removed Record in the direct-share list: 1 | `e82e0620-4051-5ccd-8e96-0fe42192052c/start` | `0f6a92cc-8619-59f3-a86d-09320e47f455/start`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea/start`<br>`76496563-0809-5025-9218-9ae2d2e0698b/start`<br>`821be22a-1034-5671-abac-5a09a5dd4be9/start`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3/start` |
| `record_recurrence` | 7,258 | 7,268 | 2 | 12 | Space differs from its Record's: 2 | on a deleted Record: 5; on a hidden-type Record: 2; partner reads the member feed: child of a removed Record: 2; Space differs from its Record's: 2; child of a removed Record in the direct-share list: 1 | `e82e0620-4051-5ccd-8e96-0fe42192052c` | `0f6a92cc-8619-59f3-a86d-09320e47f455`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea`<br>`76496563-0809-5025-9218-9ae2d2e0698b`<br>`821be22a-1034-5671-abac-5a09a5dd4be9`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3` |
| `record_assignee_index` | 37 | 47 | 2 | 12 | Space differs from its Record's: 2 | on a deleted Record: 5; on a hidden-type Record: 2; partner reads the member feed: child of a removed Record: 2; Space differs from its Record's: 2; child of a removed Record in the direct-share list: 1 | `e82e0620-4051-5ccd-8e96-0fe42192052c` | `0f6a92cc-8619-59f3-a86d-09320e47f455`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea`<br>`76496563-0809-5025-9218-9ae2d2e0698b`<br>`821be22a-1034-5671-abac-5a09a5dd4be9`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3` |
| `record_completed_index` | 31 | 41 | 2 | 12 | Space differs from its Record's: 2 | on a deleted Record: 5; on a hidden-type Record: 2; partner reads the member feed: child of a removed Record: 2; Space differs from its Record's: 2; child of a removed Record in the direct-share list: 1 | `e82e0620-4051-5ccd-8e96-0fe42192052c` | `0f6a92cc-8619-59f3-a86d-09320e47f455`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea`<br>`76496563-0809-5025-9218-9ae2d2e0698b`<br>`821be22a-1034-5671-abac-5a09a5dd4be9`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3` |
| `assignment` | 7,860 | 7,868 | 4 | 12 | assignment variable outside calendar, origin, driver: 2; Space differs from its Record's: 2 | on a deleted Record: 5; on a hidden-type Record: 2; partner reads the member feed: child of a removed Record: 2; Space differs from its Record's: 2; child of a removed Record in the direct-share list: 1 | `0bf46c93-a2bc-5b14-8680-aaad73f6a06d`<br>`d556840d-fc00-5b8c-a011-3431cde9591f` | `0bf46c93-a2bc-5b14-8680-aaad73f6a06d`<br>`56188822-7a9f-538f-8572-e5a53c60677f`<br>`771a8d9e-a685-59d6-bbde-692523df9ccc`<br>`87f6651f-51bd-5aed-9ff6-cfac0676ab87`<br>`9381e4ef-8525-5d30-a6f2-49cd03e52a07` |
| `calendar_event` | 5,407 | 5,410 | 0 | 3 | — | partner-mirror copy: S has no mirror exclusion: 2; event's user is not its calendar's owner: 1 | — | `61143878-43fb-5c67-b635-eb263c8593a4`<br>`7219e3f9-5544-56f1-8d3f-e20bb41aa127` |
| `graph_edge` | 6,861 | 6,419 | 564 | 122 | edge on a directly shared Record: S as written gives graph_edge no share list: 559; Resource in a grantor's archived or removed Space, which the Record rule leaves out of the list: 2; edge Space differs from its end's: 2; generated/updated edge to a directly shared Resource in an unlisted Space: 1 | partner reads the member feed: generic or relates_to edge in a grantor's Space: 104; on a deleted or superseded Resource: 5; on a deleted Record: 5; partner reads the member feed: edge of a removed Record: 3; on a hidden-type Record: 2; edge Space differs from its end's: 2; partner reads the member feed: edge of an account Resource: 1 | `0060f6ae-d6da-5906-be58-aec73c1dd003`<br>`02b8b5de-1cab-58a1-96db-d0a0401dc4d3`<br>`03a3e6af-1504-56d0-9d08-b5391ea898e7`<br>`04082e08-6558-5b8d-adbe-98ed4925b28f`<br>`04446901-b23b-5da5-b876-793268feecfe` | `008e5d87-aef2-5e16-95b4-63c706e7bc0e`<br>`0267627f-32b2-5042-ac68-48d466055968`<br>`037cea25-966b-52b0-8da9-377d9925d6c8`<br>`0422fca4-41f7-5be8-a9c0-06b04087d20f`<br>`07c18fd8-8e03-56de-8885-0acbebaee66b` |

`graph_edge` under the readings S leaves open: graph_edge with the direct-share Record list: (a) only 5, (b) only 123; graph_edge with generic and relates_to tested against member Spaces only: (a) only 564, (b) only 18; both: (a) only 5, (b) only 19.

Drift check (no accounts; rows against their parents): `record_date_index` 4 (on a deleted Record: 2; Space differs from its Record's: 1; on a hidden-type Record: 1); `record_recurrence` 4 (on a deleted Record: 2; Space differs from its Record's: 1; on a hidden-type Record: 1); `record_assignee_index` 4 (on a deleted Record: 2; Space differs from its Record's: 1; on a hidden-type Record: 1); `record_completed_index` 4 (on a deleted Record: 2; Space differs from its Record's: 1; on a hidden-type Record: 1); `assignment` 4 (on a deleted Record: 2; Space differs from its Record's: 1; on a hidden-type Record: 1); `graph_edge` 7 (on a deleted Record: 3; on a deleted or superseded Resource: 2; edge Space differs from its end's: 1; on a hidden-type Record: 1).

## Planted rows

Drift rows found by the drift check: 25 of 25. Labels whose differences were not exactly the ones predicted before the run: 0.

| Planted row | Tables | (a) only, accounts | (b) only, accounts | Reason | Examples |
| --- | --- | --- | --- | --- | --- |
| shared hidden-type Record | record | 0: — | 1: [0] | hidden-type Record in the direct-share list | `cc8a5371-fc96-54ae-b39a-216167fa0c5a` |
| shared removed Record | record | 0: — | 1: [4] | partner reads the member feed: removed Record row | `9d0566df-986e-5559-acc1-0ac481dcacc3` |
| deleted shared drift parent | record | 0: — | 1: [4] | partner reads the member feed: deleted Record row | `0f6a92cc-8619-59f3-a86d-09320e47f455` |
| removed Record | record | 0: — | 1: [17] | partner reads the member feed: removed Record row | `76496563-0809-5025-9218-9ae2d2e0698b` |
| deleted Record with no children | record | 0: — | 1: [17] | partner reads the member feed: deleted Record row | `8b985d84-0d27-565e-b020-88c4739e5fdc` |
| deleted drift parent | record | 0: — | 1: [17] | partner reads the member feed: deleted Record row | `19dac5e1-c3ef-5158-a9ca-679487f3b1ea` |
| child of a shared removed Record | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 0: — | 10: [0, 4] | child of a removed Record in the direct-share list; partner reads the member feed: child of a removed Record | `9d0566df-986e-5559-acc1-0ac481dcacc3/start`<br>`9d0566df-986e-5559-acc1-0ac481dcacc3`<br>`771a8d9e-a685-59d6-bbde-692523df9ccc` |
| child whose Space differs from its Record's | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 10: [0, 17] | 10: [4, 5] | Space differs from its Record's | `e82e0620-4051-5ccd-8e96-0fe42192052c/start`<br>`e82e0620-4051-5ccd-8e96-0fe42192052c`<br>`0bf46c93-a2bc-5b14-8680-aaad73f6a06d` |
| child on a deleted Record | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 0: — | 10: [0, 17] | on a deleted Record | `19dac5e1-c3ef-5158-a9ca-679487f3b1ea/start`<br>`19dac5e1-c3ef-5158-a9ca-679487f3b1ea`<br>`9381e4ef-8525-5d30-a6f2-49cd03e52a07` |
| child on a hidden-type Record | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 0: — | 10: [0, 17] | on a hidden-type Record | `821be22a-1034-5671-abac-5a09a5dd4be9/start`<br>`821be22a-1034-5671-abac-5a09a5dd4be9`<br>`979c137f-3fcc-57fd-a290-6b76080fe521` |
| child on a deleted shared Record | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 0: — | 15: [0, 4, 5] | on a deleted Record | `0f6a92cc-8619-59f3-a86d-09320e47f455/start`<br>`0f6a92cc-8619-59f3-a86d-09320e47f455`<br>`56188822-7a9f-538f-8572-e5a53c60677f` |
| child of a removed Record | assignment, record_assignee_index, record_completed_index, record_date_index, record_recurrence | 0: — | 5: [17] | partner reads the member feed: child of a removed Record | `76496563-0809-5025-9218-9ae2d2e0698b/start`<br>`76496563-0809-5025-9218-9ae2d2e0698b`<br>`87f6651f-51bd-5aed-9ff6-cfac0676ab87` |
| assignment with another variable | assignment | 2: [0, 17] | 0: — | assignment variable outside calendar, origin, driver | `d556840d-fc00-5b8c-a011-3431cde9591f` |
| partner mirror copy | calendar_event | 0: — | 2: [0, 1] | partner-mirror copy: S has no mirror exclusion | `7219e3f9-5544-56f1-8d3f-e20bb41aa127` |
| event of another user on a grantor calendar | calendar_event | 0: — | 1: [0] | event's user is not its calendar's owner | `61143878-43fb-5c67-b635-eb263c8593a4` |
| generic edge into a partner's Space | graph_edge | 0: — | 1: [0] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `69bdb8f0-41f0-5b32-9495-c7d20cb8f5b7` |
| relates_to a partner's Resource | graph_edge | 0: — | 1: [0] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `c14ab196-9ddb-5bb7-b759-bdbdada0399d` |
| cross-home family edge from a superseded Resource | graph_edge | 0: — | 4: [0, 4, 5, 17] | on a deleted or superseded Resource | `a25b3fe6-3b40-5051-9858-244a1c430438` |
| family edge in a grantor's archived Space | graph_edge | 1: [0] | 0: — | Resource in a grantor's archived or removed Space, which the Record rule leaves out of the list | `1b27aa54-3fba-5611-bbf4-1174161c1c74` |
| generated edge to a Resource in a grantor's archived Space | graph_edge | 1: [0] | 0: — | Resource in a grantor's archived or removed Space, which the Record rule leaves out of the list | `928cf0f2-3bd2-5949-a18b-364f328d4e41` |
| attendee edge of a shared Record | graph_edge | 1: [0] | 0: — | edge on a directly shared Record: S as written gives graph_edge no share list | `a7331ec6-a6b5-5d9b-91bc-5f386312efcd` |
| generated edge from an own Record to a shared Record | graph_edge | 1: [0] | 0: — | edge on a directly shared Record: S as written gives graph_edge no share list | `a0176a29-dc68-5497-8ec0-fab999b11342` |
| updated edge between shared Records | graph_edge | 1: [0] | 0: — | edge on a directly shared Record: S as written gives graph_edge no share list | `9461776b-9ed1-55f9-9d4c-9c54cf748921` |
| generated edge to a directly shared Resource | graph_edge | 1: [0] | 0: — | generated/updated edge to a directly shared Resource in an unlisted Space | `789251fb-f5f8-53f7-b1d2-994cf36ade14` |
| open attendee edge on a deleted Record | graph_edge | 0: — | 2: [0, 17] | on a deleted Record | `a0a887ef-c430-5cbf-937f-cf3001cf1c17` |
| open updated edge on a deleted Record | graph_edge | 0: — | 2: [0, 17] | on a deleted Record | `14bc9c73-fb95-5295-81a5-b8f091ad1565` |
| open attendee edge on a hidden-type Record | graph_edge | 0: — | 2: [0, 17] | on a hidden-type Record | `de9d064c-1632-5d58-8984-fd932906b648` |
| attendee edge whose Space differs from its Record's | graph_edge | 2: [0, 17] | 2: [4, 5] | edge Space differs from its end's | `bf3867bd-931a-5ac5-9cb4-f157f1c5a258` |
| attendee edge of a shared removed Record | graph_edge | 0: — | 1: [4] | partner reads the member feed: edge of a removed Record | `de7c9cc3-ae10-5156-8ca1-7e5b68edbab1` |
| same-Space accepted_as record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `ffd8e6ed-ecfd-57a4-b3b1-f7f42d06fe2a` |
| same-Space accepted_as record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `475e5e64-bcdd-57d4-8785-81606db6b059` |
| same-Space accepted_as resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `57c173fa-4320-59be-981e-cb301c159706` |
| same-Space accepted_as resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `644fc0c0-36ed-5dad-a791-f83bd84ea01c` |
| same-Space attached_to record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `970c142b-7a69-542c-b7d5-2e871a34b32a` |
| same-Space attached_to record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `521e1000-8426-50d7-80c1-49ef0bc12964` |
| same-Space attached_to resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `57fec9e2-c758-5926-86d5-ece47702a07c` |
| same-Space attached_to resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `6a59be25-e515-5db2-b580-6b8f0465f3af` |
| same-Space attends_school record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `5ebd6c2e-6806-517c-aa5e-789b5e475358` |
| same-Space attends_school record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `c4e0abda-8489-5fb2-a496-b281b7de0180` |
| same-Space attends_school resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `9817c77a-8f7f-5457-ab70-2931121643b8` |
| same-Space caregiver_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `f6a8b191-e127-561c-a6e5-8481c171bb8a` |
| same-Space caregiver_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d9eca926-1dce-5aa5-99c3-fa79bc962cd0` |
| same-Space caregiver_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d9369388-e2c4-562a-9de5-baa3781bee0e` |
| same-Space child_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `4a1b862e-c370-51db-9f27-0976753075ce` |
| same-Space child_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `bd6b78d9-9caf-5dc2-a823-e1646a80a8dc` |
| same-Space child_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `aba0e744-7444-557a-8126-fe0dd6fa22df` |
| same-Space child_of resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `95d12c35-0e9a-5cf7-8c02-64480e156f22` |
| same-Space client_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `e4770043-be0e-5a19-b4ec-40490dc2ae82` |
| same-Space client_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `3cce5d7e-76ef-5fdc-9f8d-9c3ca8caf115` |
| same-Space client_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `5f82612d-e966-5086-b4a4-f13a50cc0fb5` |
| same-Space client_of resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `8968451a-e386-5c72-98f3-9bf780bfbeca` |
| same-Space focuses_on record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d1acfe20-2ad1-5eac-9e8a-933bfab408ff` |
| same-Space focuses_on record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d2d87b70-2427-5096-bd20-152065a6131b` |
| same-Space focuses_on resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `c4b439d5-baac-5c2a-af6d-29f0a54aa44a` |
| same-Space focuses_on resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `8e99fdca-92f6-5d00-a08a-dbf76c5dce3c` |
| same-Space generated resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `95a405f7-c3bd-5f11-aa54-8f783ff0b2d1` |
| same-Space generated resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d11be54a-cceb-5a5e-b24f-1a40bdfa4066` |
| same-Space has_teacher record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `6204e071-1dbd-5cfb-8df7-2d785608299e` |
| same-Space has_teacher record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `539fed66-f937-524a-a086-76b1e27216c4` |
| same-Space has_teacher resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `c98b3841-0d20-53a0-b110-5d41b4961ee1` |
| same-Space has_teacher resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `f6903301-224d-57ca-8be6-d885d6715512` |
| same-Space in_family_circle_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `174c9eb1-0849-5ee8-9f87-43f85d09baf9` |
| same-Space in_family_circle_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `6e4303e5-0bb1-585f-8f1d-4a78d210f53c` |
| same-Space in_family_circle_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `f2a178d8-ffb1-51bd-8bef-b84526a37daf` |
| same-Space lives_at record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `1c484531-2bb1-570b-8faf-83c8b329e51c` |
| same-Space lives_at record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `3c6d8438-480f-5911-a28a-84cf940b4046` |
| same-Space lives_at resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `8353d8b0-1453-5556-8079-5f220f9b2e05` |
| same-Space lives_at resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7a0387d6-aa2b-5baa-ba35-5a4ad5eeb99f` |
| same-Space located_at record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `e3cae374-8cdd-59a0-98f7-4eae6b857f98` |
| same-Space located_at record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `1978e902-58e1-5435-ada1-8abb238295a6` |
| same-Space located_at resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0d533603-2ba1-5e92-8fe1-01be6d75264a` |
| same-Space located_at resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `fa20af8f-37cb-58cb-b532-f0ba4dba9e2a` |
| same-Space mentions record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `426c5e8e-af66-5cc4-b4a0-d831133f8676` |
| same-Space mentions record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `3c35dad0-031d-5468-a587-acc13b524065` |
| same-Space mentions resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `90e430a5-0f61-51ba-9da5-3e0d8ff4b60b` |
| same-Space mentions resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d14a7645-c655-5165-8639-75a7bf240b7f` |
| same-Space merged_into record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `37b1ab5b-1f73-5557-88e3-00375dd5d048` |
| same-Space merged_into record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `a2e61d0d-eb9d-55b9-a1bf-3ec256456d72` |
| same-Space merged_into resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `da2c984b-930b-56cc-8e92-0acbcae99b07` |
| same-Space merged_into resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `eeb1a9e0-6b7a-5b1e-b34a-543d8ce109d1` |
| same-Space parent_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7f4be8b9-8b22-5f14-b406-f6167fcc0fc9` |
| same-Space parent_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `68f7e2ae-f603-5528-ad9f-860bd7df3f42` |
| same-Space parent_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `aebbdcc2-5189-5896-9c2a-9e4a979213bc` |
| same-Space participates_in record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `cd20a275-63df-5b2b-a214-0af524263865` |
| same-Space participates_in record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7a40c9ac-1f23-50d8-a245-88ab740445ec` |
| same-Space participates_in resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `f9f2bde5-2824-50b0-856f-cf01bb83d721` |
| same-Space participates_in resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `9734ff21-df30-5f95-a096-f8aaf7ee098c` |
| same-Space partner_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b9ab459d-b780-5867-9224-53efb0080a5f` |
| same-Space partner_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7267ae4e-dfbd-5cea-bee0-62276f0473d0` |
| same-Space partner_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `a49063f2-aaea-5375-b1a9-d829b8c267eb` |
| same-Space patient_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `6a63dc1c-a12a-50f6-bdd7-ec0d0a5fb980` |
| same-Space patient_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `1060bd0f-4389-537b-bbf1-0dc45764a7ec` |
| same-Space patient_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `8734a9a7-55a5-5b53-ba33-f9630e2f6eee` |
| same-Space patient_of resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b62a80e1-5821-5724-a485-ff759c797cbe` |
| same-Space pet_of record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0f0d25b4-245a-5c82-8a94-c7004458b689` |
| same-Space pet_of record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `4e1db9d7-2d5b-594b-a681-65f6f58e7a4e` |
| same-Space pet_of resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0422fca4-41f7-5be8-a9c0-06b04087d20f` |
| same-Space posted_to record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7b7172eb-af9d-538a-a90c-4e1a8ff475c9` |
| same-Space posted_to record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0cd8944d-0572-51f4-85b9-0cdc6c525405` |
| same-Space posted_to resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0f35a463-eb56-5b4e-aa8d-6b898c0ca115` |
| same-Space posted_to resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `171a0419-d30f-543d-9745-48c872053e14` |
| same-Space provides_service_at record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `037cea25-966b-52b0-8da9-377d9925d6c8` |
| same-Space provides_service_at record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `f87ffb9b-dbbe-56e6-b4ce-bf80f71688f4` |
| same-Space provides_service_at resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `a5caea88-c744-5c2f-9c43-d61d392b307d` |
| same-Space provides_service_at resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `07c18fd8-8e03-56de-8885-0acbebaee66b` |
| same-Space references record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `ed41e313-01e4-5938-95f8-01bd2911f81b` |
| same-Space references record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b70fae87-0a8e-5aae-b929-e5458e292d5a` |
| same-Space references resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `e5fc7abb-4644-5184-ad5a-d410d1e780b8` |
| same-Space references resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7440b70a-b411-5e14-9c1f-b099d2d6766a` |
| same-Space relates_to record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b20d17bb-39bd-59d8-9e4c-30d8c8d28a60` |
| same-Space shared_with record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `ebb4f06b-ce4b-5e1c-a4b7-a09c5d79d3b6` |
| same-Space shared_with record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `e130d11c-b1a6-5a10-ae8f-5df6582b0ee2` |
| same-Space shared_with resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `0267627f-32b2-5042-ac68-48d466055968` |
| same-Space shared_with resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b5c9adf5-0765-56c2-9648-4c603e42a720` |
| same-Space source_audio record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `3cd626cb-459a-5a00-bf4e-284cc3af23df` |
| same-Space source_audio record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `be591bec-87f5-50ce-8278-d2676743bb80` |
| same-Space source_audio resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `ba6ae460-6f73-54fd-b231-633ffab95208` |
| same-Space source_audio resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `83e0ea6a-0a11-5867-8b21-6c6554c63a0e` |
| same-Space tagged_with record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `7ee03acf-1e5c-51b9-a145-5ba36f57866e` |
| same-Space tagged_with record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `75aeb4d2-d033-5800-9b8e-66ff0766a334` |
| same-Space tagged_with resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `9fb1d01b-d2c7-5235-83b8-9385a7ad431b` |
| same-Space tagged_with resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `9b0f4e9c-61e8-58b0-bbb6-8d9766798e36` |
| same-Space triggered_by record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `504b8a18-d742-5f00-be14-3033f01ab032` |
| same-Space triggered_by record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `397214b4-d885-5fd3-8935-78afabaf5837` |
| same-Space triggered_by resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `3ac496bd-084d-51cc-a09b-7bf4297cecf8` |
| same-Space triggered_by resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `008e5d87-aef2-5e16-95b4-63c706e7bc0e` |
| same-Space works_at record→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `2285b850-6142-546d-9f97-71574d83246c` |
| same-Space works_at record→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `c7f9d7b1-a90f-528a-837b-99d2cb43273e` |
| same-Space works_at resource→record | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `827d611d-dfc3-592b-8a7c-8df4556df832` |
| same-Space works_at resource→resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `b2c9947d-cf69-596e-920b-9a1cb7f318fe` |
| same-Space generic edge scoped to its Space | graph_edge | 0: — | 1: [17] | partner reads the member feed: generic or relates_to edge in a grantor's Space | `d0c2b113-aec9-55d8-afcb-63648cd8820e` |
| attendee edge of a removed Record | graph_edge | 0: — | 1: [17] | partner reads the member feed: edge of a removed Record | `97d22ee9-3bdc-5442-ae49-b75ab85b039d` |
| updated edge from a removed Record | graph_edge | 0: — | 1: [17] | partner reads the member feed: edge of a removed Record | `e0670151-f4da-52e1-a868-15acad361bc3` |
| family edge to an account Resource | graph_edge | 0: — | 1: [17] | partner reads the member feed: edge of an account Resource | `748017c4-8518-584e-a868-59d0e14a4d1c` |
| family edge from a superseded Resource | graph_edge | 0: — | 1: [17] | on a deleted or superseded Resource | `def6b5e4-c184-5ec8-b6e4-2e547d8d202f` |
| open generated edge on a deleted Record | graph_edge | 0: — | 1: [17] | on a deleted Record | `8ee2ac4e-ec3f-5d9f-9352-5830f0964016` |
