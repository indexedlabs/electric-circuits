ALTER TABLE "public"."record_date_index" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."record" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."record_recurrence" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."graph_edge" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."assignment" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."record_completed_index" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
ALTER TABLE "public"."record_assignee_index" ADD COLUMN audience_key uuid, ADD COLUMN reader_class text;
