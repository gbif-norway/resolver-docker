BEGIN;
--
-- Create model DarwinCoreObject
--
CREATE TABLE "website_darwincoreobject" ("id" varchar(200) NOT NULL PRIMARY KEY, "data" jsonb NOT NULL, "deleted_date" date NULL, "created_date" date NOT NULL);
--
-- Create model Statistic
--
CREATE TABLE "website_statistic" ("name" varchar(100) NOT NULL PRIMARY KEY, "value" integer NOT NULL);
--
-- Create model History
--
CREATE TABLE "website_history" ("id" serial NOT NULL PRIMARY KEY, "changed_data" jsonb NOT NULL, "changed_date" date NOT NULL, "darwin_core_object_id" varchar(200) NOT NULL);
--
-- Create index website_dar_data_12a5e6_gin on field(s) data of model darwincoreobject
--
CREATE INDEX "website_dar_data_12a5e6_gin" ON "website_darwincoreobject" USING gin ("data");
CREATE INDEX "website_darwincoreobject_id_9b37f2ce_like" ON "website_darwincoreobject" ("id" varchar_pattern_ops);
CREATE INDEX "website_statistic_name_52e34005_like" ON "website_statistic" ("name" varchar_pattern_ops);
ALTER TABLE "website_history" ADD CONSTRAINT "website_history_darwin_core_object_i_9a84500f_fk_website_d" FOREIGN KEY ("darwin_core_object_id") REFERENCES "website_darwincoreobject" ("id") DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX "website_history_darwin_core_object_id_9a84500f" ON "website_history" ("darwin_core_object_id");
CREATE INDEX "website_history_darwin_core_object_id_9a84500f_like" ON "website_history" ("darwin_core_object_id" varchar_pattern_ops);
COMMIT;
