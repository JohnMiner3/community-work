\set ON_ERROR_STOP
\set ECHO all
BEGIN;
\set ECHO all
CREATE SEQUENCE "public"."jobs_job_id_seq" INCREMENT BY 1 START WITH 1 OWNED BY "public"."jobs"."job_id";
ALTER TABLE "public"."authors" ADD CONSTRAINT "upkcl_auidind" PRIMARY KEY ("au_id");
ALTER TABLE "public"."employee" ADD CONSTRAINT "pk_emp_id" PRIMARY KEY ("emp_id");
ALTER TABLE "public"."jobs" ADD PRIMARY KEY ("job_id");
ALTER TABLE "public"."pub_info" ADD CONSTRAINT "upkcl_pubinfo" PRIMARY KEY ("pub_id");
ALTER TABLE "public"."publishers" ADD CONSTRAINT "upkcl_pubind" PRIMARY KEY ("pub_id");
ALTER TABLE "public"."sales" ADD CONSTRAINT "upkcl_sales" PRIMARY KEY ("stor_id","ord_num","title_id");
ALTER TABLE "public"."stores" ADD CONSTRAINT "upk_storeid" PRIMARY KEY ("stor_id");
ALTER TABLE "public"."titleauthor" ADD CONSTRAINT "upkcl_taind" PRIMARY KEY ("au_id","title_id");
ALTER TABLE "public"."titles" ADD CONSTRAINT "upkcl_titleidind" PRIMARY KEY ("title_id");
ALTER TABLE "public"."discounts" ADD FOREIGN KEY ("stor_id") REFERENCES "public"."stores" ( "stor_id");
ALTER TABLE "public"."employee" ADD FOREIGN KEY ("job_id") REFERENCES "public"."jobs" ( "job_id");
ALTER TABLE "public"."employee" ADD FOREIGN KEY ("pub_id") REFERENCES "public"."publishers" ( "pub_id");
ALTER TABLE "public"."pub_info" ADD FOREIGN KEY ("pub_id") REFERENCES "public"."publishers" ( "pub_id");
ALTER TABLE "public"."roysched" ADD FOREIGN KEY ("title_id") REFERENCES "public"."titles" ( "title_id");
ALTER TABLE "public"."sales" ADD FOREIGN KEY ("stor_id") REFERENCES "public"."stores" ( "stor_id");
ALTER TABLE "public"."sales" ADD FOREIGN KEY ("title_id") REFERENCES "public"."titles" ( "title_id");
ALTER TABLE "public"."titleauthor" ADD FOREIGN KEY ("au_id") REFERENCES "public"."authors" ( "au_id");
ALTER TABLE "public"."titleauthor" ADD FOREIGN KEY ("title_id") REFERENCES "public"."titles" ( "title_id");
ALTER TABLE "public"."titles" ADD FOREIGN KEY ("pub_id") REFERENCES "public"."publishers" ( "pub_id");
ALTER TABLE "public"."authors" ALTER COLUMN "phone" SET DEFAULT 'UNKNOWN';
ALTER TABLE "public"."employee" ALTER COLUMN "job_id" SET DEFAULT 1;
ALTER TABLE "public"."employee" ALTER COLUMN "job_lvl" SET DEFAULT 10;
ALTER TABLE "public"."employee" ALTER COLUMN "pub_id" SET DEFAULT '9952';
ALTER TABLE "public"."jobs" ALTER COLUMN "job_desc" SET DEFAULT 'New Position - title not formalized yet';
ALTER TABLE "public"."jobs" ALTER COLUMN "job_id" SET DEFAULT nextval('"public"."jobs_job_id_seq"');
ALTER TABLE "public"."publishers" ALTER COLUMN "country" SET DEFAULT 'USA';
ALTER TABLE "public"."titles" ALTER COLUMN "type" SET DEFAULT 'UNDECIDED';
COMMIT;
