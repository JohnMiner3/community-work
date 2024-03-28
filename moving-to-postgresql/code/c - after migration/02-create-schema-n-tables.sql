/******************************************************
 *
 * Name:         02-create-schema-n-tables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2024
 *     Purpose:  Create schema + tables.
 * 
 ******************************************************/



/*  
	Create dbo schema, postgres's default is public
*/

-- drop existing schema
drop schema if exists "dbo" cascade;

-- create new schema
create schema "dbo";



/*  
	Create tables under dbo schema
*/

BEGIN;


CREATE TABLE "dbo"."authors"( 
	"au_id" varchar(11) NOT NULL,
	"au_lname" varchar(40) NOT NULL,
	"au_fname" varchar(20) NOT NULL,
	"phone" char(12) NOT NULL,
	"address" varchar(40),
	"city" varchar(20),
	"state" char(2),
	"zip" char(5),
	"contract" boolean NOT NULL);

CREATE TABLE "dbo"."discounts"( 
	"discounttype" varchar(40) NOT NULL,
	"stor_id" char(4),
	"lowqty" smallint,
	"highqty" smallint,
	"discount" numeric(4, 2) NOT NULL);

CREATE TABLE "dbo"."employee"( 
	"emp_id" char(9) NOT NULL,
	"fname" varchar(20) NOT NULL,
	"minit" char(1),
	"lname" varchar(30) NOT NULL,
	"job_id" smallint NOT NULL,
	"job_lvl" smallint,
	"pub_id" char(4) NOT NULL,
	"hire_date" timestamp NOT NULL);

CREATE TABLE "dbo"."jobs"( 
	"job_id" smallint NOT NULL,
	"job_desc" varchar(50) NOT NULL,
	"min_lvl" smallint NOT NULL,
	"max_lvl" smallint NOT NULL);

CREATE TABLE "dbo"."pub_info"( 
	"pub_id" char(4) NOT NULL,
	"logo" bytea,
	"pr_info" text);

CREATE TABLE "dbo"."publishers"( 
	"pub_id" char(4) NOT NULL,
	"pub_name" varchar(40),
	"city" varchar(20),
	"state" char(2),
	"country" varchar(30));

CREATE TABLE "dbo"."roysched"( 
	"title_id" varchar(6) NOT NULL,
	"lorange" int,
	"hirange" int,
	"royalty" int);

CREATE TABLE "dbo"."sales"( 
	"stor_id" char(4) NOT NULL,
	"ord_num" varchar(20) NOT NULL,
	"ord_date" timestamp NOT NULL,
	"qty" smallint NOT NULL,
	"payterms" varchar(12) NOT NULL,
	"title_id" varchar(6) NOT NULL);

CREATE TABLE "dbo"."stores"( 
	"stor_id" char(4) NOT NULL,
	"stor_name" varchar(40),
	"stor_address" varchar(40),
	"city" varchar(20),
	"state" char(2),
	"zip" char(5));

CREATE TABLE "dbo"."titleauthor"( 
	"au_id" varchar(11) NOT NULL,
	"title_id" varchar(6) NOT NULL,
	"au_ord" smallint,
	"royaltyper" int);

CREATE TABLE "dbo"."titles"( 
	"title_id" varchar(6) NOT NULL,
	"title" varchar(80) NOT NULL,
	"type" char(12) NOT NULL,
	"pub_id" char(4),
	"price" numeric,
	"advance" numeric,
	"royalty" int,
	"ytd_sales" int,
	"notes" varchar(200),
	"pubdate" timestamp
);

COMMIT;
