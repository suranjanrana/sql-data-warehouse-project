/*
========================================================================
Create Schemas
========================================================================
Script Purpose:
	This script creates 3 schemas (only if the schema doesn't exist) within the database selected in Postgres.
	The schemas created are: 'bronze', 'silver', and 'gold'
*/

-- Create schemas
create schema if not exists bronze;

create schema if not exists silver;

create schema if not exists gold;
