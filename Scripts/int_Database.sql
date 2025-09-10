
/*

-----------------------------
Create Database and Schemas
-----------------------------
Script Purpose:

	This script creates a new database named 'Data Warehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within 
	the database : 'bronze', 'silver', and 'gold' 

WARNING:
	
	Running this script will drop the entire 'Data Warehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups
	before running this script.

	*/

	----Create Database 'DataWareHouse'
use master;

----Drop and recreate 'DataWareHouse' database
if exists (select 1 from sys.databases where name= 'DataWareHouse')
begin 
	alter database DataWareHouse set single_user with rollback immediate;
	drop database DataWareHouse;
	end;
	go

----Create the 'DataWareHouse' database
create database DataWareHouse;
go

use DataWareHouse;
go

----Create Schemas
Create schema bronze;
go

Create schema silver;
go

Create schema gold;
go
