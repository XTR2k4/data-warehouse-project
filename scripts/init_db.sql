/*
=============================================================
Create the Database and Schemas
=============================================================
Script Purpose: 
  This script creates a new database named 'DataWarehouse' and creating the required three schemas within the database namely, 'bronze', 'silver'  and 'gold'.
*/

-- Creating the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creating Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
