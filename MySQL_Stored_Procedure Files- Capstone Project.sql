-- Create Database and import Tables

DROP DATABASE IF EXISTS mining_project;

CREATE DATABASE mining_project;
USE mining_project;

/* All cleaned CSV files for cycle, delay and location are imported directly in new tables */

SELECT COUNT(*) FROM cycle_cleaned_data; -- verify counts in cycle table

SELECT COUNT(*) FROM delay_cleaned_data; -- verify counts in delay table

SELECT COUNT(*) FROM location_cleaned_data; -- verify counts in location table

-- ------------------------------------------------------------------------------------------------------------------------------------------------

/* First, create a table to hold the derived movement data:*/

/* "Create Stored Procedure to Populate Movement Data"

This procedure will pull data from cycle_cleaned_data into movement_data:*/

DROP PROCEDURE IF EXISTS sp_create_movement_data;
DELIMITER $$
CREATE PROCEDURE sp_create_movement_data()
BEGIN
    CREATE TABLE IF NOT EXISTS Movement_Data AS
    SELECT 
        ROW_NUMBER() OVER () AS movement_id,
        primary_machine_name     AS equipment_name,
        secondary_machine_name   AS secondary_equipment,
        source_location_name     AS source_location,
        destination_location_name AS destination_location,
        payload_kg            AS payload_kg,
        cycle_start_timestamp_gmt8 AS start_time,
        cycle_end_timestamp_gmt8   AS end_time
    FROM cycle_cleaned_data;
END$$
DELIMITER ;

/* Lets Run the Procedure to populate movement data: */

CALL sp_create_movement_data();


/* Now lets check the newly drived movement data:*/

SELECT 
    *
FROM
    Movement_Data
LIMIT 10;


-- -----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Lets Create Master Tables using Stored Procedures :
-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*  1) Equipment Master */

DROP PROCEDURE IF EXISTS sp_equipment_master;  -- drop procedure if exists

DELIMITER $$

CREATE PROCEDURE sp_equipment_master()
BEGIN
    CREATE TABLE IF NOT EXISTS Equipment_Master AS
    SELECT
        primary_machine_name      AS equipment_name,
        primary_machine_class_name AS equipment_class,
        secondary_machine_name     AS secondary_equipment,
        secondary_machine_class_name AS secondary_class,
        loading_count              AS loading_count,
        imine_load_fctr_truck      AS load_factor,
        previoussecondarymachine   AS prev_secondary_machine,
        previoussinkdestination    AS prev_sink_destination,
        end_processor_name         AS processor,
        imine_engine_hours         AS engine_hours,
        imine_operating_hours      AS operating_hours,
        operatingtime_cat        AS cat_operating_time,
        operhoursseconds           AS operating_seconds,
        full_travel_duration       AS full_travel_duration,
        empty_travel_duration      AS empty_travel_duration,
        idle_duration              AS idle_duration,
        loading_duration           AS loading_duration,
        waitfordumpduration        AS wait_for_dump,
        dumping_duration           AS dumping_duration,
        payload_kg               AS payload_kg,
        estimated_fuel_used        AS est_fuel_used,
        fuel_used                  AS fuel_used,
        loading_efficiency         AS loading_efficiency,
        operatingburnrate          AS operating_burnrate,
        tmph                       AS tmph,
        job_code_name              AS job_code
    FROM cycle_cleaned_data;
END$$
DELIMITER ;

CALL sp_equipment_master(); -- verify by calling stored procedure

/* Lets check Equipment master table */

SELECT 
    *
FROM
    Equipment_Master
LIMIT 10;


-- ---------------------------------------------------------------------------------------------------------------------------------------------------

/* 2) Equipment Type Master */


DROP PROCEDURE IF EXISTS sp_equipment_type_master;
DELIMITER $$
CREATE PROCEDURE sp_equipment_type_master()
BEGIN
    CREATE TABLE IF NOT EXISTS Equipment_Type_Master AS
    SELECT
        cycle_type                   AS cycle_type,
        primary_machine_category_name AS primary_category,
        secondary_machine_category_name AS secondary_category,
        tc                           AS tc,
        at_available_time_imine    AS available_time,
        available_smu_time           AS available_smu,
        cycle_duration               AS cycle_duration,
        cycle_smu_duration           AS cycle_smu,
        down_time                    AS down_time,
        completed_cycle_count        AS completed_cycles,
        imine_availability           AS imine_availability,
        imine_utilisation            AS imine_utilisation,
        job_type                     AS job_type
    FROM cycle_cleaned_data;
END$$
DELIMITER ;

CALL sp_equipment_type_master(); -- verify by calling stored procedure

/* Lets check Equipment Type master table */

SELECT 
    *
FROM
    Equipment_Type_Master
LIMIT 10;
-- ----------------------------------------------------------------------------------------------------------------


/* 3) Location Master */


DROP PROCEDURE IF EXISTS sp_location_master;  -- drop stored procedure if exists


DELIMITER $$
CREATE PROCEDURE sp_location_master()
BEGIN
    CREATE TABLE IF NOT EXISTS Location_Master AS
    SELECT
        source_location_name           AS source_location,
        destination_location_name      AS destination_location,
        queuing_at_sink_duration       AS queue_sink,
        queuing_at_source_duration     AS queue_source,
        queuing_duration               AS queue_total,
        cycle_end_timestamp_gmt8     AS end_time,
        cycle_start_timestamp_gmt8   AS start_time,
        source_loading_end_timestamp_gmt8 AS load_start_time,
        source_loading_start_timestamp_gmt8   AS load_end_time
    FROM cycle_cleaned_data;
END$$
DELIMITER ;

CALL sp_location_master(); -- verify by calling stored procedure

/* Lets check Location master table */

SELECT 
    *
FROM
    Location_Master
LIMIT 10;


-- ------------------------------------------------------------------------------------------------------------------------------

/* 4) Location Type Master */


DROP PROCEDURE IF EXISTS sp_location_type_master;   -- drop stored procedure if exists


DELIMITER $$
CREATE PROCEDURE sp_location_type_master()
BEGIN
    CREATE TABLE IF NOT EXISTS Location_Type_Master AS
    SELECT
        source_location_description       AS source_loc_desc,
        destination_location_description  AS dest_loc_desc,
        empty_efh_distance                AS empty_distance,
        empty_slope_distance              AS slope_distance,
        queuing_at_sink_duration          AS queue_sink,
        queuing_at_source_duration        AS queue_source,
        queuing_duration                  AS queue_total,
        source_location_is_active_flag    AS src_active_flag,
        source_location_is_source_flag    AS src_flag,
        destination_location_is_active_flag AS dest_active_flag,
        destination_location_is_source_flag AS dest_flag
    FROM cycle_cleaned_data;
END$$
DELIMITER ;

CALL sp_location_type_master(); -- verify by calling stored procedure

/* Lets check Location Type master table */

SELECT 
    *
FROM
    Location_Type_Master
LIMIT 10;



-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Lets Create Metrics Stored Procedures :
-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------



/* 5) Stored Procedure for Cycle Data */
  

DROP PROCEDURE IF EXISTS sp_cycle_data_metrics;  -- drop stored procedure if exists

DELIMITER $$
CREATE PROCEDURE sp_cycle_data_metrics()
BEGIN
    SELECT
        primary_machine_name        AS equipment,
        primary_machine_class_name  AS equipment_class,
        secondary_machine_name      AS secondary_equipment,
        secondary_machine_class_name AS secondary_class,
        loading_count,
        cycle_type,
        tc,
        cycle_duration,
        cycle_smu_duration,
        imine_engine_hours,
        imine_operating_hours,
        operatingtime_cat,
        operhoursseconds,
        cycle_start_timestamp_gmt8 AS start_time,
        cycle_end_timestamp_gmt8   AS end_time,
		TIMESTAMPDIFF(
            MINUTE,
            cycle_start_timestamp_gmt8,
            cycle_end_timestamp_gmt8
        )                                AS total_cycle_minutes,
        payload_kg,
        empty_efh_distance,
        completed_cycle_count,
        imine_availability,
        imine_utilisation
    FROM cycle_cleaned_data;
END$$
DELIMITER ;cycle_cleaned_data


CALL sp_cycle_data_metrics();  -- verify by calling stored procedure
-- ----------------------------------------------------------------------------------------------------------------------------

/* 6) Stored Procedure for Delay Data */

DROP PROCEDURE IF EXISTS sp_delay_data_metrics;   -- drop stored procedure if exists

DELIMITER $$

CREATE PROCEDURE sp_delay_data_metrics()
BEGIN
    SELECT
        delay_oid                        AS delay_id,
        target_machine_name              AS equipment_name,
        target_machine_class_name        AS equipment_class,
        delay_class_name                 AS delay_class_name,
        delay_class_category_name        AS delay_category,
        delay_status_description         AS delay_status,
        delay_start_timestamp_gmt8       AS delay_start_time,
        delay_finish_timestamp_gmt8      AS delay_end_time,
        TIMESTAMPDIFF(
            MINUTE,
            delay_start_timestamp_gmt8,
            delay_finish_timestamp_gmt8
        )                                AS total_delay_minutes,
        delay_reported_by_person_name    AS reported_by_person
    FROM delay_cleaned_data;
END$$

DELIMITER ;

-- Run the procedure to populate Delay_Data
CALL sp_delay_data_metrics();


-- ---------------------------------------------------------------------------------------------------------------------------------------------------------

/* 7) Stored Procedure for Movement Data (derived from Cycle Data) */ 

DROP PROCEDURE IF EXISTS sp_movement_data;   -- drop procedure if exists


DELIMITER $$

CREATE PROCEDURE sp_movement_data_metrics()
BEGIN
    SELECT
        movement_id,
        equipment_name,
        secondary_equipment,
        source_location,
        destination_location,
        payload_kg,
        start_time,
        end_time,
        TIMESTAMPDIFF(
            MINUTE,
            start_time,
            end_time
        ) AS movement_duration_minutes
    FROM Movement_Data;
END$$

DELIMITER ;

-- Run the procedure
CALL sp_movement_data_metrics();

-- -------------------------------------------------------------------------------------------------------------------------------------------------------

/*
    8) Stored Procedure for OEE

	OEE = Availability * Performance * Quality 
*/

DROP PROCEDURE IF EXISTS sp_oee_metric;   -- drop procedure if exists


DELIMITER $$

CREATE PROCEDURE sp_oee_metric()
BEGIN
	CREATE TABLE IF NOT EXISTS OEE AS
    SELECT
        primary_machine_name AS equipment_name,
        primary_machine_class_name AS equipment_class,
        -- Aggregated raw values
        SUM(at_available_time_imine) AS total_available_time,
        SUM(down_time) AS total_down_time,
        SUM(imine_operating_hours) AS total_operating_hours,
        SUM(operatingtime_cat) AS total_operating_time_cat,
        SUM(idle_duration) AS total_idle_duration,

        -- Availability
        ((SUM(at_available_time_imine) - SUM(down_time)) / 
          NULLIF(SUM(at_available_time_imine),0)) * 100 AS Availability,

        -- Performance
        ((SUM(operatingtime_cat) - SUM(idle_duration)) / 
          NULLIF(SUM(operatingtime_cat),0)) * 100 AS Performance,

        -- Quality
        ((SUM(imine_operating_hours) - SUM(down_time)) / 
          NULLIF(SUM(down_time) + SUM(idle_duration),0)) * 100 AS Quality,

        -- Final OEE
       ROUND (
          ((SUM(at_available_time_imine) - SUM(down_time)) / NULLIF(SUM(at_available_time_imine),0)) *
          ((SUM(operatingtime_cat) - SUM(idle_duration)) / NULLIF(SUM(operatingtime_cat),0)) *
          ((SUM(imine_operating_hours) - SUM(down_time)) / NULLIF(SUM(down_time) + SUM(idle_duration),0))
        ,2) * 100 AS Final_OEE

   FROM cycle_cleaned_data
   GROUP BY primary_machine_name, primary_machine_class_name;
END$$

DELIMITER ;
-- Run the procedure

CALL sp_oee_metric();

-- verify oee table
SELECT 
    *
FROM
    OEE
LIMIT 10;

-- ----------------------------------------------------------------------------------------------------------------------------------------------------------------

