-- 2 procedures to trasfer data in different schemas -----------------

-- Below procedure to Transform data from stage schema to clean schema --------
CREATE OR REPLACE PROCEDURE stage_sch.merge_location_to_clean()
RETURNS STRING
LANGUAGE SQL
AS

DECLARE 
    error_msg VARCHAR;

BEGIN

    BEGIN
    
        MERGE INTO clean_sch.restaurant_location AS target
        	USING (
            	SELECT 
                	CAST(LocationID AS NUMBER) AS Location_ID,
                	CAST(City AS STRING) AS City,
                	CASE 
                    		WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
                    		ELSE CAST(State AS STRING)
                	END AS State,
                -- State Code Mapping
                	CASE 
                    		WHEN State = 'Andhra Pradesh' THEN 'AP'
                            WHEN State = 'Arunachal Pradesh' THEN 'AR'                            
                    		WHEN State = 'Assam' THEN 'AS'
                            WHEN State = 'Bihar' THEN 'BR'
                            WHEN State = 'Chhattisgarh' THEN 'CG'
                            --WHEN State = 'Delhi' THEN 'DL'
                            WHEN State = 'Goa' THEN 'GA'
                            WHEN State = 'Gujarat' THEN 'GJ'
                            WHEN State = 'Haryana' THEN 'HR'
                            WHEN State = 'Himachal Pradesh' THEN 'HP'
                    		--WHEN State = 'Jammu and Kashmir' THEN 'JK'
                            WHEN State = 'Jharkhand' THEN 'JH'
                            WHEN State = 'Karnataka' THEN 'KA'
                            WHEN State = 'Kerala' THEN 'KL'
                            WHEN State = 'Madhya Pradesh' THEN 'MP'
                            WHEN State = 'Maharashtra' THEN 'MH'
                            WHEN State = 'Manipur' THEN 'MN'
                            WHEN State = 'Meghalaya' THEN 'ML'
                            WHEN State = 'Mizoram' THEN 'MZ'
                            WHEN State = 'Nagaland' THEN 'NL'
                            WHEN State = 'Odisha' THEN 'OR'
                    		--WHEN State = 'Puducherry' THEN 'PY'
                            WHEN State = 'Punjab' THEN 'PB'
                            WHEN State = 'Rajasthan' THEN 'RJ'
                            WHEN State = 'Sikkim' THEN 'SK'
                    		WHEN State = 'Tamil Nadu' THEN 'TN'
                            WHEN State = 'Telangana' THEN 'TG'
                            WHEN State = 'Tripura' THEN 'TR'                      
                            WHEN State = 'Uttar Pradesh' THEN 'UP'
                    		WHEN State = 'Uttarakhand' THEN 'UK'
                            WHEN State = 'West Bengal' THEN 'WB'
                            -- below are the union territories and their codes -----------------
                            WHEN State = 'Andaman and Nicobar Islands' THEN 'AN'
                            WHEN State = 'Chandigarh' THEN 'CH'
                            WHEN State = 'Dadra and Nagar Haveli and Daman and Diu' THEN 'DD'
                            WHEN State = 'Delhi' THEN 'DL'
                            WHEN State = 'New Delhi' THEN 'DL'
                            WHEN State = 'Jammu and Kashmir' THEN 'JK'
                            WHEN State = 'Ladakh' THEN 'LA'
                            WHEN State = 'Lakshadweep' THEN 'LD'                            
                            WHEN State = 'Puducherry' THEN 'PY'                            
                    		ELSE NULL
                	END AS state_code,
                	CASE 
                    		WHEN State IN ('Andaman and Nicobar Islands', 'Chandigarh','Dadra and Nagar Haveli and Daman and Diu',
                                           'Delhi','New Delhi', 'Puducherry', 'Jammu and Kashmir', 'Ladakh', 'Lakshadweep') THEN 'Y'
                    		ELSE 'N'
                	END AS is_union_territory,
                	CASE 
                    		WHEN (State = 'Andhra Pradesh' AND City = 'Amaravati') THEN TRUE
                            WHEN (State = 'Arunachal Pradesh' AND City = 'Itanagar') THEN TRUE
                            WHEN (State = 'Assam' AND City = 'Dispur') THEN TRUE
                            WHEN (State = 'Bihar' AND City = 'Patna') THEN TRUE
                            WHEN (State = 'Chhattisgarh' AND City = 'Raipur') THEN TRUE
                            WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
                            WHEN (State = 'Gujarat' AND City = 'Gandhinagar') THEN TRUE
                            WHEN (State = 'Haryana' AND City = 'Chandigarh') THEN TRUE
                            WHEN (State = 'Himachal Pradesh' AND City = 'Shimla') THEN TRUE
                            WHEN (State = 'Jharkhand' AND City = 'Ranchi') THEN TRUE
                            WHEN (State = 'Karnataka' AND City = 'Bangaluru') THEN TRUE
                            WHEN (State = 'Kerala' AND City = 'Thiruvananthapuram') THEN TRUE
                            WHEN (State = 'Madhya Pradesh' AND City = 'Bhopal') THEN TRUE
                            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
                            WHEN (State = 'Manipur' AND City = 'Imphal') THEN TRUE
                            WHEN (State = 'Meghalaya' AND City = 'Shillong') THEN TRUE
                            WHEN (State = 'Mizoram' AND City = 'Aizawl') THEN TRUE
                            WHEN (State = 'Nagaland' AND City = 'Kohima') THEN TRUE
                            WHEN (State = 'Odisha' AND City = 'Bhubaneswar') THEN TRUE
                            WHEN (State = 'Punjab' AND City = 'Chandigarh') THEN TRUE
                            WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
                            WHEN (State = 'Sikkim' AND City = 'Gangtok') THEN TRUE
                            WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
                            WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
                            WHEN (State = 'Tripura' AND City = 'Agartala') THEN TRUE
                            WHEN (State = 'Uttar Pradesh' AND City = 'Lucknow') THEN TRUE
                            WHEN (State = 'Uttarakhand' AND City = 'Dehradun') THEN TRUE
                            WHEN (State = 'West Bengal' AND City = 'Kolkata') THEN TRUE
                             -- below are the union territories and their capitals  ------------------------------------------------------
                            WHEN (State = 'Andaman and Nicobar Islands' AND City = 'Port Blair') THEN TRUE
                            WHEN (State = 'Chandigarh' AND City = 'Chandigarh') THEN TRUE
                            WHEN (State = 'Dadra and Nagar Haveli and Daman and Diu' AND City = 'Daman') THEN TRUE
                            WHEN (State IN ('Delhi','New Delhi') AND City IN ('Delhi','New Delhi')) THEN TRUE
                            WHEN (State = 'Jammu and Kashmir' AND City IN('Srinagar','Jammu')) THEN TRUE
                            WHEN (State = 'Ladakh' AND City IN ('Leh','Kargil')) THEN TRUE
                            WHEN (State = 'Lakshadeep' AND City = 'Kavaratti') THEN TRUE
                    		WHEN (State = 'Puducherry' AND City = 'Puducherry') THEN TRUE
                    		ELSE FALSE
                	END AS capital_city_flag,

                	CASE 
                    		WHEN City IN ('Ahmedabad','Bengaluru','Chennai','Delhi','New Delhi','Hyderabad','Kolkata','Mumbai','Pune') THEN 'Tier-1'
                    		WHEN City IN ( 'Agra', 'Aurangabad', 'Bhopal', 'Bhubaneswar', 'Chandigarh', 'Chandrapur', 'Coimbatore', 'Dhanbad', 'Guwahati', 'Indore', 'Jaipur',   'Jammu', 'Kanpur', 'Kochi', 'Lucknow', 'Ludhiana', 'Madurai', 'Meerut', 'Mysuru', 'Mysore', 'Nagpur', 'Nashik', 'Patna', 'Rajkot', 'Raipur', 'Ranchi', 'Surat', 'Tiruchirappalli', 'Trichy', 'Vadodara', 'Vijayawada', 'Visakhapatnam','Vellore') THEN 'Tier-2'
                    		ELSE 'Tier-3'
                	END AS city_tier,
                	CAST(ZipCode AS STRING) AS Zip_Code,
                	CAST(ActiveFlag AS STRING) AS Active_Flag,
                	TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
                	TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
                	_stg_file_name,
                	_stg_file_load_ts,
                	_stg_file_md5,
                	CURRENT_TIMESTAMP AS _copy_data_ts
        	FROM stage_sch.location_stm
        	) AS source
        	ON target.Location_ID = source.Location_ID
        WHEN MATCHED AND (
            target.City != source.City OR
            target.State != source.State OR
            target.state_code != source.state_code OR
            target.is_union_territory != source.is_union_territory OR
            target.capital_city_flag != source.capital_city_flag OR
            target.city_tier != source.city_tier OR
            target.Zip_Code != source.Zip_Code OR
            target.Active_Flag != source.Active_Flag OR
            target.modified_ts != source.modified_ts
        ) THEN 
            UPDATE SET 
                target.City = source.City,
                target.State = source.State,
                target.state_code = source.state_code,
                target.is_union_territory = source.is_union_territory,
                target.capital_city_flag = source.capital_city_flag,
                target.city_tier = source.city_tier,
                target.Zip_Code = source.Zip_Code,
                target.Active_Flag = source.Active_Flag,
                target.modified_ts = source.modified_ts,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            INSERT (
                Location_ID,
                City,
                State,
                state_code,
                is_union_territory,
                capital_city_flag,
                city_tier,
                Zip_Code,
                Active_Flag,
                created_ts,
                modified_ts,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                source.Location_ID,
                source.City,
                source.State,
                source.state_code,
                source.is_union_territory,
                source.capital_city_flag,
                source.city_tier,
                source.Zip_Code,
                source.Active_Flag,
                source.created_ts,
                source.modified_ts,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                source._copy_data_ts
            );
        
        EXCEPTION WHEN OTHER THEN
                -- Capture the error message
                LET error_message := SQLERRM;
        
                -- Log the error in an error table
                INSERT INTO common.error_log (process_name, error_message, error_time)
                VALUES ('merge_location_into_clean', :error_message, CURRENT_TIMESTAMP);
        
                -- Return the error message
          RETURN ('Error during MERGE location tbl to clean schema: ' || error_message);
    
    END;

   RETURN 'data transferred into clean schema';
END;


-- Below procedure to Load data from clean schema to consumption schema --------

CREATE OR REPLACE PROCEDURE clean_sch.merge_location_to_consumption()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    BEGIN     
        MERGE INTO CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
        USING CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
        ON 
            target.LOCATION_ID = source.LOCATION_ID and 
            target.ACTIVE_FLAG = source.ACTIVE_FLAG
            
        WHEN MATCHED 
            AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
        -- Update the existing record to close its validity period
        UPDATE SET 
            target.EFF_END_DT = CURRENT_TIMESTAMP(),
            target.CURRENT_FLAG = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
        THEN
        -- Insert new record with current data and new effective start date
        INSERT (
            RESTAURANT_LOCATION_HK,
            LOCATION_ID,
            CITY,
            STATE,
            STATE_CODE,
            IS_UNION_TERRITORY,
            CAPITAL_CITY_FLAG,
            CITY_TIER,
            ZIP_CODE,
            ACTIVE_FLAG,
            EFF_START_DT,
            EFF_END_DT,
            CURRENT_FLAG
        )
        VALUES (
            hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
            source.LOCATION_ID,
            source.CITY,
            source.STATE,
            source.STATE_CODE,
            source.IS_UNION_TERRITORY,
            source.CAPITAL_CITY_FLAG,
            source.CITY_TIER,
            source.ZIP_CODE,
            source.ACTIVE_FLAG,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE
        )
        WHEN NOT MATCHED AND 
        source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
        -- Insert new record with current data and new effective start date
        INSERT (
            RESTAURANT_LOCATION_HK,
            LOCATION_ID,
            CITY,
            STATE,
            STATE_CODE,
            IS_UNION_TERRITORY,
            CAPITAL_CITY_FLAG,
            CITY_TIER,
            ZIP_CODE,
            ACTIVE_FLAG,
            EFF_START_DT,
            EFF_END_DT,
            CURRENT_FLAG
        )
        VALUES (
            hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
            source.LOCATION_ID,
            source.CITY,
            source.STATE,
            source.STATE_CODE,
            source.IS_UNION_TERRITORY,
            source.CAPITAL_CITY_FLAG,
            source.CITY_TIER,
            source.ZIP_CODE,
            source.ACTIVE_FLAG,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE
        );
        
    EXCEPTION WHEN OTHER THEN
            -- Capture the error message
            LET error_message := SQLERRM;
    
            -- Log the error in an error table
            INSERT INTO common.error_log (process_name, error_message, error_time)
            VALUES ('merge_location_into_consumption', :error_message, CURRENT_TIMESTAMP);
    
            -- Return the error message
      RETURN 'Error during MERGE location to consumption schema: ' || error_message;
    
    END;

    RETURN 'Data ingestion and transformation completed successfully';

END;
