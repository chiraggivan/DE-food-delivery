-- to load data from stage_sch to clean_sch once new file is loaded in @stage and above task is completed 
CREATE OR REPLACE TASK common.merge_location_to_clean_task 
WAREHOUSE = compute_WH
WHEN SYSTEM$STREAM_HAS_DATA('stage_sch.location_stm')
AS
CALL stage_sch.merge_location_to_clean();


-- to load data from clean_sch to consumption_sch once above task is completed 
CREATE OR REPLACE TASK common.merge_location_to_consumption_task
WAREHOUSE = compute_WH
WHEN SYSTEM$STREAM_HAS_DATA('clean_sch.restaurant_location_stm')
AS
CALL clean_sch.merge_location_to_consumption();

ALTER TASK common.new_location_task RESUME; -- need to resume task once created. 
ALTER TASK common.merge_location_to_clean_task RESUME; -- need to resume task once created
ALTER TASK common.merge_location_to_consumption_task RESUME; -- need to resume task once created
