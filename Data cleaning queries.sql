USE Final_prj_group_4;

-- Adding new column for creating unique identifier
SET SQL_SAFE_UPDATES = 0;

ALTER TABLE cost_center_data
ADD COLUMN object_doc_no VARCHAR(255);
UPDATE cost_center_data
SET object_doc_no = CONCAT(Object, Document_Number);

-- Adding new column for creating unique identifier
ALTER TABLE wbs_with_io
ADD COLUMN partner_object_doc_no VARCHAR(255);
UPDATE wbs_with_io
SET partner_object_doc_no = CONCAT(Partner_Object, Document_Number);

-- Adding new column WBS element
ALTER TABLE cost_center_data
ADD COLUMN `WBS_Element` VARCHAR(60) AFTER Period;
UPDATE cost_center_data
SET `WBS_Element` = `Partner_object`;

-- Step 1: Create the 'cc_deletes' table with the same structure as 'cost_center_data'
CREATE TABLE cc_deletes AS
SELECT * FROM cost_center_data
WHERE 1=0; -- Creates an empty table with the same structure

-- Step 2: Insert unique 'object_doc_no' rows
INSERT INTO cc_deletes
SELECT ccd.*
FROM cost_center_data ccd
LEFT JOIN wbs_with_io wio ON ccd.object_doc_no = wio.partner_object_doc_no
WHERE wio.Object IS NULL;

-- Create table name wbs_cc

CREATE TABLE wbs_cc AS
SELECT 
    Full_Name,
    Personnel_Number,
    Total_Quantity,
    Val_COArea_Crcy,
    Period,
    WBS_Element,
    Object,
    Partner_Object,
    CO_Object_Name,
    Cost_Element_Descr,
    Document_Number,
    ParActivity,
    Fiscal_Year,
    Cost_Element,
    Functional_Area,
    Posting_Date
FROM 
    wbs_with_io

UNION ALL

SELECT 
    Full_Name,
    Personnel_Number,
    Total_Quantity,
    Val_COArea_Crcy,
    Period,
    WBS_Element,
    Partner_Object AS Object,  -- Aligning 'Partner_Object' with 'Object'
    Partner_Object,
    CO_Object_Name,
    Cost_Element_Name AS Cost_Element_Descr,  -- Aligning 'Cost_Element_Name' with 'Cost_Element_Descr'
    Document_Number,
    Activity_Type AS ParActivity,  -- Aligning 'Activity_Type' with 'ParActivity'
    Fiscal_Year,
    Cost_Element,
    Functional_Area,
    Posting_Date
FROM 
    cc_deletes;

-- Final Clean up of the data 

SELECT *
FROM wbs_cc
WHERE Cost_Element_descr NOT IN ('Settle IO-MS(NP)', 'Settle IO-REM.COS (NP)', 'Settle IO-RD(NP)', 'Settle IO-REM COS')
AND Object NOT LIKE '9%'
AND Partner_Object NOT LIKE 'PA15%'
AND Cost_Element_descr <> 'Internal Labor';



