DROP DATABASE IF EXISTS Final_prj_group_4;
CREATE DATABASE Final_prj_group_4;
USE Final_prj_group_4;

-- Create table wbs_with_io

DROP TABLE IF EXISTS wbs_with_io;
CREATE TABLE wbs_with_io (
    Full_Name VARCHAR(255),
    Personnel_Number INT,
    Total_Quantity float,
    Val_COArea_Crcy float,
    Period INT,
    WBS_Element VARCHAR(60),
    Object VARCHAR(60),
    Partner_Object VARCHAR(60),
    CO_Object_Name VARCHAR(255),
    Cost_Element_Descr VARCHAR(255),
    Document_Number BIGINT,
    ParActivity VARCHAR(60),
    Fiscal_Year INT,
    Cost_Element VARCHAR(255),
    Functional_Area INT,
    Posting_Date DATE,
    Material VARCHAR(60),
    Material_Description VARCHAR(60),
    Purchasing_Document VARCHAR(60),
    Purchase_Order_Text VARCHAR(255),
    Name_of_Offsetting_Account VARCHAR(60),
    Document_Header_Text VARCHAR(255),
    Name VARCHAR(255),
    Dr_Cr_Indicator CHAR(1)
);

-- Create table cost_center_data

DROP TABLE IF EXISTS cost_center_data;
CREATE TABLE cost_center_data (
    Full_Name VARCHAR(255),
    Personnel_Number INT,
    Total_Quantity FLOAT,
    Val_COArea_Crcy FLOAT,
    Period INT,
    Partner_Object VARCHAR(60),
    Object VARCHAR(60),
    CO_Object_Name VARCHAR(255),
    Cost_Element_Name VARCHAR(255),
    Document_Number BIGINT,
    Activity_Type VARCHAR(60),
    Fiscal_Year INT,
    Cost_Element VARCHAR(60),
    Functional_Area INT,
    Posting_Date DATE,
    User_Name VARCHAR(60)
);

-- Create table project_lists

DROP TABLE IF EXISTS project_lists;
CREATE TABLE project_lists (
    WBS_Element VARCHAR(60),
    Project VARCHAR(60),
    Project_Type VARCHAR(60)
);

