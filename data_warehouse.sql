CREATE SCHEMA data_warehouse;
USE data_warehouse;

-- Create Dimension tables

CREATE TABLE dim_personnel (
    personnel_id INT PRIMARY KEY,
    full_name VARCHAR(255)
);

CREATE TABLE dim_prj (
    wbs_element VARCHAR(255) PRIMARY KEY,
    project VARCHAR(255),
    project_type VARCHAR(255)
);


-- fact table

CREATE TABLE fact_fin_data (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    personnel_id INT,
    wbs_element VARCHAR(255),
    total_quantity DECIMAL(10, 2),
    val_coarea_crcy DECIMAL(10, 2),
    period INT,
    fiscal_year INT,
    posting_date DATE,
    FOREIGN KEY (personnel_id) REFERENCES dim_personnel(personnel_id),
    FOREIGN KEY (wbs_element) REFERENCES dim_prj(wbs_element)
);

-- insert data in dim tables

INSERT INTO dim_personnel (personnel_id, full_name)
SELECT personnel_id, full_name
FROM final_prj_group_4.personnel;

INSERT INTO dim_prj (wbs_element, project, project_type)
SELECT wbs_element, project, project_type
FROM final_prj_group_4.project_lists;



-- insert data in Fact table
INSERT INTO fact_fin_data (
    personnel_id, wbs_element, total_quantity, val_coarea_crcy, 
    period, fiscal_year, posting_date
)
SELECT 
    dp.personnel_id, dpj.wbs_element, 
    fcd.Total_Quantity, fcd.Val_COArea_Crcy, 
    fcd.Period, fcd.Fiscal_Year, fcd.Posting_Date
FROM 
    final_prj_group_4.final_clean_data fcd
    LEFT JOIN dim_personnel dp ON fcd.Personnel_Number = dp.personnel_id
    LEFT JOIN dim_prj dpj ON fcd.WBS_Element = dpj.wbs_element;


