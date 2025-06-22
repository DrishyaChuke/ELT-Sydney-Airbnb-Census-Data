CREATE SCHEMA IF NOT EXISTS bronze;

-- Airbnb Listings Data 

CREATE TABLE IF NOT EXISTS bronze.airbnb_listings (
    LISTING_ID BIGINT,
    SCRAPE_ID BIGINT,
    SCRAPED_DATE DATE,
    HOST_ID BIGINT,
    HOST_NAME TEXT,
    HOST_SINCE DATE,
    HOST_IS_SUPERHOST TEXT,
    HOST_NEIGHBOURHOOD TEXT,
    LISTING_NEIGHBOURHOOD TEXT,
    PROPERTY_TYPE TEXT,
    ROOM_TYPE TEXT,
    ACCOMMODATES INTEGER,
    PRICE NUMERIC,
    HAS_AVAILABILITY TEXT,
    AVAILABILITY_30 INTEGER,
    NUMBER_OF_REVIEWS INTEGER,
    REVIEW_SCORES_RATING NUMERIC,
    REVIEW_SCORES_ACCURACY NUMERIC,
    REVIEW_SCORES_CLEANLINESS NUMERIC,
    REVIEW_SCORES_CHECKIN NUMERIC,
    REVIEW_SCORES_COMMUNICATION NUMERIC,
    REVIEW_SCORES_VALUE NUMERIC
);

-- Census Data (G01)

CREATE TABLE bronze.census_g01 (
    LGA_CODE_2016 TEXT,
    Tot_P_M INTEGER,
    Tot_P_F INTEGER,
    Tot_P_P INTEGER,
    Age_0_4_yr_M INTEGER,
    Age_0_4_yr_F INTEGER,
    Age_0_4_yr_P INTEGER,
    Age_5_14_yr_M INTEGER,
    Age_5_14_yr_F INTEGER,
    Age_5_14_yr_P INTEGER,
    Age_15_19_yr_M INTEGER,
    Age_15_19_yr_F INTEGER,
    Age_15_19_yr_P INTEGER,
    Age_20_24_yr_M INTEGER,
    Age_20_24_yr_F INTEGER,
    Age_20_24_yr_P INTEGER,
    Age_25_34_yr_M INTEGER,
    Age_25_34_yr_F INTEGER,
    Age_25_34_yr_P INTEGER,
    Age_35_44_yr_M INTEGER,
    Age_35_44_yr_F INTEGER,
    Age_35_44_yr_P INTEGER,
    Age_45_54_yr_M INTEGER,
    Age_45_54_yr_F INTEGER,
    Age_45_54_yr_P INTEGER,
    Age_55_64_yr_M INTEGER,
    Age_55_64_yr_F INTEGER,
    Age_55_64_yr_P INTEGER,
    Age_65_74_yr_M INTEGER,
    Age_65_74_yr_F INTEGER,
    Age_65_74_yr_P INTEGER,
    Age_75_84_yr_M INTEGER,
    Age_75_84_yr_F INTEGER,
    Age_75_84_yr_P INTEGER,
    Age_85ov_M INTEGER,
    Age_85ov_F INTEGER,
    Age_85ov_P INTEGER,
    Counted_Census_Night_home_M INTEGER,
    Counted_Census_Night_home_F INTEGER,
    Counted_Census_Night_home_P INTEGER,
    Count_Census_Nt_Ewhere_Aust_M INTEGER,
    Count_Census_Nt_Ewhere_Aust_F INTEGER,
    Count_Census_Nt_Ewhere_Aust_P INTEGER,
    Indigenous_psns_Aboriginal_M INTEGER,
    Indigenous_psns_Aboriginal_F INTEGER,
    Indigenous_psns_Aboriginal_P INTEGER,
    Indig_psns_Torres_Strait_Is_M INTEGER,
    Indig_psns_Torres_Strait_Is_F INTEGER,
    Indig_psns_Torres_Strait_Is_P INTEGER,
    Indig_Bth_Abor_Torres_St_Is_M INTEGER,
    Indig_Bth_Abor_Torres_St_Is_F INTEGER,
    Indig_Bth_Abor_Torres_St_Is_P INTEGER,
    Indigenous_P_Tot_M INTEGER,
    Indigenous_P_Tot_F INTEGER,
    Indigenous_P_Tot_P INTEGER,
    Birthplace_Australia_M INTEGER,
    Birthplace_Australia_F INTEGER,
    Birthplace_Australia_P INTEGER,
    Birthplace_Elsewhere_M INTEGER,
    Birthplace_Elsewhere_F INTEGER,
    Birthplace_Elsewhere_P INTEGER,
    Lang_spoken_home_Eng_only_M INTEGER,
    Lang_spoken_home_Eng_only_F INTEGER,
    Lang_spoken_home_Eng_only_P INTEGER,
    Lang_spoken_home_Oth_Lang_M INTEGER,
    Lang_spoken_home_Oth_Lang_F INTEGER,
    Lang_spoken_home_Oth_Lang_P INTEGER,
    Australian_citizen_M INTEGER,
    Australian_citizen_F INTEGER,
    Australian_citizen_P INTEGER,
    Age_psns_att_educ_inst_0_4_M INTEGER,
    Age_psns_att_educ_inst_0_4_F INTEGER,
    Age_psns_att_educ_inst_0_4_P INTEGER,
    Age_psns_att_educ_inst_5_14_M INTEGER,
    Age_psns_att_educ_inst_5_14_F INTEGER,
    Age_psns_att_educ_inst_5_14_P INTEGER,
    Age_psns_att_edu_inst_15_19_M INTEGER,
    Age_psns_att_edu_inst_15_19_F INTEGER,
    Age_psns_att_edu_inst_15_19_P INTEGER,
    Age_psns_att_edu_inst_20_24_M INTEGER,
    Age_psns_att_edu_inst_20_24_F INTEGER,
    Age_psns_att_edu_inst_20_24_P INTEGER,
    Age_psns_att_edu_inst_25_ov_M INTEGER,
    Age_psns_att_edu_inst_25_ov_F INTEGER,
    Age_psns_att_edu_inst_25_ov_P INTEGER,
    High_yr_schl_comp_Yr_12_eq_M INTEGER,
    High_yr_schl_comp_Yr_12_eq_F INTEGER,
    High_yr_schl_comp_Yr_12_eq_P INTEGER,
    High_yr_schl_comp_Yr_11_eq_M INTEGER,
    High_yr_schl_comp_Yr_11_eq_F INTEGER,
    High_yr_schl_comp_Yr_11_eq_P INTEGER,
    High_yr_schl_comp_Yr_10_eq_M INTEGER,
    High_yr_schl_comp_Yr_10_eq_F INTEGER,
    High_yr_schl_comp_Yr_10_eq_P INTEGER,
    High_yr_schl_comp_Yr_9_eq_M INTEGER,
    High_yr_schl_comp_Yr_9_eq_F INTEGER,
    High_yr_schl_comp_Yr_9_eq_P INTEGER,
    High_yr_schl_comp_Yr_8_belw_M INTEGER,
    High_yr_schl_comp_Yr_8_belw_F INTEGER,
    High_yr_schl_comp_Yr_8_belw_P INTEGER,
    High_yr_schl_comp_D_n_g_sch_M INTEGER,
    High_yr_schl_comp_D_n_g_sch_F INTEGER,
    High_yr_schl_comp_D_n_g_sch_P INTEGER,
    Count_psns_occ_priv_dwgs_M INTEGER,
    Count_psns_occ_priv_dwgs_F INTEGER,
    Count_psns_occ_priv_dwgs_P INTEGER,
    Count_Persons_other_dwgs_M INTEGER,
    Count_Persons_other_dwgs_F INTEGER,
    Count_Persons_other_dwgs_P INTEGER
);

-- Census Data (G02)

CREATE table IF NOT EXISTS bronze.census_g02 (
    LGA_CODE_2016 TEXT,
    Median_age_persons INTEGER,
    Median_mortgage_repay_monthly INTEGER,
    Median_tot_prsnl_inc_weekly INTEGER,
    Median_rent_weekly INTEGER,
    Median_tot_fam_inc_weekly INTEGER,
    Average_num_psns_per_bedroom INTEGER,
    Median_tot_hhd_inc_weekly INTEGER,
    Average_household_size INTEGER
);


-- LGA Subhurb data

CREATE TABLE bronze.lga_codes (
    LGA_CODE VARCHAR(50),
    LGA_NAME VARCHAR(255)
);


-- LGA code data

CREATE TABLE bronze.lga_suburb (
	LGA_NAME VARCHAR(255),
    SUBURB_NAME VARCHAR(255)  
);

