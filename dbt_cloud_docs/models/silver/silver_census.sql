{{ config(
    schema="silver",
    materialized="table"
) }}


WITH cleaned_census_g01 AS (
    SELECT
        TRIM(REPLACE("lga_code_2016", 'LGA', '')) AS lga_code,
        CAST("tot_p_m" AS BIGINT) AS total_population_male,
        CAST("tot_p_f" AS BIGINT) AS total_population_female,
        CAST("tot_p_p" AS BIGINT) AS total_population,
        CAST("age_0_4_yr_m" AS BIGINT) AS age_0_4_male,
        CAST("age_0_4_yr_f" AS BIGINT) AS age_0_4_female,
        CAST("age_0_4_yr_p" AS BIGINT) AS age_0_4_total,
        CAST("age_5_14_yr_m" AS BIGINT) AS age_5_14_male,
        CAST("age_5_14_yr_f" AS BIGINT) AS age_5_14_female,
        CAST("age_5_14_yr_p" AS BIGINT) AS age_5_14_total,
        CAST("age_15_19_yr_m" AS BIGINT) AS age_15_19_male,
        CAST("age_15_19_yr_f" AS BIGINT) AS age_15_19_female,
        CAST("age_15_19_yr_p" AS BIGINT) AS age_15_19_total,
        CAST("age_20_24_yr_m" AS BIGINT) AS age_20_24_male,
        CAST("age_20_24_yr_f" AS BIGINT) AS age_20_24_female,
        CAST("age_20_24_yr_p" AS BIGINT) AS age_20_24_total,
        CAST("age_25_34_yr_m" AS BIGINT) AS age_25_34_male,
        CAST("age_25_34_yr_f" AS BIGINT) AS age_25_34_female,
        CAST("age_25_34_yr_p" AS BIGINT) AS age_25_34_total,
        CAST("age_35_44_yr_m" AS BIGINT) AS age_35_44_male,
        CAST("age_35_44_yr_f" AS BIGINT) AS age_35_44_female,
        CAST("age_35_44_yr_p" AS BIGINT) AS age_35_44_total,
        CAST("age_45_54_yr_m" AS BIGINT) AS age_45_54_male,
        CAST("age_45_54_yr_f" AS BIGINT) AS age_45_54_female,
        CAST("age_45_54_yr_p" AS BIGINT) AS age_45_54_total,
        CAST("age_55_64_yr_m" AS BIGINT) AS age_55_64_male,
        CAST("age_55_64_yr_f" AS BIGINT) AS age_55_64_female,
        CAST("age_55_64_yr_p" AS BIGINT) AS age_55_64_total,
        CAST("age_65_74_yr_m" AS BIGINT) AS age_65_74_male,
        CAST("age_65_74_yr_f" AS BIGINT) AS age_65_74_female,
        CAST("age_65_74_yr_p" AS BIGINT) AS age_65_74_total,
        CAST("age_75_84_yr_m" AS BIGINT) AS age_75_84_male,
        CAST("age_75_84_yr_f" AS BIGINT) AS age_75_84_female,
        CAST("age_75_84_yr_p" AS BIGINT) AS age_75_84_total,
        CAST("age_85ov_m" AS BIGINT) AS age_85_plus_male,
        CAST("age_85ov_f" AS BIGINT) AS age_85_plus_female,
        CAST("age_85ov_p" AS BIGINT) AS age_85_plus_total,
        CAST("counted_census_night_home_m" AS BIGINT) AS counted_census_night_home_male,
        CAST("counted_census_night_home_f" AS BIGINT) AS counted_census_night_home_female,
        CAST("counted_census_night_home_p" AS BIGINT) AS counted_census_night_home_total,
        CAST("count_census_nt_ewhere_aust_m" AS BIGINT) AS count_census_elsewhere_australia_male,
        CAST("count_census_nt_ewhere_aust_f" AS BIGINT) AS count_census_elsewhere_australia_female,
        CAST("count_census_nt_ewhere_aust_p" AS BIGINT) AS count_census_elsewhere_australia_total,
        CAST("indigenous_psns_aboriginal_m" AS BIGINT) AS indigenous_aboriginal_male,
        CAST("indigenous_psns_aboriginal_f" AS BIGINT) AS indigenous_aboriginal_female,
        CAST("indigenous_psns_aboriginal_p" AS BIGINT) AS indigenous_aboriginal_total,
        CAST("indig_psns_torres_strait_is_m" AS BIGINT) AS indigenous_torres_strait_islander_male,
        CAST("indig_psns_torres_strait_is_f" AS BIGINT) AS indigenous_torres_strait_islander_female,
        CAST("indig_psns_torres_strait_is_p" AS BIGINT) AS indigenous_torres_strait_islander_total,
        CAST("indig_bth_abor_torres_st_is_m" AS BIGINT) AS indigenous_both_aboriginal_tsi_male,
        CAST("indig_bth_abor_torres_st_is_f" AS BIGINT) AS indigenous_both_aboriginal_tsi_female,
        CAST("indig_bth_abor_torres_st_is_p" AS BIGINT) AS indigenous_both_aboriginal_tsi_total,
        CAST("indigenous_p_tot_m" AS BIGINT) AS indigenous_population_total_male,
        CAST("indigenous_p_tot_f" AS BIGINT) AS indigenous_population_total_female,
        CAST("indigenous_p_tot_p" AS BIGINT) AS indigenous_population_total,
        CAST("birthplace_australia_m" AS BIGINT) AS birthplace_australia_male,
        CAST("birthplace_australia_f" AS BIGINT) AS birthplace_australia_female,
        CAST("birthplace_australia_p" AS BIGINT) AS birthplace_australia_total,
        CAST("birthplace_elsewhere_m" AS BIGINT) AS birthplace_elsewhere_male,
        CAST("birthplace_elsewhere_f" AS BIGINT) AS birthplace_elsewhere_female,
        CAST("birthplace_elsewhere_p" AS BIGINT) AS birthplace_elsewhere_total,
        CAST("lang_spoken_home_eng_only_m" AS BIGINT) AS lang_spoken_home_english_only_male,
        CAST("lang_spoken_home_eng_only_f" AS BIGINT) AS lang_spoken_home_english_only_female,
        CAST("lang_spoken_home_eng_only_p" AS BIGINT) AS lang_spoken_home_english_only_total,
        CAST("lang_spoken_home_oth_lang_m" AS BIGINT) AS lang_spoken_home_other_lang_male,
        CAST("lang_spoken_home_oth_lang_f" AS BIGINT) AS lang_spoken_home_other_lang_female,
        CAST("lang_spoken_home_oth_lang_p" AS BIGINT) AS lang_spoken_home_other_lang_total,
        CAST("australian_citizen_m" AS BIGINT) AS australian_citizen_male,
        CAST("australian_citizen_f" AS BIGINT) AS australian_citizen_female,
        CAST("australian_citizen_p" AS BIGINT) AS australian_citizen_total,
        CAST("count_persons_other_dwgs_p" AS BIGINT) AS count_persons_other_dwgs_total
    FROM {{ ref('census_g01_bronze') }}
    WHERE "lga_code_2016" IS NOT NULL
),

cleaned_census_g02 AS (
    SELECT
        TRIM(REPLACE("lga_code_2016", 'LGA', '')) AS lga_code, 
        CAST("median_age_persons" AS BIGINT) AS median_age_persons,
        CAST("median_mortgage_repay_monthly" AS BIGINT) AS median_mortgage_repay_monthly,
        CAST("median_tot_prsnl_inc_weekly" AS BIGINT) AS median_total_personal_income_weekly,
        CAST("median_rent_weekly" AS BIGINT) AS median_rent_weekly,
        CAST("median_tot_fam_inc_weekly" AS BIGINT) AS median_total_family_income_weekly,
        CAST("average_num_psns_per_bedroom" AS DOUBLE PRECISION) AS average_persons_per_bedroom,
        CAST("median_tot_hhd_inc_weekly" AS BIGINT) AS median_total_household_income_weekly,
        CAST("average_household_size" AS DOUBLE PRECISION) AS average_household_size
    FROM {{ ref('census_g02_bronze') }}
    WHERE "lga_code_2016" IS NOT NULL
)

SELECT
    g01.*,
    g02.median_age_persons,
    g02.median_mortgage_repay_monthly,
    g02.median_total_personal_income_weekly,
    g02.median_rent_weekly,
    g02.median_total_family_income_weekly,
    g02.average_persons_per_bedroom,
    g02.median_total_household_income_weekly,
    g02.average_household_size
FROM cleaned_census_g01 AS g01
LEFT JOIN cleaned_census_g02 AS g02
ON g01.lga_code = g02.lga_code