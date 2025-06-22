# ELT-Sydney-Airbnb-Census-Data
This project focuses on building a production-ready ELT data pipeline for Airbnb and Census data specific to Sydney. Using Apache Airflow, dbt Cloud, and a GCP-hosted Postgres database, the pipeline processes, transforms, and loads data into a structured warehouse. This enables eEicient data retrieval and insights generation for key business queries.
# Objectives
1. DataIngestionandProcessing:LoadandtransformrawAirbnbandCensusdata into a structured medallion architecture (Bronze, Silver, Gold).
2. DataMartCreation:Developanalyticalviewsforneighborhoodandproperty-type insights.
3. Business Insights: Answer key questions using SQL queries on demographic and revenue patterns, supporting strategic decision-making.
# Tools and Techniques
• Airflow: Orchestrates data ingestion and transformation.
• dbt: Manages data transformations and schema definitions for eEicient data
warehousing.
• Postgres: Stores and organizes transformed data into structured schemas,
enhancing data accessibility.
# Dataset Overview
The analysis uses two key datasets: 
1. AirbnbListings
Collected from May 2020 to April 2021, the Airbnb dataset represents Sydney’s short- term rental market. It includes property listings, rental prices, and details of host- guest interactions, providing insights into rental supply, demand, density, price variations, and host behavior across neighborhoods.
2. AustralianCensus
The 2016 Census data, conducted by the Australian Bureau of Statistics, oEers comprehensive demographic and housing information, including age, income, education, and dwelling types. With geographic identifiers like LGAs and suburbs, it enables a detailed look at Sydney's population distribution and demographic characteristics, allowing analysis of factors that may impact Airbnb listing trends.
Together, these datasets provide a detailed view of Sydney’s rental market and demographics, supporting analysis of the relationship between short-term rental activity and demographic indicators.
