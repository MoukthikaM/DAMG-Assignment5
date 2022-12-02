# DAMG-Assignment5:PREDICTING CUSTOMER SPEND - REGRESSION USING SNOWFLAKE:

Codelabs Document link : https://codelabs-preview.appspot.com/?file_id=100uwraioKrKJntZWNlOcgEOgk9j_7RplUaPgZbZamRk#3 

## REQUIREMENTS:

1. Create a Snowflake Trial Account

2. Snowpark for Python

This use case uses the following libraries:

	scikit-learn
	pandas
	numpy
	matplotlib
	seaborn
	Streamlit

Optional: Docker runtime environment such as Docker Desktop will be used for running and managing Apache Airflow DAGs.

## SETUP STEPS: 
 
1. Clone the repository and create the environment using the following command :

		 conda env create -f environment.yml
 
2. Launch Jupyter Lab, once jupyter lab is up and running update the creds.json to reflect to your snowflake environment

3. To run streamlit on your terminal, run :

	 	streamlit run streamlit.py

   Link for Streamlit: https://moukthikam-damg-assignment5-streamlit-7r83od.streamlit.app 

4. Run airflow using astro cli :  

## CONTRIBUTIONS:

1. Moukthika Manapati :

- Airflow, Astro

- Docker

- Streamlit

- Model build and train

2. Adhrushta Arashanapalli :

- Data Preprocessing

- ELT pipeline

- functions for Bulk load and incremental ingest

- Documentation 
