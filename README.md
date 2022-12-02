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
		 
		 conda activate snowpark (from the env file)
 
2. Launch Jupyter Lab, once jupyter lab is up and running update the creds.json to reflect to your snowflake environment and use the conda environment to run the notebooks.

3. To run streamlit on your terminal, run :

	 	streamlit run streamlit.py

  - This is the link for the streamlit hoseted in streamlit cloud. Link for Streamlit: https://moukthikam-damg-assignment5-streamlit-7r83od.streamlit.app 

4. Run airflow using astro cli :  
  
  - Install Astro CLI using https://docs.astronomer.io/astro/cli/install-cli
  
  - You can initialise the astro using astro dev init but as the environemnt and folders are already set we can run the below commands to run the airflow:
     		
		astro dev start
      		
		astro dev stop
   
  - If you make any changes, we can run: 
		
		astro dev restart.

  - Link for Airflow Instance : http://ec2-35-172-118-249.compute-1.amazonaws.com/dags/customerspend_incremental_setup_taskflow/graph   

  - Credentials for airflow : username -admin , password -admin
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
