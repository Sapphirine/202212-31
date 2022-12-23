This repository contains the code files for EDA and DAGs for both static data and dynamic data for the project ‘Trending Youtube Videos Analysis’.
 
Static datasets are present at 
- https://www.kaggle.com/datasets/datasnaek/YouTube-new
- https://www.kaggle.com/datasets/datasnaek/YouTube


To set up the DAGs, please follow the steps below
- Create dataset named ‘bda’ in BigQuery
- Create a dataproc cluster
- Update Youtube api keys in the scraping code files
- Update bucket name, project id and dataproc cluster name wherever mentioned in the code files
- Upload all the code files in ‘code’ folder in your bucket flattening the folder hierarchy
- Set up a cloud composer with Airflow enabled and upload both the files under the ‘dag’ folder in the cloud composer’s bucket

The DAGs named ‘bda_static_data’ and ‘bda_yt_data’ should now appear in the Airflow. Run ‘bda_static_data’ first, and only then start the schedule for ‘bda_yt_data’.
