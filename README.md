# Weather Data Pipeline

An end-to-end, modular data pipeline to collect, process, store, and visualize weather data—ideal for real-time or historical analysis.

---

## Project Overview

This pipeline fetches weather data from APIs or IoT devices that is already stored in the Cloude. Then processes and cleans the data through pyspark, and stores it, through Databricks. It supports 3 pipelines: for batch,  batch streaming and near-real-time streaming. 

---

## Project Structure

```
weather-data-pipeline/
├── notebooks/                                                             # Notebooks used for pipelines
│   └── databricks/                                                        
│      └──  batch-streaming/                                               # Components for batch streaming pipeline
│          ├── 01_weather_data_batch_streaming_pipeline_bronze.ipynb       # Create and feeds bronze table              
│          ├── 02_weather_data_batch_streaming_pipeline_silver.ipynb       # Create and feeds silver table                   
│          └── 03_weather_data_batch_streaming_pipeline_gold.ipynb         # Create and feeds gold table               
│      └──  batch/                                                         # Components for batch streaming pipeline
│          ├── 01_weather_data_batch_pipeline_bronze.ipynb                 # Create and feeds bronze table              
│          ├── 02_weather_data_batch_pipeline_silver.ipynb                 # Create and feeds silver table                   
│          └── 03_weather_data_batch_pipeline_gold.ipynb                   # Create and feeds gold table    
│      └──  batch-streaming/                                               # Components for batch streaming pipeline
│          ├── 01_weather_data_streaming_pipeline_bronze.ipynb             # Create and feeds bronze table              
│          ├── 02_weather_data_streaming_pipeline_silver.ipynb             # Create and feeds silver table                   
│          └── 03_weather_data_streaming_pipeline_gold.ipynb               # Create and feeds gold table    
├── src/                                                                   # Auxiliary code
│   └── databricks/                                                        
│       ├── copy_into_health_check.py                                      # Health check for COPY INTO operation
│       ├── reload_locations.py                                            # Load of location master tabe          
└── README.md
```

---

## Steps
- Ingest data from the cloud provider into gold bronze table (e.g., AWS S3)
- Clean and standardize data (e.g., temperature units, timestamps) into silver table.
- Create aggregations into the gold table
- Afterwards data can be used for Analysis and Visualization
- (Optional but recommended) Create a workflow in databricks to run the notebooks and auxiliary scrips as a one-time or continous job