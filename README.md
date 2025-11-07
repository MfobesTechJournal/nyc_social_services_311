Tech Stack
--------------------
 Python
 MySQL 
 Azure Blob Storage 
Streamlit 
pandas, SQLAlchemy, azure-storage-blob

---
 Features:
 ---------------------
- SQL Analysis
- Schema creation  
- Aggregations using `GROUP BY`, `HAVING`, and joins  
- CRUD operations (Create, Read, Update, Delete)  
- Subqueries 

ETL Pipeline
- Extract: Load raw data from Azure Blob  
- Transform: Clean and validate records (handle nulls, duplicates)  
- Load: Store processed data in MySQL  
 

 Setup Instructions
---------------------
1. Clone the Repository
```bash
git clone https://github.com/mfobestechjournal/nyc_social_services_311.git
change directory -> nyc_social_services_311
```
```

2. Install Dependencies
```bash
pip install -r requirements.txt
```

3.  Configure Environment Variables
Create a `.env` 
```
AZURE_CONNECTION_STRING=
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DB=nyc_social_services
```

4. Run Database Schema
```bash
python sql/run_schema.py
```

 5. Execute ETL Pipeline
```bash
python sql/azure_to_sql.py
```

 6. Launch Dashboard
```bash
streamlit run 
```

---

Dataset
------------------
Source: NYC Open Data ( Service Requests)  
Description:Public dataset detailing service requests related to social services in NYC.  
Size:>1,000 records  
Format: CSV   

Cloud Integration
-------------------
- Azure Blob Storage: Stores raw and processed data files  
- MySQL on Azure: Centralized storage for analytics  
- db_to_azure.py: Exports database results back to the cloud  

---
