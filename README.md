#### CHUBB Capstone Project
## Global Sales & Retail Performance Analytics System

### PATH TO NOTEBOOK IN dags/databricks_dag.py WILL BE DIFFERENT. IT WILL NOT WORK IN IT'S CURRENT STATE AND WILL NEED TO BE CHANGED TO A PATH IN YOUR OWN DATABRICKS ACCOUNT 
---
## Architecture Diagram
<img width="903" height="531" alt="diagram-export-1-5-2026-8_31_21-PM" src="https://github.com/user-attachments/assets/9dc5bb9c-773e-4c1b-a870-c6f2d10e2443" />

## Setup
1. Start the Airflow Docker container: `docker compose up -d`
2. Navigate to `http://localhost:8080/`
3. Sign in with following credentials:
- Username: `airflow`
- Password: `airflow`
4. Go to DAGs and trigger `capstone_databricks_pipeline`
5. In Databricks, navigate to "Job Runs" under "Data Engineering"
6. Wait for execution to complete
7. Open `powerbi_vis.pibx' in Power BI and sign in to Databricks if prompted

## Output
<img width="1448" height="823" alt="image" src="https://github.com/user-attachments/assets/3445bc19-3768-42c9-b930-640d14fe1f2a" />
<img width="1459" height="818" alt="image" src="https://github.com/user-attachments/assets/76707716-3da4-455e-b0c2-c9b6926971a9" />


