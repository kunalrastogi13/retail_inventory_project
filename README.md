🧾 Project Summary – Retail Analytics ETL & Real-Time Streaming

This project implements a data analytics platform for retail operations that combines historical batch ETL and real-time streaming using Python, PostgreSQL, and Apache Kafka.
The goal is to enable data-driven insights for sales and inventory by integrating automated data pipelines and preparing the foundation for machine learning–based forecasting.

Key Highlights
	•	Batch ETL Pipeline: Extracts and loads historical retail data from global_superstore.csv into PostgreSQL.
	•	Real-Time Streaming: Uses Kafka producers and consumers to ingest live sales data into the same warehouse (retail.fact_sales).
	•	Data Warehouse Design: Follows a dimensional model with staging and retail schemas (customers, products, and sales facts).
	•	Logging & Monitoring: Tracks ETL runs for reliability and data quality assurance.
	•	Scalable Architecture: Ready for integration with dashboards (Power BI/Streamlit) and ML forecasting models.

 Tech Stack

Python • Pandas • SQLAlchemy • psycopg2 • PostgreSQL • Apache Kafka • Docker • Tableau


