Retail Inventory Forecasting System

End-to-end Retail Data Engineering + Forecasting + Real-Time Streaming Platform

This project is a complete inventory analytics system built using modern data engineering and machine learning tools.
It processes batch and real-time data, stores it in a clean warehouse, runs demand forecasting models, and provides business dashboards for actionable insights.

It solves a real industry problem:
“How can retailers avoid stockouts, understand product demand, and plan inventory better?”

                +-----------------------+
                |       CSV Source      |
                |  (Global Superstore)  |
                +-----------+-----------+
                            |
                    Batch ETL (Python)
                            |
                    +-------v--------+
                    |   Postgres DB  |
                    |  Star Schema   |
                    +---+------+-----+
                        |      |
                Demand Forecast | Business Dashboards
                        |      |
                    Prophet/MA  |     Metabase UI
                        |      |
             +----------v------+-----------+
             |   Forecast vs Actual         |
             |   Sales Overview             |
             |   Product Demand Trends      |
             |   Inventory Health           |
             +-----------------------------+
                        |
         +-----------------------------------+
         |      Kafka Real-Time Streams      |
         | (sales events, inventory events)  |
         +-----------------------------------+

Tech Stack

Data Engineering
	•	Python
	•	Pandas
	•	SQLAlchemy
	•	Docker
	•	Kafka (Producers + Consumers)
	•	PostgreSQL

Machine Learning
	•	Prophet
	•	Scikit-learn
	•	Feature engineering (rolling averages, lag features)

BI & Dashboards
	•	Metabase

Future Roadmap (Already Architected)

Next steps to convert this into a production-grade system:

1. ML-based Stock-Out Prediction Model
Uses engineered features like:
	•	rolling means
	•	lag sales
	•	stock velocity
	•	demand seasonality

2. API Layer (FastAPI) for real-time predictions

3. Airflow orchestration
Manage:
	•	ETL
	•	model retraining
	•	dashboard refresh

4. CI/CD pipeline for data and ML

5. Integration with Snowflake / BigQuery

⸻

Team & Contribution

This project was built as a collaborative system covering:
	•	Data engineering
	•	Data modeling
	•	Machine learning
	•	Dashboarding
	•	Real-time streaming

Open for contributions and improvements!