# Real-Time ETL Pipeline with Python, Kafka, and PostgreSQL

This project demonstrates a robust, scalable, and fault-tolerant real-time ETL (Extract, Transform, Load) pipeline. It consumes streaming data from a Kafka topic, processes it with Python and Pandas, and loads it into a PostgreSQL database with high efficiency and data integrity.

---

## 🚀 Key Features

- **Real-Time Data Ingestion**: The pipeline processes streaming data with low latency, transitioning from traditional batch jobs to a near real-time system.
- **Scalable Architecture**: Utilizes **Apache Kafka** as a message broker to decouple the producer and consumer, enabling horizontal scaling of the consumer service.
- **Data Integrity & Reliability**:
  - **Manual Offset Commits**: Implements manual Kafka offset management to ensure a reliable "at-least-once" message delivery, preventing data loss and minimizing duplicates.
  - **Dead-Letter Queue (DLQ)**: Includes a **DLQ** to handle malformed or unprocessable messages, ensuring the pipeline remains resilient and doesn't crash on bad data.
- **Efficient Database Operations**: Performs **batch inserts** into the PostgreSQL database using `psycopg2.extras.execute_values`, significantly improving data load performance by reducing transaction overhead.

---

## ⚙️ Technologies Used

- **Apache Kafka**: The core message broker for the streaming data pipeline.
- **Python 3.10+**: The programming language used for both the producer and consumer.
- **Pandas**: A powerful library used for data transformation within the consumer.
- **PostgreSQL 15**: The relational database used as the final data destination.
- **Docker & Docker Compose**: Used to easily set up and orchestrate all the required services in a local environment.

---
## 🛠️ How to Run the Project

Follow these steps to get the entire pipeline up and running on your machine.

### Prerequisites

- [**Docker**](https://www.docker.com/) and [**Docker Compose**](https://docs.docker.com/compose/install/) installed.
- [**Python 3.10+**](https://www.python.org/) installed.

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
pip install -r requirements.txt
docker-compose up -d --build
docker exec -it postgres_db psql -U postgres -d etl -f schema.sql
python consumer.py #in one terminal
python producer.py #in another terminal


