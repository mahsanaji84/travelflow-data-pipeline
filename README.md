<h1>TravelFlow Data Pipeline</h1>

<p>
  <a href="https://github.com/mahsanaji84/travelflow-data-pipeline/actions/workflows/ci.yml">
    <img src="https://github.com/mahsanaji84/travelflow-data-pipeline/actions/workflows/ci.yml/badge.svg" alt="TravelFlow CI">
  </a>
</p>

<p>
  TravelFlow is an end-to-end <strong>data engineering and DevOps portfolio project</strong>
  that demonstrates both <strong>real-time streaming</strong> and
  <strong>batch processing</strong> workflows using Apache Kafka, Apache Spark,
  Apache Cassandra, Apache Airflow, PostgreSQL, Docker Compose, Python, Pandas,
  and GitHub Actions.
</p>

<hr>

<h2>Highlights</h2>

<ul>
  <li>Real-time event ingestion with <strong>Apache Kafka</strong></li>
  <li>Stream processing with <strong>Spark Structured Streaming</strong></li>
  <li>Batch orchestration with <strong>Apache Airflow</strong></li>
  <li>Historical data analysis with <strong>Pandas</strong></li>
  <li>NoSQL persistence with <strong>Apache Cassandra</strong></li>
  <li>Airflow metadata storage with <strong>PostgreSQL</strong></li>
  <li>Local multi-service deployment with <strong>Docker Compose</strong></li>
  <li>Continuous integration with <strong>GitHub Actions</strong></li>
</ul>

<hr>

<h2>Architecture</h2>

<h3>Real-time pipeline</h3>

<pre>
Python Simulator
      ↓
Apache Kafka
      ↓
Spark Structured Streaming
      ↓
Apache Cassandra
</pre>

<h3>Batch pipeline</h3>

<pre>
reservations.csv
      ↓
Apache Airflow
      ↓
Python / Pandas
      ↓
Apache Cassandra

Airflow metadata
      ↓
PostgreSQL
</pre>

<hr>

<h2>Technology Stack</h2>

<table>
  <tr>
    <th>Technology</th>
    <th>Role</th>
  </tr>
  <tr>
    <td>Python</td>
    <td>Event simulation and batch logic</td>
  </tr>
  <tr>
    <td>Apache Kafka 4.3.1</td>
    <td>Real-time event ingestion</td>
  </tr>
  <tr>
    <td>Apache Spark 3.5.1</td>
    <td>Structured Streaming and aggregation</td>
  </tr>
  <tr>
    <td>Apache Cassandra 5.x</td>
    <td>NoSQL storage</td>
  </tr>
  <tr>
    <td>Apache Airflow 2.10.5</td>
    <td>Batch orchestration</td>
  </tr>
  <tr>
    <td>PostgreSQL 16</td>
    <td>Airflow metadata database</td>
  </tr>
  <tr>
    <td>Pandas</td>
    <td>Batch data analysis</td>
  </tr>
  <tr>
    <td>Docker Compose</td>
    <td>Local service orchestration</td>
  </tr>
  <tr>
    <td>GitHub Actions</td>
    <td>Continuous integration</td>
  </tr>
</table>

<hr>

<h2>Project Structure</h2>

<pre>
travelflow-data-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   └── travelflow_batch_dag.py
│   └── logs/
├── data/
│   └── reservations.csv
├── simulator/
│   └── generate_events.py
├── spark/
│   ├── streaming_consumer.py
│   ├── streaming_to_cassandra.py
│   └── test_cassandra_write.py
├── .gitignore
├── docker-compose.yml
└── README.md
</pre>

<hr>

<h2>Real-Time Pipeline</h2>

<h3>1. Event Simulation</h3>

<p>The Python simulator generates synthetic TravelFlow user activity such as:</p>

<ul>
  <li><code>view_destination</code></li>
  <li><code>view_package</code></li>
  <li><code>start_booking</code></li>
  <li><code>booking_abandoned</code></li>
  <li><code>booking_completed</code></li>
</ul>

<h3>2. Kafka Ingestion</h3>

<p>Events are published to:</p>

<pre>travelflow-events</pre>

<p>Kafka listeners:</p>

<pre>
Windows host: localhost:9092
Docker network: kafka:29092
</pre>

<h3>3. Spark Structured Streaming</h3>

<p>
  Spark consumes the Kafka topic, parses the JSON payload, and aggregates events by destination.
</p>

<pre>
spark.sql.shuffle.partitions = 5
</pre>

<h3>4. Cassandra Real-Time Storage</h3>

<pre>
CREATE TABLE destination_stats (
    destination text PRIMARY KEY,
    event_count bigint
);
</pre>

<h4>Validated streaming result</h4>

<table>
  <tr>
    <th>Destination</th>
    <th>Event Count</th>
  </tr>
  <tr><td>New York</td><td>29</td></tr>
  <tr><td>Montreal</td><td>23</td></tr>
  <tr><td>Rome</td><td>20</td></tr>
  <tr><td>Paris</td><td>15</td></tr>
  <tr><td>Cancun</td><td>23</td></tr>
</table>

<hr>

<h2>Batch Pipeline</h2>

<h3>1. Historical Data Source</h3>

<pre>data/reservations.csv</pre>

<p>The dataset contains 20 sample reservations.</p>

<h3>2. Airflow DAG</h3>

<p><strong>DAG:</strong></p>

<pre>travelflow_batch_analysis</pre>

<p><strong>Task:</strong></p>

<pre>analyse_reservations</pre>

<p><strong>Schedule:</strong></p>

<pre>@daily</pre>

<p>The DAG performs the following steps:</p>

<ol>
  <li>Reads the CSV file with Pandas</li>
  <li>Groups reservations by destination</li>
  <li>Calculates reservation KPIs</li>
  <li>Connects to Cassandra</li>
  <li>Writes the aggregated results</li>
</ol>

<h3>3. Batch KPIs</h3>

<ul>
  <li>Total reservations</li>
  <li>Completed reservations</li>
  <li>Abandoned reservations</li>
  <li>Total revenue from completed reservations</li>
</ul>

<h3>4. Cassandra Batch Storage</h3>

<pre>
CREATE TABLE batch_booking_stats (
    destination text PRIMARY KEY,
    total_reservations int,
    completed_reservations int,
    abandoned_reservations int,
    total_revenue double
);
</pre>

<h4>Validated batch result</h4>

<table>
  <tr>
    <th>Destination</th>
    <th>Total</th>
    <th>Completed</th>
    <th>Abandoned</th>
    <th>Revenue</th>
  </tr>
  <tr><td>New York</td><td>4</td><td>3</td><td>1</td><td>5400</td></tr>
  <tr><td>Montreal</td><td>4</td><td>3</td><td>1</td><td>2600</td></tr>
  <tr><td>Rome</td><td>4</td><td>3</td><td>1</td><td>4000</td></tr>
  <tr><td>Paris</td><td>4</td><td>3</td><td>1</td><td>4700</td></tr>
  <tr><td>Cancun</td><td>4</td><td>3</td><td>1</td><td>6800</td></tr>
</table>

<hr>

<h2>Airflow Architecture</h2>

<p>The final Airflow setup uses:</p>

<ul>
  <li><code>airflow-init</code></li>
  <li><code>airflow-webserver</code></li>
  <li><code>airflow-scheduler</code></li>
  <li><code>postgres</code></li>
</ul>

<p>
  PostgreSQL is used instead of SQLite for Airflow metadata.
</p>

<p><strong>Airflow UI:</strong></p>

<pre>http://localhost:8081</pre>

<hr>

<h2>Continuous Integration</h2>

<p>
  GitHub Actions automatically validates the project on every push and pull request.
</p>

<pre>
Git Push / Pull Request
          ↓
Checkout Repository
          ↓
Setup Python
          ↓
Install Dependencies
          ↓
Validate Python Syntax
          ↓
Validate Docker Compose
          ↓
Build Airflow Image
          ↓
CI Success / Failure
</pre>

<p><strong>Current CI status:</strong> Passing</p>

<hr>

<h2>Running the Project</h2>

<h3>Batch Pipeline</h3>

<pre>
docker compose up -d postgres cassandra
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
</pre>

<h3>Real-Time Pipeline</h3>

<pre>
docker compose up -d kafka spark cassandra
</pre>

<h3>Run the Producer</h3>

<pre>
python simulator/generate_events.py
</pre>

<hr>

<h2>Engineering Challenges Solved</h2>

<ul>
  <li>Spark / Cassandra connector compatibility</li>
  <li>Spark shuffle partition optimization</li>
  <li>Cassandra readiness timing</li>
  <li>Airflow instability with SQLite</li>
  <li>Airflow dependency compatibility</li>
  <li>Docker Desktop resource management</li>
</ul>

<hr>

<h2>What This Project Demonstrates</h2>

<ul>
  <li>Data engineering</li>
  <li>Streaming architectures</li>
  <li>Batch processing</li>
  <li>Workflow orchestration</li>
  <li>Distributed processing</li>
  <li>NoSQL storage</li>
  <li>Container networking</li>
  <li>Docker Compose</li>
  <li>CI automation</li>
  <li>Multi-service troubleshooting</li>
</ul>

<hr>

<h2>Future Improvements</h2>

<ul>
  <li>Add Kafka persistent volumes</li>
  <li>Add health checks for Kafka, Cassandra, and Airflow</li>
  <li>Move credentials to environment variables or secrets</li>
  <li>Add automated unit and integration tests</li>
  <li>Add Power BI or Streamlit dashboards</li>
  <li>Add Docker Hub publishing</li>
  <li>Extend CI to CI/CD</li>
  <li>Deploy to Kubernetes</li>
  <li>Deploy to a cloud platform</li>
</ul>

<hr>

<h2>Author</h2>

<p>
  <strong>Mahsa Najimoghadam</strong><br>
  Computer Science graduate and Big Data / Business Intelligence student.
</p>

<p>
  Repository:
  <a href="https://github.com/mahsanaji84/travelflow-data-pipeline">
    github.com/mahsanaji84/travelflow-data-pipeline
  </a>
</p>
