import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType


def create_spark_connection():

    spark_conn = None
    try:
        spark_conn = (
            SparkSession.builder.appName("SparkStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
    except Exception as e:
        logging.error(f"Could not create spark session. Error message: {e}")
        print(e)

    return spark_conn


def create_cassndra_connection():

    try:
        cluster = Cluster(["localhost"])
        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"couldn't create cassandra cluster. Error msgg: {e}")
        return None


def create_keyspace(session):
    session.execute(
        """
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy',
                    replication_factor': '1'}
                    """
    )
    print("Keyspace creates successfully!")


def create_table(session):
    session.execute(
        """
            CREATE TABLE IF NOT EXITSTS spark_streams.created_user (

            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            Address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
            )"""
    )
    print("Table created successfully")


def insert_data(session, **kwargs):
    print("Inserting data ....")
    id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    Address = kwargs.get("Address")
    postcode = kwargs.get("postcode")
    email = kwargs.get("email")
    username = kwargs.get("username")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute(
            """INSERT INTO spark_streams.created_user(id, first_name,
                    last_name, gender, Address,postcode, email, username,
                    registered_date, phone, picture
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id,
                first_name,
                last_name,
                gender,
                Address,
                postcode,
                email,
                username,
                registered_date,
                phone,
                picture,
            ),
        )
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error("Data could not be inserted. Error msg: {}")


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "user_created")
            .option("startingOffset", "earliest")
            .load()
        )
        logging.info(" Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created. Err msg: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("Address", StringType(), False),
            StructField("postcode", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    selection_df = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return selection_df


if __name__ == "__main__":

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        """
            - connect to spark
            - connect kafka to spark
            - reformat df to kafka df
            - connect to cossandra
            - create keyspace
            - create table
            - inset data in table
            - write streaming data to cassandra
          """
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassndra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)

            streaming_query = (
                selection_df.writeStream.format(
                    "org.apache.spark.sql.cassandra"
                )
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .start()
            )

            streaming_query.awaitTermination()
