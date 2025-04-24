import os
import sys
import pymongo
import certifi
import urllib.parse
from dotenv import load_dotenv
from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME, MONGODB_URL_KEY

# Load environment variables from .env file with explicit path
load_dotenv("/home/suhas/Music/mlops/insurance/insurance-project/.env")
print(f"Current working directory: {os.getcwd()}")
print(f"Loaded MONGODB_CLUSTER_URL: {os.getenv('MONGODB_CLUSTER_URL')}")

# Load the certificate authority file to avoid timeout errors when connecting to MongoDB
ca = certifi.where()

class MongoDBClient:
    """
    MongoDBClient is responsible for establishing a connection to the MongoDB database.

    Attributes:
    ----------
    client : MongoClient
        A shared MongoClient instance for the class.
    database : Database
        The specific database instance that MongoDBClient connects to.

    Methods:
    -------
    __init__(database_name: str) -> None
        Initializes the MongoDB connection using the given database name.
    """

    client = None  # Shared MongoClient instance across all MongoDBClient instances

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """
        Initializes a connection to the MongoDB database. If no existing connection is found, it establishes a new one.

        Parameters:
        ----------
        database_name : str, optional
            Name of the MongoDB database to connect to. Default is set by DATABASE_NAME constant.

        Raises:
        ------
        MyException
            If there is an issue connecting to MongoDB or if the environment variables for credentials are not set.
        """
        try:
            # Check if a MongoDB client connection has already been established; if not, create a new one
            if MongoDBClient.client is None:
                # Fetch raw credentials from environment variables
                username = os.getenv("MONGODB_USERNAME")
                password = os.getenv("MONGODB_PASSWORD")
                cluster_url = os.getenv("MONGODB_CLUSTER_URL", "cluster0.mongodb.net")
                print(f"Using cluster URL in MongoDBClient: {cluster_url}")

                if not username or not password:
                    raise Exception("MONGODB_USERNAME or MONGODB_PASSWORD environment variables are not set.")

                # Encode username and password
                encoded_username = urllib.parse.quote_plus(username)
                encoded_password = urllib.parse.quote_plus(password)

                # Construct the MongoDB URL with encoded credentials
                mongo_db_url = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority"

                # Log the URL for debugging (remove in production)
                logging.info(f"Connecting to MongoDB with URL: {mongo_db_url}")

                # Establish a new MongoDB client connection
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)

            # Use the shared MongoClient for this instance
            self.client = MongoDBClient.client
            self.database = self.client[database_name]  # Connect to the specified database
            self.database_name = database_name
            logging.info("MongoDB connection successful.")

        except Exception as e:
            # Raise a custom exception with traceback details if connection fails
            raise MyException(e, sys)