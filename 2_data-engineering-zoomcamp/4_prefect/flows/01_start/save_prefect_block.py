
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
import os

connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.POSTGRESQL_PSYCOPG2,
        username=os.environ.get("USERNAME_PLACEHOLDER"),
        password=os.environ.get("PASSWORD_PLACEHOLDER"),
        host="localhost",
        port=5432,
        database=os.environ.get("DATABASE_PLACEHOLDER"),
    )
)

connector.save("ny-taxi-db-credentials")
