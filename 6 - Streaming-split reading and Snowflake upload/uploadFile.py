
import json
import gzip
import os
from datetime import datetime

import ijson
from internal.library.SnowflakeUtils import SnowflakeUtils
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, LongType, StructType, StructField


class UploadFile:
    """Utility class to discover JSON(.gz) files, stream them in chunks and upload
    to Snowflake while logging processed files. A SparkSession must be provided
    by the caller (e.g. notebook / driver script) – this class does not create one.

    Args:
        dbutils: Databricks utilities (for file listing)
        spark_session: An active SparkSession provided by the caller
    """

    def __init__(self, dbutils, spark_session: SparkSession):
        if spark_session is None:
            raise ValueError("spark_session must be provided (use SparkSession.getActiveSession()).")
        self.spark = spark_session
        self.dbutils = dbutils
        self.storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME_ZW')
        self.container = os.getenv('CONTAINER_ZW')
        self.process_start_timestamp = datetime.utcnow()
        self.etl_azure_files_control = 'DB.SCHEMA.ETL_AZURE_FILES_CONTROL'

    #llevamos a snowflake las particiones que se van leyendo del fichero json
    def upload_json_to_snowflake(self, buffer, input_file_path, target_db, target_schema, target_table):
        df = (
            self.spark.createDataFrame(buffer)
            .withColumnRenamed("_1", "PAYLOAD")
            .withColumn("ingestion_timestamp", lit(datetime.utcnow()))
            .withColumn("process_start_timestamp", lit(self.process_start_timestamp))
            .withColumn("file_path", lit(input_file_path))
        )
        SnowflakeUtils().uploadDataset(df, target_db, target_schema, target_table, append=True)
        
    #parseamos el array con ijson para hacer que la lectura de ficheros grandes sea mejor
    def upload_split_jsongz_array_to_snowflake(
        self,
        input_file_path,
        target_db,
        target_schema,
        target_table,
        chunk_size=100_000
    ):
        try:
            #si el array es gzip o solo un json se leen de diferentes maneras
            with (gzip.open(input_file_path, 'rb', encoding='utf-8') if input_file_path.endswith('.gz') else open(input_file_path, 'r', encoding='utf-8')) as f:
                parser = ijson.items(f, 'item')
                buffer = []
                file_count = 0
 
                for obj in parser:  # recorremos el objeto JSON entero
                    buffer.append(Row(json.dumps(obj, ensure_ascii=False, default=str)))  # mete una fila del json en la lista
                    if len(buffer) >= chunk_size:  # comprobamos si la longitud de la lista es mayor al chunk definido
                        file_count += 1  # cuenta los chuncks procesados
                        self.upload_json_to_snowflake(buffer, input_file_path, target_db, target_schema, target_table)
                        buffer = []

                #Esta parte se incluye por si al final tenemos un chunk < 100_000
                if buffer:  # comprueba si la lista tiene algún elemenento
                    file_count += 1  # cuenta los chuncks procesados
                    self.upload_json_to_snowflake(buffer, input_file_path, target_db, target_schema, target_table)
            print(f"Done. Total chunks: {file_count}")
            return True
        except Exception as e:
            print(f"Error processing file: {e}")
            return False
        
    # se obtienen todos los ficheros de manera recursiva
    def list_json_gz_files(self, main_folder, file_name):
        files = [] #lista con todos los ficheros
        dbfs_main_folder = main_folder.replace('/dbfs', '', 1) #se quita /dbfs porque para buscar dbutils no hace falta
        for entry in self.dbutils.fs.ls(dbfs_main_folder):
            if entry.isDir():  # comprueba si es un directorio
                files.extend(self.list_json_gz_files(entry.path, file_name))
            elif entry.name.endswith(file_name):
                files.append({"file_path": entry.path, "file_last_modified_date": entry.modificationTime})
        return files
    
    #se obtienen todo el log de ficheros procesados para no volver a procesarlos
    def get_processed_files(self, process_code):
        etl_control_df = SnowflakeUtils().executeQuery(
            self.etl_azure_files_control.split('.')[0], self.etl_azure_files_control.split('.')[1],
            f"""
            SELECT PROCESS_CODE, FILE_PATH, FILE_LAST_MODIFIED_DATE
            FROM {self.etl_azure_files_control}
            WHERE PROCESS_CODE = '{process_code}'
            """)
        return etl_control_df
 
    def get_files_to_process(self, process_code, main_folder, file_name):
        processed_df = self.get_processed_files(process_code) #obtenemos todos los ficheros procesados
        processed_set = set(
            zip(
                processed_df.select("FILE_PATH").rdd.flatMap(lambda x: x).collect(),
                processed_df.select("FILE_LAST_MODIFIED_DATE").rdd.flatMap(lambda x: x).collect()
            )
        )  # conjunto con path + fecha modificacion

        # Se obtienen todos los ficheros que no se han procesado
        all_files = self.list_json_gz_files(main_folder, file_name)
        files_to_process = [
            {"file_path": f["file_path"], "file_last_modified_date": f["file_last_modified_date"]}
            for f in all_files
            if (f["file_path"], f["file_last_modified_date"]) not in processed_set
        ]  # lista de ficheros no procesados

        return files_to_process
   
    #se mete en la tabla  ETL_AZURE_FILES_CONTROL el log de todos los ficheros que se han procesado
    def log_processed_files(self, processed_files):
        schema = StructType([
            StructField("process_code", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("file_last_modified_date", LongType(), True)
        ])
        # Create DataFrame and upload to Snowflake
        log_df = (
            spark.createDataFrame(processed_files, schema=schema)
            .withColumn("load_date", lit(datetime.utcnow()))
        )
        SnowflakeUtils().uploadDataset(
            log_df,
            self.etl_azure_files_control.split('.')[0],
            self.etl_azure_files_control.split('.')[1],
            self.etl_azure_files_control.split('.')[2],
            append=True
        )