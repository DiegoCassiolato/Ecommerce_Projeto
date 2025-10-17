"""
BRONZE LAYER - Ingestão Incremental
Adiciona novos dados mantendo histórico
"""

import sys
import os
from pyspark.sql.functions import current_timestamp, lit, col
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import create_local_spark_session, get_data_paths, stop_spark_session

class BronzeIngestion:
    """Classe para ingestão incremental na camada Bronze"""
    
    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths
        self.ingestion_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def ingest_json_to_bronze(self, source_file, table_name):
        """Ingere arquivo JSON para Bronze (APPEND)"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        df_raw = self.spark.read.option("inferSchema", "true").json(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_id", lit(self.ingestion_id)) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        # APPEND - mantém histórico
        df_bronze.write \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ {df_bronze.count():,} registros ADICIONADOS")
        print(f"   💾 Modo: APPEND (mantém histórico)")
        print(f"   🆔 Ingestion ID: {self.ingestion_id}")
        
        return df_bronze
    
    def ingest_csv_to_bronze(self, source_file, table_name):
        """Ingere arquivo CSV para Bronze (APPEND)"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        df_raw = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_id", lit(self.ingestion_id)) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        df_bronze.write \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ {df_bronze.count():,} registros ADICIONADOS")
        print(f"   💾 Modo: APPEND (mantém histórico)")
        print(f"   🆔 Ingestion ID: {self.ingestion_id}")
        
        return df_bronze
    
    def ingest_parquet_to_bronze(self, source_file, table_name):
        """Ingere arquivo Parquet para Bronze (APPEND)"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        df_raw = self.spark.read.parquet(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_id", lit(self.ingestion_id)) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        df_bronze.write \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ {df_bronze.count():,} registros ADICIONADOS")
        print(f"   💾 Modo: APPEND (mantém histórico)")
        print(f"   🆔 Ingestion ID: {self.ingestion_id}")
        
        return df_bronze
    
    def run_full_ingestion(self):
        """Executa ingestão incremental completa"""
        print("\n" + "="*70)
        print("🚀 INICIANDO INGESTÃO BRONZE INCREMENTAL")
        print("="*70)
        print(f"🆔 Ingestion ID: {self.ingestion_id}")
        print("💾 Modo: APPEND (mantém histórico de todas as execuções)")
        print("="*70)
        
        start_time = datetime.now()
        
        self.ingest_json_to_bronze("customers.json", "bronze_customers")
        self.ingest_csv_to_bronze("products.csv", "bronze_products")
        self.ingest_json_to_bronze("sales.json", "bronze_sales")
        self.ingest_parquet_to_bronze("payments.parquet", "bronze_payments")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("✅ INGESTÃO BRONZE INCREMENTAL CONCLUÍDA!")
        print("="*70)
        print(f"   ⏱️  Tempo: {duration:.2f} segundos")
        print(f"   🆔 Ingestion ID: {self.ingestion_id}")
        print("="*70 + "\n")

if __name__ == "__main__":
    spark = create_local_spark_session("BronzeIngestion")
    paths = get_data_paths()
    
    ingestion = BronzeIngestion(spark, paths)
    ingestion.run_full_ingestion()
    
    print("\n📊 TOTAL DE REGISTROS (TODAS AS INGESTÕES):")
    print("="*70)
    for table in ["bronze_customers", "bronze_products", "bronze_sales", "bronze_payments"]:
        table_path = os.path.join(paths['bronze'], table)
        if os.path.exists(table_path):
            df = spark.read.parquet(table_path)
            total = df.count()
            unique_ingestions = df.select("ingestion_id").distinct().count()
            print(f"   📦 {table}:")
            print(f"      • Total: {total:,} registros")
            print(f"      • Ingestões: {unique_ingestions} execuções")
    print("="*70 + "\n")
    
    stop_spark_session(spark)
