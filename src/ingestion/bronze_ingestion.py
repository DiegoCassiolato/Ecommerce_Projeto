"""
BRONZE LAYER - Ingestão de dados brutos
Lê arquivos raw e salva em formato Parquet na camada Bronze
"""

import sys
import os
from pyspark.sql.functions import current_timestamp, lit, col
from datetime import datetime

# Adicionar path do projeto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import create_local_spark_session, get_data_paths, stop_spark_session

class BronzeIngestion:
    """Classe para ingestão de dados na camada Bronze"""
    
    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths
        
    def ingest_json_to_bronze(self, source_file, table_name):
        """Ingere arquivo JSON para Bronze"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        # Ler JSON
        df_raw = self.spark.read.option("inferSchema", "true").json(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        print(f"   📋 Schema:")
        df_raw.printSchema()
        
        # Adicionar metadados de auditoria
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        # Escrever em Parquet
        df_bronze.write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ Salvo em: {bronze_path}")
        print(f"   💾 Formato: Parquet (particionado por data)")
        
        return df_bronze
    
    def ingest_csv_to_bronze(self, source_file, table_name):
        """Ingere arquivo CSV para Bronze"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        # Ler CSV
        df_raw = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        print(f"   📋 Schema:")
        df_raw.printSchema()
        
        # Adicionar metadados
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        # Escrever em Parquet
        df_bronze.write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ Salvo em: {bronze_path}")
        print(f"   💾 Formato: Parquet (particionado por data)")
        
        return df_bronze
    
    def ingest_parquet_to_bronze(self, source_file, table_name):
        """Ingeste arquivo Parquet para Bronze"""
        print(f"\n{'='*70}")
        print(f"🔄 Processando: {source_file}")
        print(f"{'='*70}")
        
        source_path = os.path.join(self.paths['raw'], source_file)
        bronze_path = os.path.join(self.paths['bronze'], table_name)
        
        # Ler Parquet
        df_raw = self.spark.read.parquet(source_path)
        
        print(f"   📊 Registros lidos: {df_raw.count():,}")
        print(f"   📋 Schema:")
        df_raw.printSchema()
        
        # Adicionar metadados
        df_bronze = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("ingestion_date", current_timestamp().cast("date"))
        
        # Escrever em Parquet
        df_bronze.write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_path)
        
        print(f"   ✅ Salvo em: {bronze_path}")
        print(f"   💾 Formato: Parquet (particionado por data)")
        
        return df_bronze
    
    def run_full_ingestion(self):
        """Executa ingestão completa de todas as fontes"""
        print("\n" + "="*70)
        print("🚀 INICIANDO INGESTÃO BRONZE COMPLETA")
        print("="*70)
        
        start_time = datetime.now()
        
        # Ingerir cada fonte (SEM events por enquanto)
        self.ingest_json_to_bronze("customers.json", "bronze_customers")
        self.ingest_csv_to_bronze("products.csv", "bronze_products")
        self.ingest_json_to_bronze("sales.json", "bronze_sales")
        self.ingest_parquet_to_bronze("payments.parquet", "bronze_payments")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("✅ INGESTÃO BRONZE CONCLUÍDA COM SUCESSO!")
        print("="*70)
        print(f"   ⏱️  Tempo total: {duration:.2f} segundos")
        print("="*70 + "\n")

# Script de execução
if __name__ == "__main__":
    # Criar sessão Spark
    spark = create_local_spark_session("BronzeIngestion")
    paths = get_data_paths()
    
    # Executar ingestão
    ingestion = BronzeIngestion(spark, paths)
    ingestion.run_full_ingestion()
    
    # Mostrar estatísticas
    print("\n📊 ESTATÍSTICAS DAS TABELAS BRONZE:")
    print("="*70)
    for table in ["bronze_customers", "bronze_products", "bronze_sales", "bronze_payments"]:
        table_path = os.path.join(paths['bronze'], table)
        if os.path.exists(table_path):
            df = spark.read.parquet(table_path)
            print(f"   📦 {table}: {df.count():,} registros")
    print("="*70 + "\n")
    
    stop_spark_session(spark)
