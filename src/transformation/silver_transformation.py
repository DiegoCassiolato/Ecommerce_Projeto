"""
SILVER LAYER - Transformação Incremental
Processa apenas dados novos da Bronze
"""

import sys
import os
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, upper, lower, 
    regexp_replace, when, row_number, to_timestamp, to_date, max as _max
)
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import create_local_spark_session, get_data_paths, stop_spark_session

class SilverTransformation:
    """Classe para transformações incrementais da camada Silver"""
    
    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths
        self.processing_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def get_last_processed_timestamp(self, table_path):
        """Retorna o último timestamp processado"""
        try:
            if os.path.exists(table_path):
                df = self.spark.read.parquet(table_path)
                last_ts = df.agg(_max("processing_timestamp")).collect()[0][0]
                return last_ts
            return None
        except:
            return None
    
    def transform_customers(self):
        """Transforma clientes (incremental)"""
        print(f"\n{'='*70}")
        print(f"🔄 Transformando: CUSTOMERS (Incremental)")
        print(f"{'='*70}")
        
        bronze_path = os.path.join(self.paths['bronze'], "bronze_customers")
        silver_path = os.path.join(self.paths['silver'], "silver_customers")
        
        df_bronze = self.spark.read.parquet(bronze_path)
        
        # Filtrar apenas novos registros
        last_ts = self.get_last_processed_timestamp(silver_path)
        if last_ts:
            df_bronze = df_bronze.filter(col("ingestion_timestamp") > last_ts)
            print(f"   📊 Novos registros desde {last_ts}: {df_bronze.count():,}")
        else:
            print(f"   📊 Primeira execução - processando todos: {df_bronze.count():,}")
        
        if df_bronze.count() == 0:
            print("   ⏭️  Nenhum registro novo para processar")
            return None
        
        # Transformações
        df_silver = df_bronze \
            .withColumn("name", trim(col("name"))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
            .withColumn("cpf", regexp_replace(col("cpf"), "[^0-9]", "")) \
            .withColumn("state", upper(trim(col("state")))) \
            .withColumn("created_at", to_timestamp(col("created_at"))) \
            .withColumn("updated_at", to_timestamp(col("updated_at"))) \
            .withColumn("birth_date", to_date(col("birth_date"))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_id", lit(self.processing_id))
        
        # Deduplicação
        window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
        df_silver = df_silver \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        # Validações
        df_silver = df_silver.filter(
            (col("email").isNotNull()) & 
            (col("email").contains("@")) &
            (col("cpf").isNotNull())
        )
        
        print(f"   ✅ Registros processados: {df_silver.count():,}")
        
        # APPEND
        df_silver.write \
            .mode("append") \
            .partitionBy("state") \
            .parquet(silver_path)
        
        print(f"   💾 Modo: APPEND")
        
        return df_silver
    
    def transform_products(self):
        """Transforma produtos (incremental)"""
        print(f"\n{'='*70}")
        print(f"🔄 Transformando: PRODUCTS (Incremental)")
        print(f"{'='*70}")
        
        bronze_path = os.path.join(self.paths['bronze'], "bronze_products")
        silver_path = os.path.join(self.paths['silver'], "silver_products")
        
        df_bronze = self.spark.read.parquet(bronze_path)
        
        last_ts = self.get_last_processed_timestamp(silver_path)
        if last_ts:
            df_bronze = df_bronze.filter(col("ingestion_timestamp") > last_ts)
            print(f"   📊 Novos registros: {df_bronze.count():,}")
        else:
            print(f"   📊 Primeira execução: {df_bronze.count():,}")
        
        if df_bronze.count() == 0:
            print("   ⏭️  Nenhum registro novo")
            return None
        
        df_silver = df_bronze \
            .withColumn("product_name", trim(col("product_name"))) \
            .withColumn("category", trim(col("category"))) \
            .withColumn("price", col("price").cast("decimal(10,2)")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_id", lit(self.processing_id))
        
        df_silver = df_silver.filter(
            (col("price") > 0) &
            (col("stock_quantity") >= 0) &
            (col("product_name").isNotNull())
        )
        
        df_silver = df_silver.withColumn(
            "is_available",
            when(col("stock_quantity") > 0, True).otherwise(False)
        )
        
        print(f"   ✅ Registros processados: {df_silver.count():,}")
        
        df_silver.write \
            .mode("append") \
            .partitionBy("category") \
            .parquet(silver_path)
        
        print(f"   💾 Modo: APPEND")
        
        return df_silver
    
    def transform_sales(self):
        """Transforma vendas (incremental)"""
        print(f"\n{'='*70}")
        print(f"🔄 Transformando: SALES (Incremental)")
        print(f"{'='*70}")
        
        bronze_path = os.path.join(self.paths['bronze'], "bronze_sales")
        silver_path = os.path.join(self.paths['silver'], "silver_sales")
        
        df_bronze = self.spark.read.parquet(bronze_path)
        
        last_ts = self.get_last_processed_timestamp(silver_path)
        if last_ts:
            df_bronze = df_bronze.filter(col("ingestion_timestamp") > last_ts)
            print(f"   📊 Novos registros: {df_bronze.count():,}")
        else:
            print(f"   📊 Primeira execução: {df_bronze.count():,}")
        
        if df_bronze.count() == 0:
            print("   ⏭️  Nenhum registro novo")
            return None
        
        df_silver = df_bronze \
            .withColumn("order_date", to_timestamp(col("order_date"))) \
            .withColumn("order_year", col("order_date").substr(1, 4).cast("int")) \
            .withColumn("order_month", col("order_date").substr(6, 2).cast("int")) \
            .withColumn("total_amount", col("total_amount").cast("decimal(10,2)")) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_id", lit(self.processing_id))
        
        df_silver = df_silver.filter(
            (col("total_amount") > 0) &
            (col("quantity") > 0) &
            (col("order_date").isNotNull())
        )
        
        print(f"   ✅ Registros processados: {df_silver.count():,}")
        
        df_silver.write \
            .mode("append") \
            .partitionBy("order_year", "order_month") \
            .parquet(silver_path)
        
        print(f"   💾 Modo: APPEND")
        
        return df_silver
    
    def transform_payments(self):
        """Transforma pagamentos (incremental)"""
        print(f"\n{'='*70}")
        print(f"🔄 Transformando: PAYMENTS (Incremental)")
        print(f"{'='*70}")
        
        bronze_path = os.path.join(self.paths['bronze'], "bronze_payments")
        silver_path = os.path.join(self.paths['silver'], "silver_payments")
        
        df_bronze = self.spark.read.parquet(bronze_path)
        
        last_ts = self.get_last_processed_timestamp(silver_path)
        if last_ts:
            df_bronze = df_bronze.filter(col("ingestion_timestamp") > last_ts)
            print(f"   📊 Novos registros: {df_bronze.count():,}")
        else:
            print(f"   📊 Primeira execução: {df_bronze.count():,}")
        
        if df_bronze.count() == 0:
            print("   ⏭️  Nenhum registro novo")
            return None
        
        df_silver = df_bronze \
            .withColumn("payment_date", to_timestamp(col("payment_date"))) \
            .withColumn("created_at", to_timestamp(col("created_at"))) \
            .withColumn("amount", col("amount").cast("decimal(10,2)")) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_id", lit(self.processing_id))
        
        df_silver = df_silver.filter(
            (col("amount") > 0) &
            (col("payment_date").isNotNull())
        )
        
        print(f"   ✅ Registros processados: {df_silver.count():,}")
        
        df_silver.write \
            .mode("append") \
            .partitionBy("payment_method") \
            .parquet(silver_path)
        
        print(f"   💾 Modo: APPEND")
        
        return df_silver
    
    def run_full_transformation(self):
        """Executa transformação incremental completa"""
        print("\n" + "="*70)
        print("🚀 INICIANDO TRANSFORMAÇÃO SILVER INCREMENTAL")
        print("="*70)
        print(f"🆔 Processing ID: {self.processing_id}")
        print("="*70)
        
        start_time = datetime.now()
        
        self.transform_customers()
        self.transform_products()
        self.transform_sales()
        self.transform_payments()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("✅ TRANSFORMAÇÃO SILVER INCREMENTAL CONCLUÍDA!")
        print("="*70)
        print(f"   ⏱️  Tempo: {duration:.2f} segundos")
        print("="*70 + "\n")

if __name__ == "__main__":
    spark = create_local_spark_session("SilverTransformation")
    paths = get_data_paths()
    
    transformation = SilverTransformation(spark, paths)
    transformation.run_full_transformation()
    
    print("\n📊 TOTAL DE REGISTROS SILVER:")
    print("="*70)
    for table in ["silver_customers", "silver_products", "silver_sales", "silver_payments"]:
        table_path = os.path.join(paths['silver'], table)
        if os.path.exists(table_path):
            df = spark.read.parquet(table_path)
            print(f"   📦 {table}: {df.count():,} registros")
    print("="*70 + "\n")
    
    stop_spark_session(spark)
