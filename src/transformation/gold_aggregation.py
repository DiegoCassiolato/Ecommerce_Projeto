"""
GOLD LAYER - Agregações e Métricas de Negócio
Cria tabelas analíticas otimizadas para consumo
"""

import sys
import os
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    round as _round, current_timestamp, countDistinct
)
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import create_local_spark_session, get_data_paths, stop_spark_session

class GoldAggregation:
    """Classe para agregações da camada Gold"""
    
    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths
        
    def create_sales_summary(self):
        """Cria resumo de vendas por período"""
        print(f"\n{'='*70}")
        print(f"📊 Criando: SALES SUMMARY")
        print(f"{'='*70}")
        
        silver_sales = os.path.join(self.paths['silver'], "silver_sales")
        gold_path = os.path.join(self.paths['gold'], "gold_sales_summary")
        
        df_sales = self.spark.read.parquet(silver_sales)
        
        df_summary = df_sales.groupBy("order_year", "order_month", "status") \
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                _sum("total_amount").alias("total_revenue"),
                _sum("quantity").alias("total_quantity"),
                _round(avg("total_amount"), 2).alias("avg_order_value"),
                _max("total_amount").alias("max_order_value"),
                _min("total_amount").alias("min_order_value")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .orderBy("order_year", "order_month", "status")
        
        print(f"   ✅ Registros agregados: {df_summary.count():,}")
        
        df_summary.write.mode("overwrite").parquet(gold_path)
        print(f"   💾 Salvo em: {gold_path}")
        
        return df_summary
    
    def create_customer_metrics(self):
        """Cria métricas por cliente"""
        print(f"\n{'='*70}")
        print(f"📊 Criando: CUSTOMER METRICS")
        print(f"{'='*70}")
        
        silver_customers = os.path.join(self.paths['silver'], "silver_customers")
        silver_sales = os.path.join(self.paths['silver'], "silver_sales")
        gold_path = os.path.join(self.paths['gold'], "gold_customer_metrics")
        
        df_customers = self.spark.read.parquet(silver_customers)
        df_sales = self.spark.read.parquet(silver_sales)
        
        df_metrics = df_sales.groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                _sum("total_amount").alias("total_spent"),
                _round(avg("total_amount"), 2).alias("avg_order_value"),
                _max("order_date").alias("last_order_date"),
                _min("order_date").alias("first_order_date")
            )
        
        df_result = df_customers.select("customer_id", "name", "email", "city", "state") \
            .join(df_metrics, "customer_id", "left") \
            .withColumn("processing_timestamp", current_timestamp())
        
        print(f"   ✅ Clientes com métricas: {df_result.count():,}")
        
        df_result.write.mode("overwrite").partitionBy("state").parquet(gold_path)
        print(f"   💾 Salvo em: {gold_path}")
        
        return df_result
    
    def create_product_performance(self):
        """Cria análise de performance de produtos"""
        print(f"\n{'='*70}")
        print(f"📊 Criando: PRODUCT PERFORMANCE")
        print(f"{'='*70}")
        
        silver_products = os.path.join(self.paths['silver'], "silver_products")
        silver_sales = os.path.join(self.paths['silver'], "silver_sales")
        gold_path = os.path.join(self.paths['gold'], "gold_product_performance")
        
        df_products = self.spark.read.parquet(silver_products)
        df_sales = self.spark.read.parquet(silver_sales)
        
        df_performance = df_sales.groupBy("product_id") \
            .agg(
                count("order_id").alias("total_orders"),
                _sum("quantity").alias("total_quantity_sold"),
                _sum("total_amount").alias("total_revenue"),
                _round(avg("total_amount"), 2).alias("avg_sale_value")
            )
        
        df_result = df_products.select("product_id", "product_name", "category", "price", "stock_quantity") \
            .join(df_performance, "product_id", "left") \
            .withColumn("processing_timestamp", current_timestamp()) \
            .orderBy(col("total_revenue").desc())
        
        print(f"   ✅ Produtos analisados: {df_result.count():,}")
        
        df_result.write.mode("overwrite").partitionBy("category").parquet(gold_path)
        print(f"   💾 Salvo em: {gold_path}")
        
        return df_result
    
    def create_payment_analysis(self):
        """Cria análise de métodos de pagamento"""
        print(f"\n{'='*70}")
        print(f"📊 Criando: PAYMENT ANALYSIS")
        print(f"{'='*70}")
        
        silver_payments = os.path.join(self.paths['silver'], "silver_payments")
        gold_path = os.path.join(self.paths['gold'], "gold_payment_analysis")
        
        df_payments = self.spark.read.parquet(silver_payments)
        
        df_analysis = df_payments.groupBy("payment_method", "status") \
            .agg(
                count("payment_id").alias("total_transactions"),
                _sum("amount").alias("total_amount"),
                _round(avg("amount"), 2).alias("avg_transaction_value"),
                _round(avg("total_installments"), 2).alias("avg_installments")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .orderBy("payment_method", "status")
        
        print(f"   ✅ Registros de análise: {df_analysis.count():,}")
        
        df_analysis.write.mode("overwrite").parquet(gold_path)
        print(f"   💾 Salvo em: {gold_path}")
        
        return df_analysis
    
    def run_full_aggregation(self):
        """Executa todas as agregações Gold"""
        print("\n" + "="*70)
        print("🚀 INICIANDO AGREGAÇÕES GOLD")
        print("="*70)
        
        start_time = datetime.now()
        
        self.create_sales_summary()
        self.create_customer_metrics()
        self.create_product_performance()
        self.create_payment_analysis()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("✅ AGREGAÇÕES GOLD CONCLUÍDAS COM SUCESSO!")
        print("="*70)
        print(f"   ⏱️  Tempo total: {duration:.2f} segundos")
        print("="*70 + "\n")

if __name__ == "__main__":
    spark = create_local_spark_session("GoldAggregation")
    paths = get_data_paths()
    
    aggregation = GoldAggregation(spark, paths)
    aggregation.run_full_aggregation()
    
    print("\n📊 TABELAS GOLD CRIADAS:")
    print("="*70)
    for table in ["gold_sales_summary", "gold_customer_metrics", "gold_product_performance", "gold_payment_analysis"]:
        table_path = os.path.join(paths['gold'], table)
        if os.path.exists(table_path):
            df = spark.read.parquet(table_path)
            print(f"   📦 {table}: {df.count():,} registros")
    print("="*70 + "\n")
    
    stop_spark_session(spark)
