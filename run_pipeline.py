"""
PIPELINE COMPLETO - Orquestração End-to-End
Executa todo o fluxo: Data Generation → Bronze → Silver → Gold
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config.spark_config import create_local_spark_session, get_data_paths, stop_spark_session
from ingestion.data_generator import EcommerceDataGenerator
from ingestion.bronze_ingestion import BronzeIngestion
from transformation.silver_transformation import SilverTransformation
from transformation.gold_aggregation import GoldAggregation

def print_header(title):
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def print_step(step_number, step_name):
    print("\n" + "-"*80)
    print(f"  ETAPA {step_number}: {step_name}")
    print("-"*80)

def run_full_pipeline(regenerate_data=False):
    print_header("PIPELINE E-COMMERCE - ARQUITETURA MEDALLION")
    
    pipeline_start = datetime.now()
    
    if regenerate_data:
        print_step(1, "GERACAO DE DADOS SINTETICOS")
        try:
            generator = EcommerceDataGenerator(num_customers=1000, num_products=500)
            datasets = generator.save_to_files()
            print("OK Dados sinteticos gerados com sucesso!")
        except Exception as e:
            print(f"ERRO na geracao de dados: {str(e)}")
            return False
    else:
        print_step(1, "GERACAO DE DADOS SINTETICOS [PULADO]")
        print("   Usando dados existentes em data/raw/")
    
    print_step(2, "INGESTAO BRONZE (Raw -> Bronze)")
    try:
        spark = create_local_spark_session("BronzeIngestion")
        paths = get_data_paths()
        bronze = BronzeIngestion(spark, paths)
        bronze.run_full_ingestion()
        stop_spark_session(spark)
        print("OK Ingestao Bronze concluida!")
    except Exception as e:
        print(f"ERRO na ingestao Bronze: {str(e)}")
        return False
    
    print_step(3, "TRANSFORMACAO SILVER (Bronze -> Silver)")
    try:
        spark = create_local_spark_session("SilverTransformation")
        paths = get_data_paths()
        silver = SilverTransformation(spark, paths)
        silver.run_full_transformation()
        stop_spark_session(spark)
        print("OK Transformacao Silver concluida!")
    except Exception as e:
        print(f"ERRO na transformacao Silver: {str(e)}")
        return False
    
    print_step(4, "AGREGACAO GOLD (Silver -> Gold)")
    try:
        spark = create_local_spark_session("GoldAggregation")
        paths = get_data_paths()
        gold = GoldAggregation(spark, paths)
        gold.run_full_aggregation()
        stop_spark_session(spark)
        print("OK Agregacao Gold concluida!")
    except Exception as e:
        print(f"ERRO na agregacao Gold: {str(e)}")
        return False
    
    pipeline_end = datetime.now()
    duration = (pipeline_end - pipeline_start).total_seconds()
    
    print_header("PIPELINE EXECUTADO COM SUCESSO!")
    
    print("RESUMO DA EXECUCAO:")
    print("-"*80)
    print(f"   Tempo total: {duration:.2f} segundos ({duration/60:.2f} minutos)")
    print(f"   Inicio: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Fim: {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*80)
    
    print("\nESTATISTICAS FINAIS:")
    print("-"*80)
    
    try:
        spark = create_local_spark_session("Statistics")
        paths = get_data_paths()
        
        print("\nBRONZE:")
        for table in ["bronze_customers", "bronze_products", "bronze_sales", "bronze_payments"]:
            table_path = os.path.join(paths['bronze'], table)
            if os.path.exists(table_path):
                df = spark.read.parquet(table_path)
                print(f"   - {table}: {df.count():,} registros")
        
        print("\nSILVER:")
        for table in ["silver_customers", "silver_products", "silver_sales", "silver_payments"]:
            table_path = os.path.join(paths['silver'], table)
            if os.path.exists(table_path):
                df = spark.read.parquet(table_path)
                print(f"   - {table}: {df.count():,} registros")
        
        print("\nGOLD:")
        for table in ["gold_sales_summary", "gold_customer_metrics", "gold_product_performance", "gold_payment_analysis"]:
            table_path = os.path.join(paths['gold'], table)
            if os.path.exists(table_path):
                df = spark.read.parquet(table_path)
                print(f"   - {table}: {df.count():,} registros")
        
        stop_spark_session(spark)
    except Exception as e:
        print(f"   Nao foi possivel gerar estatisticas: {str(e)}")
    
    print("-"*80)
    print("\nPipeline concluido com sucesso!")
    print("Dados disponiveis em: data/bronze/, data/silver/, data/gold/")
    print("="*80 + "\n")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline E-commerce - Arquitetura Medallion')
    parser.add_argument('--regenerate-data', action='store_true', help='Gera novos dados sinteticos')
    
    args = parser.parse_args()
    
    success = run_full_pipeline(regenerate_data=args.regenerate_data)
    
    if not success:
        print("\nPipeline falhou! Verifique os erros acima.")
        sys.exit(1)
    else:
        print("\nPipeline executado com sucesso!")
        sys.exit(0)
