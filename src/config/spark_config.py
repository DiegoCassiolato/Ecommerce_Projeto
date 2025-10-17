"""
Configuração do Spark para execução local
Otimizado para desenvolvimento em máquina local
"""
from pyspark.sql import SparkSession
import os

def configure_spark_with_delta_pip(builder: SparkSession.builder) -> SparkSession.builder:
    """Configura Spark (sem Delta Lake por enquanto)"""
    print("🔧 Configurando Spark...")
    
    # Delta Lake pode ser habilitado depois se necessário
    # Por enquanto, use Parquet para desenvolvimento
    
    return builder

def create_local_spark_session(app_name: str) -> SparkSession:
    """Cria uma Spark Session local com configurações otimizadas"""
    print(f"\n🔧 Criando Spark Session: {app_name}...")
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_data_paths():
    """
    Retorna os caminhos para as camadas de dados
    
    Returns:
        dict: Dicionário com caminhos das camadas
    """
    # Pega o diretório raiz do projeto (2 níveis acima deste arquivo)
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    
    paths = {
        "raw": os.path.join(base_path, "raw"),
        "bronze": os.path.join(base_path, "bronze"),
        "silver": os.path.join(base_path, "silver"),
        "gold": os.path.join(base_path, "gold"),
        "export": os.path.join(base_path, "export")
    }
    
    # Criar diretórios se não existirem
    for layer, path in paths.items():
        os.makedirs(path, exist_ok=True)
        print(f"   📁 {layer.capitalize()}: {path}")
    
    return paths

def stop_spark_session(spark):
    """
    Para a sessão Spark de forma segura
    
    Args:
        spark (SparkSession): Sessão Spark a ser encerrada
    """
    if spark:
        spark.stop()
        print("🛑 Spark Session encerrada")

# Teste do módulo
if __name__ == "__main__":
    print("🧪 Testando configuração do Spark...\n")
    
    # Criar sessão
    spark = create_local_spark_session("TestConfig")
    
    # Obter caminhos
    print("\n📂 Caminhos configurados:")
    paths = get_data_paths()
    
    # Teste simples
    print("\n🧪 Executando teste simples...")
    df_test = spark.createDataFrame([(1, "teste"), (2, "spark")], ["id", "valor"])
    df_test.show()
    
    # Encerrar
    stop_spark_session(spark)
    print("\n✅ Teste concluído com sucesso!")
