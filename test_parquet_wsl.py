from src.config.spark_config import create_local_spark_session, stop_spark_session

print("🧪 Testando Leitura e Escrita de Arquivos no WSL...\n")

# Criar sessão
spark = create_local_spark_session("TestParquetWSL")

# Criar dados de teste
data = [
    (1, "João", 25, "SP", 5000.00),
    (2, "Maria", 30, "RJ", 6500.00),
    (3, "Pedro", 28, "MG", 5800.00),
    (4, "Ana", 35, "SP", 7200.00),
    (5, "Carlos", 42, "RS", 8100.00)
]

df = spark.createDataFrame(data, ["id", "nome", "idade", "estado", "salario"])

print("📊 Dados originais:")
df.show()

# SALVAR PARQUET (isso não funcionava no Windows!)
print("\n💾 Salvando arquivo Parquet...")
df.write.mode("overwrite").parquet("data/bronze/funcionarios")
print("✅ Arquivo Parquet salvo com sucesso!")

# LER PARQUET
print("\n📖 Lendo arquivo Parquet do disco...")
df_lido = spark.read.parquet("data/bronze/funcionarios")
print("✅ Arquivo lido com sucesso!")
df_lido.show()

# TRANSFORMAÇÕES
print("\n🔄 Aplicando transformações...")
df_transformado = df_lido.filter("idade > 28").select("nome", "idade", "salario")
df_transformado.show()

# SALVAR RESULTADO TRANSFORMADO
print("\n💾 Salvando dados transformados...")
df_transformado.write.mode("overwrite").parquet("data/silver/funcionarios_filtrados")
print("✅ Dados transformados salvos!")

# LER E VERIFICAR
print("\n📖 Lendo dados transformados...")
df_final = spark.read.parquet("data/silver/funcionarios_filtrados")
df_final.show()

# Encerrar
stop_spark_session(spark)
print("\n🎉 SUCESSO TOTAL! Leitura e escrita funcionando perfeitamente no WSL!")
