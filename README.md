%pip install faker 
-----------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, sum, format_number
import random
import matplotlib.pyplot as plt

# Inicializa Spark
spark = SparkSession.builder.appName("AnaliseVendas").getOrCreate()
db_name = "default"

# Função para formatar moeda
def formatar_moeda(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ------------------------------ #
#       CRIAÇÃO DAS TABELAS      #
# ------------------------------ #

# --- 1. Criar Tabela PRODUTOS ---
schema_produtos = StructType([
    StructField("id", IntegerType(), False),
    StructField("nome", StringType(), False),
    StructField("preco", FloatType(), False),
    StructField("estoque", IntegerType(), False),
    StructField("total_vendas", IntegerType(), False)
])

# Lista personalizada de produtos em português
lista_produtos = [
    "Arroz Premium", "Feijão Carioca", "Café Extra Forte", "Leite Integral",
    "Óleo de Soja", "Sabão Líquido", "Detergente Neutro", "Açúcar Refinado",
    "Macarrão Parafuso", "Molho de Tomate", "Biscoito Recheado", "Margarina Cremosa"
]

# Gerando dados para os produtos
dados_produtos = [(i + 1, random.choice(lista_produtos), 
                   float(round(random.uniform(10, 1000), 2)),  # Mantém float para cálculos corretos
                   random.randint(10, 500), random.randint(50, 1000)) 
                  for i in range(10000)]

df_produtos = spark.createDataFrame(dados_produtos, schema=schema_produtos)

# Salvando a tabela
df_produtos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{db_name}.produtos")

# --- 2. Criar Tabela ATENDIMENTO ---
schema_atendimento = StructType([
    StructField("id", IntegerType(), False),
    StructField("nome_atendente", StringType(), False),
    StructField("numero_atendimento", IntegerType(), False),
    StructField("filial", StringType(), False)
])

estados_brasil = ["SP", "RJ", "MG", "BA", "PR", "RS", "SC", "PE", "CE", "GO"]
atendentes_filiais = [(i + 1, f"Atendente {i + 1}", f"filial_{random.choice(estados_brasil)}") for i in range(400)]

dados_atendimento = [(atendentes_filiais[i % 400][0], atendentes_filiais[i % 400][1], i + 1, atendentes_filiais[i % 400][2]) for i in range(10000)]
df_atendimento = spark.createDataFrame(dados_atendimento, schema=schema_atendimento)
df_atendimento.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{db_name}.atendimento")

# --- 3. Criar Tabela DETALHE_VENDA ---
schema_detalhe_venda = StructType([
    StructField("id", IntegerType(), False),
    StructField("produto", IntegerType(), False),
    StructField("quantidade", IntegerType(), False)
])

dados_detalhe_venda = [(i + 1, random.randint(1, 10000), random.randint(1, 100)) for i in range(10000)]
df_detalhe_venda = spark.createDataFrame(dados_detalhe_venda, schema=schema_detalhe_venda)
df_detalhe_venda.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{db_name}.detalhe_venda")

print("✅ Tabelas criadas com sucesso!")

# ------------------------------ #
#       ANÁLISE DOS DADOS        #
# ------------------------------ #

# Carregar tabelas
df_produtos = spark.sql("SELECT id, nome, format_number(preco, 2) as preco, estoque, total_vendas FROM default.produtos")
df_atendimento = spark.sql("SELECT * FROM default.atendimento")

# --- 1. Fabricação por Produto ---
df_faturamento = df_produtos.withColumn("faturamento", col("preco") * col("total_vendas")) \
    .groupBy("nome").agg(sum("faturamento").alias("faturamento_total")) \
    .orderBy(col("faturamento_total").desc())

produto_top = df_faturamento.first()
produto_top_nome = produto_top["nome"]
produto_top_faturamento = formatar_moeda(produto_top["faturamento_total"])

print(f"\n📌 Produto com maior faturamento: {produto_top_nome} - {produto_top_faturamento}")

# --- 2. Valor Alocado no Estoque ---
df_valor_estoque = df_produtos.withColumn("valor_estoque", col("preco") * col("estoque")) \
    .select("nome", "valor_estoque") \
    .orderBy(col("valor_estoque").desc())

produto_estoque_top = df_valor_estoque.first()
produto_estoque_top_nome = produto_estoque_top["nome"]
produto_estoque_top_valor = formatar_moeda(produto_estoque_top["valor_estoque"])

print(f"\n📦 Produto com maior valor no estoque: {produto_estoque_top_nome} - {produto_estoque_top_valor}")

# --- 3. Atendimentos por Filial ---
df_atendimentos_filial = df_atendimento.groupBy("filial") \
    .count().orderBy(col("count").desc())

pdf_atendimentos_filial = df_atendimentos_filial.toPandas()
pdf_top5 = pdf_atendimentos_filial.head(5)

print("\n🏢 Top 5 Filiais por Atendimento:")
print(pdf_top5)

plt.figure(figsize=(8,5))
plt.bar(pdf_top5["filial"], pdf_top5["count"], color="blue")
plt.xlabel("Filial")
plt.ylabel("Número de Atendimentos")
plt.title("Top 5 Filiais por Atendimento")
plt.xticks(rotation=45)
plt.show()

# --- 4. Tomada de Decisão ---
filial_top = pdf_top5["filial"][0]

print("\n--- 🏆 TOMADA DE DECISÃO ---")
print(f"📢 O produto '{produto_top_nome}' deve ser promovido, pois gerou o maior faturamento de {produto_top_faturamento}.")
print(f"🏢 A filial '{filial_top}' deve receber mais investimentos, pois teve o maior número de atendimentos.")
print(f"📦 Para otimizar o estoque, analisar o produto '{produto_estoque_top_nome}', que tem o maior valor alocado de {produto_estoque_top_valor}.")
