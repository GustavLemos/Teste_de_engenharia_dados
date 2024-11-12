# Sugestões de Orquestração para o Notebook de Análise de Dados

A orquestração de pipelines de dados é fundamental para garantir que as etapas de processamento sejam executadas de maneira eficiente e confiável. Para o notebook de análise de dados que você forneceu, duas abordagens de orquestração podem ser utilizadas: **Job Workflows no Databricks** e **Apache Airflow**. Ambas as abordagens têm seus prós e contras, e podem ser adaptadas dependendo dos requisitos do projeto. A seguir, explicarei essas duas opções em detalhes, incluindo exemplos de código para implementação de cada uma delas.

---

## 1. **Job Workflows no Databricks**

O **Job Workflow no Databricks** é uma solução interna da plataforma para orquestrar a execução de notebooks e jobs. Ele permite agendar e monitorar tarefas de forma fácil e integrada, sem a necessidade de ferramentas externas. O Job Workflow no Databricks é especialmente útil para pipelines que utilizam exclusivamente a plataforma Databricks, aproveitando a escalabilidade e o gerenciamento automático de clusters.

### Vantagens:
- **Facilidade de uso**: Criação de jobs diretamente na interface do Databricks.
- **Escalabilidade automática**: O Databricks gerencia automaticamente a alocação de recursos para a execução das tarefas.
- **Monitoramento integrado**: Logs e métricas são fornecidos pela interface, facilitando a visualização do status do job.

### Desvantagens:
- **Não tem integração com ferramentas externas**: Apesar de ser menos complexo do que o Airflow é menos integravél com ferramentas externa também.
- **Acoplamento com a ferramenta Databricks**

**Aplicação no seu notebook**:
Você pode criar um Job Workflow que inclua cada parte do seu notebook como uma tarefa. Por exemplo, a ingestão de dados em Parquet, a limpeza dos dados, a criação de tabelas e a avaliação da qualidade dos dados.

---

## 2. **Apache Airflow para Orquestração de Pipelines**

**Apache Airflow** é uma plataforma de orquestração de workflows mais robusta e flexível. Com o Airflow, é possível criar **DAGs** (Directed Acyclic Graphs) que representam o fluxo de execução do seu pipeline, onde cada "task" pode ser configurada para executar um bloco de código Python ou PySpark. O Airflow é ideal para fluxos de trabalho mais complexos, com integração de múltiplas ferramentas e sistemas.

### Vantagens:
- **Flexibilidade**: Airflow permite definir tarefas e dependências complexas, facilitando a personalização de fluxos de trabalho.
- **Integração com sistemas externos**: Pode ser facilmente integrado com diversas fontes de dados, ferramentas de processamento e sistemas de notificação.
- **Escalabilidade e controle**: Oferece controle detalhado sobre a execução das tarefas, incluindo a possibilidade de rodar as tasks de forma paralela ou sequencial, além de monitorar e gerenciar a execução em tempo real.

### Vantagens:
- **Mais complexidade no desenvolvimento e monitoramento da pipeline**

### Como aplicar no seu notebook:
Cada **task** no Airflow pode corresponder a uma célula ou um conjunto de células do seu notebook. Cada task será responsável por uma parte do processo, como ingestão de dados, limpeza de dados, geração de tabelas e monitoramento de qualidade.

Abaixo está um exemplo de **DAG** para orquestrar a execução do seu pipeline usando o **Airflow** e o **PythonOperator**, com a execução de PySpark para cada tarefa.

---

## Exemplo de DAG no Apache Airflow

Aqui está um exemplo de **DAG** em Apache Airflow para orquestrar o seu pipeline de dados. Este código assume que você já tem o Airflow configurado e funcionando no seu ambiente. A DAG será agendada para rodar diariamente (`schedule_interval='@daily'`).

### Estrutura do código:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

# Funções que representam as tarefas do notebook (cada uma será uma task no DAG)

# Função de ingestão e conversão para Parquet
def ingest_data():
    # Inicia o SparkSession
    spark = SparkSession.builder.appName("fraud_credit_pipeline").getOrCreate()

    # Definindo o caminho do arquivo CSV
    file_location = "/FileStore/tables/df_fraud_credit.csv"
    df = spark.read.csv(file_location, header=True, inferSchema=True)
    
    # Convertendo para Parquet
    parquet_location = "/FileStore/tables/df_fraud_credit.parquet"
    df.write.parquet(parquet_location, mode='overwrite')

    print(f"Arquivo Parquet salvo em: {parquet_location}")
    
# Função de limpeza de dados
def clean_data():
    # Inicia o SparkSession
    spark = SparkSession.builder.appName("fraud_credit_pipeline").getOrCreate()

    # Carregar o Parquet
    parquet_location = "/FileStore/tables/df_fraud_credit.parquet"
    df = spark.read.parquet(parquet_location)

    # Limpeza: Remover valores nulos e conversão de tipos
    df_cleaned = df.dropna()
    df_cleaned = df_cleaned.withColumn("timestamp", df_cleaned["timestamp"].cast("timestamp"))
    
    # Salvar dados limpos novamente em Parquet
    cleaned_location = "/FileStore/tables/df_fraud_credit_cleaned.parquet"
    df_cleaned.write.parquet(cleaned_location, mode='overwrite')

    print(f"Dados limpos salvos em: {cleaned_location}")

# Função para calcular a média do 'risk_score' por 'location_region'
def calculate_avg_risk():
    # Inicia o SparkSession
    spark = SparkSession.builder.appName("fraud_credit_pipeline").getOrCreate()

    # Carregar os dados limpos
    cleaned_location = "/FileStore/tables/df_fraud_credit_cleaned.parquet"
    df_cleaned = spark.read.parquet(cleaned_location)

    # Calcular a média do 'risk_score' por 'location_region'
    from pyspark.sql import functions as F
    location_risk_avg = df_cleaned.groupBy("location_region").agg(
        F.avg("risk_score").alias("avg_risk_score")
    ).orderBy("avg_risk_score", ascending=False)

    # Salvar a tabela de resultados
    avg_risk_location_location = "/FileStore/tables/location_risk_avg.parquet"
    location_risk_avg.write.parquet(avg_risk_location_location, mode='overwrite')

    print(f"Tabela de média de risk_score por location_region salva em: {avg_risk_location_location}")

# Função para calcular o top 3 'receiving_address' com maior 'amount' para transações mais recentes de tipo 'sale'
def get_top_receiving_addresses():
    # Inicia o SparkSession
    spark = SparkSession.builder.appName("fraud_credit_pipeline").getOrCreate()

    # Carregar os dados limpos
    cleaned_location = "/FileStore/tables/df_fraud_credit_cleaned.parquet"
    df_cleaned = spark.read.parquet(cleaned_location)

    # Filtrar transações do tipo 'sale'
    sale_transactions = df_cleaned.filter(df_cleaned["transaction_type"] == "sale")
    
    # Usar janela para pegar transações mais recentes
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("receiving_address").orderBy(F.col("timestamp").desc())
    
    sale_transactions_recent = sale_transactions.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1)  # Transação mais recente para cada 'receiving_address'
    
    # Selecionar os 3 maiores 'amount'
    top_3 = sale_transactions_recent.orderBy(F.col("amount").desc()).limit(3)
    
    # Salvar a tabela de resultados
    top_3_location = "/FileStore/tables/top_3_receiving_addresses.parquet"
    top_3.write.parquet(top_3_location, mode='overwrite')

    print(f"Top 3 receiving_addresses salvos em: {top_3_location}")

# Função de monitoramento de qualidade dos dados
def monitor_data_quality():
    # Inicia o SparkSession
    spark = SparkSession.builder.appName("fraud_credit_pipeline").getOrCreate()

    # Carregar os dados limpos
    cleaned_location = "/FileStore/tables/df_fraud_credit_cleaned.parquet"
    df_cleaned = spark.read.parquet(cleaned_location)

    # Calcular métricas de qualidade (nulos, tipos de dados, duplicatas, etc.)
    total_records = df_cleaned.count()
    null_counts = {col: df_cleaned.filter(df_cleaned[col].isNull()).count() for col in df_cleaned.columns}
    
    # Gerar relatório de qualidade
    quality_report = {
        "total_records": total_records,
        "null_counts": null_counts,
        "error_percentage": sum(null_counts.values()) / total_records * 100
    }

    print(f"Relatório de qualidade de dados: {quality_report}")

# Definir a DAG
dag = DAG(
    'pipeline_fraud_credit',  # Nome da DAG
    description='Pipeline de Análise de Crédito Fraudulento',
    schedule_interval='@daily',  # Agendamento diário
    start_date=datetime(2024, 11, 11),  # Data de início
    catchup=False  # Não rodar execuções anteriores
)

# Definir as tasks
task1 = PythonOperator(task_id='ingest_data', python_callable=ingest_data, dag=dag)
task2 = PythonOperator(task_id='clean_data', python_callable=

clean_data, dag=dag)
task3 = PythonOperator(task_id='calculate_avg_risk', python_callable=calculate_avg_risk, dag=dag)
task4 = PythonOperator(task_id='get_top_receiving_addresses', python_callable=get_top_receiving_addresses, dag=dag)
task5 = PythonOperator(task_id='monitor_data_quality', python_callable=monitor_data_quality, dag=dag)

# Definir as dependências entre as tasks
task1 >> task2 >> task3 >> task4 >> task5
```

### Explicação:

- **DAG Nomeada**: A DAG é nomeada como `pipeline_fraud_credit` e está configurada para rodar diariamente com `schedule_interval='@daily'`.
- **PythonOperator**: Cada tarefa do pipeline é uma `PythonOperator`, que executa funções Python (neste caso, funções que executam código PySpark). Cada uma dessas funções mapeia para uma célula ou conjunto de células do notebook.
- **Funções de Tarefa**: As funções `ingest_data`, `clean_data`, `calculate_avg_risk`, `get_top_receiving_addresses`, e `monitor_data_quality` correspondem às tarefas definidas no seu notebook.
- **Dependências**: As dependências entre as tarefas são definidas com `task1 >> task2 >> task3 >> task4 >> task5`, o que significa que cada task será executada sequencialmente após a anterior.

---

## Conclusão

Ambas as abordagens, **Job Workflows no Databricks** e **Apache Airflow**, são eficazes para orquestrar a execução do pipeline. A escolha entre uma e outra depende do seu ambiente e dos requisitos do projeto.

- **Job Workflows no Databricks** é ideal se você deseja uma solução simples e integrada dentro da plataforma Databricks.
- **Apache Airflow** oferece maior flexibilidade e controle, sendo ideal para pipelines mais complexos ou quando há necessidade de orquestrar ferramentas externas além do Databricks.

Ambas as abordagens ajudam a garantir que o pipeline de dados seja executado de forma eficiente, escalável e monitorada.
