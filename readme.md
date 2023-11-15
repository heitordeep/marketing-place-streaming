# Aplicação Streaming de Marketplace do Itaú

## Objetivo:
O objetivo desta aplicação é realizar a ingestão contínua de dados de marketplace do Itaú em streaming, utilizando Pyspark. O objetivo final é disponibilizar os dados processados para as equipes de analistas e dados.

# Etapas de execução:

## Configuração Inicial

- Realize configurações de permissões nas pastas para permitir que o Spark escreva arquivos temporários:
    - ```shell 
      $ make config
        ```   
- Crie os containers Spark e o simulador AWS:
    - ```shell 
      $ make up
        ```
- Construa o bucket S3 executando o comando:
    - ```shell 
      $ make bucket-s3 bucket=itau-shop action=create
        ```

## Execução do Job Spark Streaming

- Inicie o processo de streaming com o Spark:
    - ```shell 
      $ make run-job
      ```
    - Após este comando, o Spark realizará a leitura dos dados do diretório do bucket com o caminho ```input/*.json```.

- Movendo arquivos para o bucket:
    - ```shell 
      $ make bucket-s3 bucket=itau-shop action=move_obj
      ```
    - Observação: Os arquivos devem estar localizados na pasta chamada 'input'. Essa pasta tem um espelho no container do spark-master, permitindo a transferência para o simulador do S3.

- Listando os arquivos no bucket:
    - Listar na raiz:
        - ```shell 
          $ make bucket-s3 bucket=itau-shop action=list
            ```
    - listar por pasta:
        - ```shell 
          $ make bucket-s3 bucket=itau-shop action=list path=input/
            ```
        - Observação: Tentar acessar uma pasta inexistente resultará em um erro: *Path does not exist*

    - Listar todos os buckets:
        - ```shell 
          $ make bucket-s3 bucket=all action=all
            ```


# Etapa do Fluxo de Dados

Os dados são capturados na pasta input/*.json do bucket criado. Os dados são então enviados em seu estado original para a camada "raw", na pasta raw/marketing_place, garantindo a preservação dos dados originais. Após a etapa de ingestão na camada "raw", o tratamento dos dados é realizado.

Quatro tabelas são construídas para as equipes analises e dados:
- orders (primary key: **txt_detl_idt_pedi_pgto**)
- products (primary key: **txt_detl_idt_pedi_pgto**)
- sellers (primary key: **idt_venr**)
- shipments (primary key: **txt_detl_idt_pedi_pgto**)

Todas as tabelas são atualizadas após a inserção, realizando apenas inserção em dados atuais.

# Consulta de Dados

Para consultar os dados, é possível utilizar o spark-shell. Execute o seguinte comando:

```shell
$ make exec-bash container=spark-master
```

```shell
$ spark-shell \
    --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.access.key=dummy \
    --conf spark.hadoop.fs.s3a.secret.key=dummy \
    --conf spark.hadoop.fs.s3a.path.style.access=true
```

# Reutilização do Código:

Para reutilizar o código, importe a classe responsável pela execução do streaming, a classe de processo, bem como os esquemas necessários. Exemplo:

```python
from models import Order
from schemas import OrderSchema
from submit import SparkSubmit

if __name__ == "__main__":

    SparkSubmit().run(
        bucket_name='s3a://itau-shop',
        input_path='input/*.json',
        output_path='analytics',
        process_name='marketing_place',
        process=Order,
        schema=OrderSchema().get()
    )
```

# Recomendações

- É necessário criar o esquema do seu job e a classe de processo responsável pela regra de negócio. A interface é obrigatória para garantir a estrutura lógica do processo. 

- Estrutura de Pasta: 
  - interface: Script Ao utilizar essa interface junto aos scripts na pasta models, facilita-se a manutenção do código, uma vez que garante a presença e correta implementação dos métodos requeridos pelos diversos componentes do projeto.

  - models: A pasta models, por sua vez, contém os arquivos responsáveis pela lógica de negócios do sistema. Todos os processos e fluxos de tratamento de dados são implementados nessa pasta. Esses scripts representam a parte central do projeto, onde são aplicadas as transformações, análises e manipulações dos dados de acordo com as regras definidas.

  - schema: detém o esquema (schema) dos dados que são lidos durante o processo de streaming com o Pyspark. Esses esquemas definem a estrutura e o formato dos dados que serão lidos, facilitando a correta interpretação e manipulação dos mesmos pelo sistema de streaming.

  - submit: é responsável por conter o script responsável pela execução do Pyspark streaming. Este diretório é essencial para realizar a execução do fluxo de streaming, permitindo o gerenciamento e a execução adequada do processo de streaming por meio do Spark.

  - process: Por fim, a pasta process compreende o fluxo preparado para a execução em streaming. Ela encapsula as etapas necessárias para o processamento contínuo dos dados. Isso inclui a escuta dos dados no bucket especificado, a invocação das classes de transformação presentes na pasta models e a disponibilização do dado final, após todo o processamento ser concluído.



