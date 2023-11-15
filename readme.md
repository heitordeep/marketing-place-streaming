# Aplicação Streaming de marketing place

## Objetivo:
Realizar ingestão dos dados de marketing place do Itau em streaming via Pyspark.
Com o objetivo de disponibilizar os dados para o time de análitico .

# Etapas de execução:

Primeiro, é preciso realizar as configurações da aplicação para que seja possível executar o job spark streaming. Para isso, segue as etapas a seguir:

- É preciso realizar algumas configurações de permissões das pastas, para que o spark consiga escrever os arquivos temporários
    - ```shell 
      $ make config
      ```

- Após realizar o comando de config, é preciso criar os containers spark e o simulador aws
    - ```shell 
      $ make up
      ```

- Para realizar a construção do bucket s3, basta executar o seguinte comando:
    - ```shell 
      $ make create-bucket
      ```
    - OBS: O nome do bucket é fixado com o nome itau-shop

- Com o bucket criado, basta executar no terminal o comando de start do spark submit
    - ```shell 
      $ make run-job
      ```
    Após esse comando, o spark irá realizar a leitura dos dados do diretório do bucket, com o caminho ```input/*.json```

- Mover os arquivos para o bucket:
    - ```shell 
      $ make move-file-bucket
      ```
    - OBS: Os arquivos precisam estar na pasta local que se chama input. A pasta input possui espelho no container do spark-master, assim é possível realizar a transferencia para o simulador do s3.


## Etapa do fluxo:

Os dados são capturados na pasta input/*.json do bucket criado. Com isso, o dado é enviado em seu estado original para a camada raw, na pasta raw/marketing_place para garantir o dado original. Após a etapa de ingestão na camada raw, é realizado o tratamento dos dados.

É construído 4 tabelas para o time de análiticos: 
    - orders
        - primary key: txt_detl_idt_pedi_pgto
    - products
        - primary key: txt_detl_idt_pedi_pgto
    - sellers
        - primary key: idt_venr
    - shipments
        - primary key: txt_detl_idt_pedi_pgto

Todas as tabelas, é feito atualização após a inserção, fazendo somente o insert em dados atuais. 

Para conferir os dados, é possível realizar consultas via spark-shell, basta executar o seguinte comando:

```shell
$ spark-shell \
    --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.access.key=dummy \
    --conf spark.hadoop.fs.s3a.secret.key=dummy \
    --conf spark.hadoop.fs.s3a.path.style.access=true
```

 

