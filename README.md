# **Projeto Aplicado (MBA - XPEducação 2022)**

### Projeto Aplicado para o curso de MBA de Engenharia de Dados da XPEducação


**Resumo:** 

Construção de um pipeline de dados escalável na AWS utilzando jobs em Spark com orquestração no Apache Airflow.

Para nosso Projeto Aplicado idealizamos um cenário fictício onde a empresa trabalha com um pipeline que executa cargas de trabalho em Spark no cluster EMR (ambiente AWS), onde também possui o seu datalake. As tarefas são executadas sob demanda e trabalham com uma faixa de tempo por vez. 
 
![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/6f9eb14c-aac8-45ee-a1cb-e399d2e3c9a5)


**OBS.:** Estes são dados comerciais reais, foram anonimizados e as referências às empresas e parceiros no texto da revisão foram substituídas pelos nomes das principais casas de Game of Thrones.Os dados trabalhados se encontram em https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_order_items_dataset.csv![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/aec18eef-8325-47f0-9498-07abd2878015)



## Problema

Os dados são recebidos e lidos, processados por um cluster EMR através de tarefas em Spark, depois enviados aos buckets para serem depois acessados pela áreas interessadas. A pipeline ETL segue o diagrama abaixo.
Temos um ambiente com tarefas Spark rodando dentro de um cluster EMR e carregando dados em buckets no ambiente AWS.
![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/0c186922-4586-4002-99a3-c22347cc96d9)

Alguns pontos onde é possível fazer melhorias:

* Performance
* Eficiência de custo
* Tolerância a falhas


## Solução

O pipeline proposto com o cluster Kubernetes executa tarefas do Spark e é orquestrado pelo Airflow. Os dados são extraidos da landing zone, tratados e enviados para a zona de entrega ao fim do processo. Adicionamos um crawler para enviar dados específicos para um database e preencher uma tabela que poderá ser consultada no Athena utilizando linguagem SQL.

![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/bde9d388-145e-456e-8523-3dd1ff1937a3)

Para a parte de código das tarefas Spark, configuramos um repositório Github onde s desenvolvedores poderão editar e administrar as DAGs.

* Criação de buckets no Datalake via Terraform
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/387e8954-0f27-40d6-94db-27b7e8c1d6e9)

* Script eksctl para criação do Cluster EKS
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/9df61c90-36d7-4531-b90c-b96b6a845ed3)

* Deploy do Airflow no Kubernetes
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/d7fedd66-b61f-4c53-8f8a-0463130541d9)

* Código YAML para configurar o Github
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/ea7ab2a3-47a6-4afc-9a15-207155d9d954)

* Pod do Airflow no cluster Kubernetes
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/2dc110f9-68fa-4b1f-9d82-9d3e1d0381d6)

* Código pyspark para o Airflow
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/0ca2fa36-9648-44f3-95a3-e6c89c2e164e)

* DAG
  
  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/ecaaf201-7f67-4258-ae0a-6e0c1ee5e4f4)

* Airflow executando os jobs
  
![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/e9eea4ea-7ab7-4959-bc3e-fc64e631bbc2)


* Pipeline final

  ![image](https://github.com/wca01/Projeto_Aplicado_mba_xpe22/assets/105304356/a81250a3-2c85-41e0-b5dd-64b44b6206b2)

Pipeline completo, utilizando o EKS, Airflow, tarefas Spark, DAGs remotas no Github e consulta de dados no Athena.






