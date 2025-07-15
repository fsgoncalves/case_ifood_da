# Case Ifood
Repositório destinado a armazenar os arquivos utilizados no case do ifood para o cargo de Data Analytics SR
---
## Etapas para execuçao deste projeto:
### Configuração do projeto:
#### 1) Instalar o Python 3.12.4 ou alguma versão estável
#### 2) Criar uma pasta localmente para armazenar a chave de conta de serviço que será utilizada para conectar no BQ
#### 3) Fazer download do repositório e salvar em uma pasta do computador
#### 4) Executar primeiro o arquivo instalar_bibliotecas_necessarias.jpynb, garantindo assim que todas as bibliotecas estarão funcionando.
#### 5) No arquivo utils.py, editar a variável pasta_projeto colocando a raíz da pasta onde foi salvo este repositório no computador local

### Etapas do ETL:
#### 1) Bibliotecas instaladas, executar o script etl_camada_bronze.jpynb para que os dados do bucket do GCP sejam baixados e carregados como tabelas no BigQuery, na camada bronze.
#### 2) Seguir depois para o script etl_camada_silver.jpynb para que sejam feitas as devidas transformações e carga de dados das tabelas na camada silver.
#### 3) Por fim, executar o script etl_camada_gold.jpynb para que a tabela final seja criada na camada gold, através desta tabela que serão feitas as análises;
---
### Etapas de Análises:
#### 1) Executar o script questao_1.jpynb para obter os resultados da análise feita para essa questão.
#### 2) Executar o script questao_2.jpynb para obter os resultados da análise feita para essa questão.
