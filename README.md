# Case Ifood
Repositório destinado a armazenar os arquivos utilizados no case do ifood para o cargo de Data Analytics SR
---
## Etapas para execuçao deste projeto:
### Etapas do ETL:
#### 1) Instalar o Python 3.12.4 ou alguma versão estável
#### 2) Criar uma pasta localmente para armazenar a chave de conta de serviço que será utilizada para conectar no BQ
#### 3) Executar primeiro o arquivo importar_bibliotecas_necessarias.jpynb, garantindo assim que todas as bibliotecas estarão funcionando.
#### 4) Bibliotecas instaladas, executar o script etl_camada_bronze.jpynb para que os dados do bucket do GCP sejam baixados e carregados como tabelas no BigQuery, na camada bronze.
#### 5) Seguir depois para o script etl_camada_silver.jpynb para que sejam feitas as devidas transformações e carga de dados das tabelas na camada silver.
#### 6) Por fim, executar o script etl_camada_gold.jpynb para que a tabela final seja criada na camada gold, através desta tabela que serão feitas as análises;
---
### Etapas de Análises:
#### 1) Executar o script questao_1a.jpynb para obter os resultados da análise feita para essa questão.
#### 2) Executar o script questao_1b.jpynb para obter os insights da pergunta sobre Viabilidade Financeira do Teste A/B
