macarronETL
============================
Um exemplo didático de como **NÃO** escrever uma aplicação ETL

Utiliza o [Criminal Reports](http://data.dc.gov/) do Distrito de Columbia como fonte de dados.

* Realiza parsing de arquivos de dados CSV (fácil, é só vírgula mesmo...)
* Transforma os dados e elimina inconsistências
* Realiza a carga dos dados em um modelo de Data warehouse pré-definido

Configurando a conexão com o banco de dados
-------------------------------------------

É possível alterar os parâmetros de configuração da conexão (connection, user, password) diretamente
no arquivo dbconn.cfg. O arquivo contém uma rápida documentação sobre o uso e alguns exemplos.
Dependendo do modo de execução do macarronETL (pelo Eclipse ou standalone via terminal) pode ser necessário
copiar o arquivo para o diretório de build das classes.
