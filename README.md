macarronETL
============================
Um exemplo didático de como **NÃO** escrever uma aplicação ETL

Utiliza o [Criminal Reports](http://data.dc.gov/) do Distrito de Columbia como fonte de dados.

* Realiza parsing de arquivos de dados CSV (fácil, é só vírgula mesmo...)
* Transforma os dados e elimina inconsistências
* Realiza a carga dos dados em um modelo de Data warehouse pré-definido

Configurando a conexão com o banco de dados
-------------------------------------------

De modo provisório, os dados de conexão (USER,PASSWD e DATABASE) devem
ser configurados dentro do arquivo DBConn.java.
A alteração desses valores exige a recompilação da classe.
