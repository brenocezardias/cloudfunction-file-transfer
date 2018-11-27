# Cloud Function - Transferência de arquivos

Função que transfere, um a um, arquivos em uma localização para outra (i.e. copia arquivos de um FTP para um bucket GCS).
A função é baseada em uma mensagem do Pub/Sub que deve ser um JSON no seguinte formato:
```
{
    "source_connection_string": "ftp://FTP/TEMP?username=user&password=pass",
    "destination_connection_string": "gs://BUCKET/temp/",
    "remove_file": False
}
```
O atributo `remove_file` determina se o arquivo deve ou não ser removido da origem caso ele seja copiado com sucesso para o destino.

As connection strings devem seguir o padrão de URI. Atualmente suportamos as seguintes conexões:

* FTP - ftp://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD
* GCS - gs://BUCKET/PATH/

As possibilidades de filtros variam de acordo com o tipo de conexão. O FTP permite usar o `*` em várias posições para criar filtros, ao passo que o GCS só permite filtros por prefixo.

## Environment Variables

* **PROJECT** = Id do projeto (ex: dotzcloud-datalabs-sandbox)

## Deployment

Publicar como Google Cloud Function - ambiente Python 3.7 com as variáveis acima. Utilizar um trigger de mensagem do PubSub.

## Built With

* [Python](https://www.python.org/) - Runtime Environment

### Autores

* [**Lucas Rosa**](https://bitbucket.org/dotz-lucas-rosa/)

[![Dotz](https://dotz.com.br/assets/dotz/img/new-site-content/logo_dotz.jpg)](https://dotz.com.br/)
