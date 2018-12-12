# Cloud Function - Transferência de arquivos

Função que transfere, um a um, arquivos em uma localização para outra (i.e. copia arquivos de um FTP para um bucket GCS).
A função é baseada em uma mensagem do Pub/Sub que deve ser um JSON no seguinte formato:
```
{
    "source_connection_string": "ftp://FTP/TEMP?username=user&password=pass",
    "destination_connection_string": "gs://BUCKET/temp/",
    "remove_file": False,
    "compress_algorithm": "zip",
    "decompress_algorithm": "zip"
}
```
O atributo `remove_file` determina se o arquivo deve ou não ser removido da origem caso ele seja copiado com sucesso para o destino.

Os atributos `compress_algorithm` e `decompress_algorithm` determinam que o arquivo deve ser compactado ou descompactado, respectivamente, antes de ser enviado para o destino.

As connection strings devem seguir o padrão de URI. Atualmente suportamos as seguintes conexões:

* FTP - `ftp://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD`
* SFTP - `sftp://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD`
* GCS - `gs://BUCKET/PATH/`

Os tipos de compactação atualmente suportados são os seguintes:

* gzip
* zip

**ATENÇÃO**: os arquivos compactados recebidos como origem devem, obrigatoriamente, conter apenas um arquivo. Da mesma forma, cada arquivo enviado para o destino será compactado em seu próprio arquivo.

Para dar um padrão mínimo de consistência, os métodos de listar arquivos usados internamente listam todos os arquivos no nível da última parte do PATH e então fazemos o filtro via [fnmatch](https://docs.python.org/3.4/library/fnmatch.html) na parte final.

Por exemplo, vamos supor um PATH = `/ARQUIVOS/*_log.txt`. Primeiro listamos todos os arquivos em `/ARQUIVOS/`. Nessa lista, fazemos o filtro `*_log.txt`. Com isso, pelo menos no último nível teremos um comportamento igual em todos os tipos de conexão (ou seja, isso reduz a inconsistência no fato que uma conexão FTP aceita *wildcards* em qualquer local, ao passo que o GCS só permite ter um prefixo).

**ATENÇÃO**: nenhuma validação é feita no caminho para o diretório. Ou seja, uma requisição do tipo `/ARQUIVOS/*/*_log.txt` irá funcionar para conexões FTP mas não em GCS. Lembre-se que a função só é recomendada em volumes relativamente pequenos de dados/arquivos.

## Environment Variables

* **PROJECT** = Id do projeto (ex: modular-aileron-191222)

## Deployment

Publicar como Google Cloud Function - ambiente Python 3.7 com as variáveis acima. Utilizar um trigger de mensagem do PubSub.

## Built With

* [Python](https://www.python.org/) - Runtime Environment

### Autores

* [**Lucas Rosa**](https://bitbucket.org/dotz-lucas-rosa/)

[![Dotz](https://dotz.com.br/assets/dotz/img/new-site-content/logo_dotz.jpg)](https://dotz.com.br/)
