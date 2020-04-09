#!/bin/bash

### Definicao de variaveis
PSQL="psql -d torkcapital"

export PSQL

horario()
{
date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

LoadTruncate() {
   TABLE=$1
   time(echo -e "\n"
   $PSQL -c "INSERT INTO covid_19.${TABLE}_hist TABLE covid_19.${TABLE}_stg"
   $PSQL -c "TRUNCATE covid_19.${TABLE}_stg;"
   $PSQL -c "VACUUM ANALYZE covid_19.${TABLE}_hist"
   echo -e "$(horario): Tabela ${TABLE}_stg truncada.\n")
}
export -f LoadTruncate


### Carrega dados nas tabelas hist

listaDeTabelas="coronavirus_who italy brazil usa spain"

TotalTabelas=$(echo $listaDeTabelas | wc -w)
parallel LoadTruncate {}\; 'echo -e "\nProgress: {#}/'$TotalTabelas'\n"' ::: $listaDeTabelas

exit 0
