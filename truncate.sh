#!/bin/bash

### Definicao de variaveis
PSQL="psql -d torkcapital"
DUMP="/home/ubuntu/dump/dados_covid_19"

export PSQL DUMP

horario()
{
date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

LoadTruncate() {
   TABLE=$1
   time(echo -e "\n"
   if [[ $TABLE -eq "brazil_cities" ]]; then
      $PSQL -c "INSERT INTO covid_19.${TABLE}_hist TABLE covid_19.${TABLE}_stg EXCEPT TABLE covid_19.${TABLE}_hist"
   else
      $PSQL -c "INSERT INTO covid_19.${TABLE}_hist TABLE covid_19.${TABLE}_stg"
   fi
   $PSQL -c "TRUNCATE covid_19.${TABLE}_stg;"
   $PSQL -c "VACUUM ANALYZE covid_19.${TABLE}_hist"
   echo -e "$(horario): Tabela ${TABLE}_stg truncada.\n")
}
export -f LoadTruncate

RemoveFile() {
   FILE=$1
   echo "Removing ${FILE}..."
   rm -f $FILE
}
export -f RemoveFile

### Carrega dados nas tabelas hist

listaDeTabelas="coronavirus_who italy brazil usa spain brazil_cities usa_cities"

TotalTabelas=$(echo $listaDeTabelas | wc -w)
parallel LoadTruncate {}\; 'echo -e "\nProgress: {#}/'$TotalTabelas'\n"' ::: $listaDeTabelas

cd $DUMP
files="cities.txt"

parallel RemoveFile {} ::: $files

exit 0