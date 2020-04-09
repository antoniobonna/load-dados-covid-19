#!/bin/bash

### Definicao de variaveis
LOG="/var/log/scripts/scripts.log"
DIR="/home/ubuntu/scripts/load-dados-covid-19"
STARTDATE=$(date +'%F %T')
SCRIPTNAME="load-dados-covid-19.sh"

horario()
{
	date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

stagingDados()
{
	FILE=$1
	time python ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f stagingDados

LoadDW()
{
	FILE=$1
	time psql -d torkcapital -f ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f LoadDW


### Carrega arquivos nas tabelas staging

echo -e "$(horario): Inicio do staging.\n-\n"

ListaArquivos="load_dados_worldmeters.py load_dados_us.py load_dados_brazil.py crawler_italy.py load_dados_spain.py load_dados_tests.py load_data_world_in_data.py"
for FILE in $ListaArquivos; do
	stagingDados $FILE
done

### Carrega dados no DW

echo -e "$(horario): Inicio da carga no DW.\n-\n"

ListaArquivos="etl_covid_19.sql"
for FILE in $ListaArquivos; do
	LoadDW $FILE
done

### Limpa tabelas staging e carrega no historico

echo -e "$(horario): Inicio da limpeza do staging.\n-\n"

${DIR}/truncate.sh

### Remove arquivos temporarios e escreve no log

ENDDATE=$(date +'%F %T')
echo "$SCRIPTNAME;$STARTDATE;$ENDDATE" >> $LOG

echo -e "$(horario):Fim da execucao.\n"

exit 0
