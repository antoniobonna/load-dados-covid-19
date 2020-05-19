#!/bin/bash

### Definicao de variaveis
LOG="/var/log/scripts/scripts.log"
DIR="/home/ubuntu/scripts/load-dados-covid-19"
STARTDATE=$(date +'%F %T')
SCRIPTNAME="load-dados-covid-19.sh"

export DIR

horario()
{
	date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

stagingDados()
{
	FILE=$1
	time python ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n"
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

ListaArquivos="load_dados_worldmeters.py load_dados_us.py load_dados_brazil.py load_dados_italy.py load_dados_spain.py load_dados_tests.py \
load_world_in_data.py load_dados_healthcare.py load_brazil_cities.py load_us_cities.py crawler_hospitalization_rj.py load_hospitalization_sp.py \
load_hospitalization_pe.py load_hospitalization_ma.py crawler_hospitalization_pa.py crawler_hospitalization_pb.py load_hospitalization_al.py \
load_hospitalization_maceio.py load_hospitalization_se.py load_hospitalization_pr.py load_hospitalization_sc.py load_hospitalization_ro.py \
load_beds_sus.py"

TotalTabelas=$(echo $ListaArquivos | wc -w)
parallel -k stagingDados {}\; 'echo -e "\nProgress: {#}/'$TotalTabelas'\n"' ::: $ListaArquivos

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
