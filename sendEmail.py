import yagmail
import credentials
from datetime import date,datetime,timedelta
from locale import setlocale,LC_TIME
import html.entities
from PIL import Image
from pdf2image import convert_from_path, convert_from_bytes
from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)

table = {k: '&{};'.format(v) for k, v in html.entities.codepoint2name.items()}
GMAIL_USERNAME, GMAIL_PASSWORD = credentials.setEmailLogin()
SENDER_EMAIL = 'abonna@torkcapital.com.br'
setlocale(LC_TIME, 'pt_BR')
current_date = date.today()-timedelta(days=1)
str_date = current_date.strftime('%d/%m/%Y')
_file = 'painel coronavirus - email.pdf'
indir = '/home/ubuntu/dump/dados_covid_19/'
subject_str = current_date.strftime('Coronavirus - Evolution (Até %d/%b/%Y)')
subject_str = subject_str.replace(current_date.strftime('/%b/'),current_date.strftime('/%b/').title())

def parsePDF(_file):
    outdir = '/home/ubuntu/dump/dados_covid_19/images/'
    images = {}
    image_list = ['Rio_de_Janeiro.jpg',
                'Rio_de_Janeiro_SUS.jpg',
                'Sao_Paulo.jpg',
                'Sao_Paulo_Occupation.jpg',
                'Pernambuco.jpg',
                'Pernambuco_Occupation.jpg',
                'Ceara.jpg',
                'Ceara_Occupation.jpg',
                'Sao_Luis.jpg',
                'Sao_Luis_Occupation.jpg',
                'Maranhao.jpg',
                'Maranhao_Occupation.jpg',
                'Rio_Grande_do_Sul.jpg',
                'Rio_Grande_do_Sul_Occupation.jpg',
                'Hapvida.jpg',
                'Hapvida_2.jpg',
                'Hapvida_3.jpg',
                'Brazil.jpg',
                'Brazil_2.jpg',
                'Cases_State.jpg',
                'Cases_State_2.jpg',
                'Deaths_State.jpg',
                'Deaths_State_2.jpg',
                'States_Comparison.jpg',
                'Cities.jpg',
                'Cities_2.jpg',
                'Cities_Comparison.jpg',
                'SRAG.jpg',
                'World.jpg',
                'World_2.jpg',
                'Cases_by_Country.jpg',
                'After_1000th.jpg',
                'After_50th.jpg',
                'Tests.jpg',
                'United States.jpg',
                'United States_2.jpg',
                'Deaths_State_3.jpg',
                'Deaths_State_4.jpg',
                'Cities_3.jpg',
                'Cities_4.jpg',
                'Rio_de_Janeiro_Occupation.jpg',
                'Rio_de_Janeiro_Fila.jpg',
                'Sao_Paulo_State.jpg',
                'Sao_Paulo_State_Occupation.jpg',
                'Top_10_Cases.jpg',
                'Top_10_Deaths.jpg',
                'Cases_After_Lockdown.jpg',
                'Deaths_After_Lockdown.jpg',
                'Sao_Paulo_Interior.jpg',
                'Sao_Paulo_Interior_2.jpg',
                'Rio_de_Janeiro_Interior.jpg',
                'Rio_de_Janeiro_Interior_2.jpg',
                'Ceara_Interior.jpg',
                'Ceara_Interior_2.jpg',
                'Amazonas_Interior.jpg',
                'Amazonas_Interior_2.jpg',
                'Amazonas.jpg',
                'Brazil_RT.jpg',
                'Traffic_change.jpg',
                'Para.jpg',
                'Para_Occupation.jpg',
                'Para_Interior.jpg',
                'Para_Interior_2.jpg']
    pages = convert_from_path(_file,size=1300)

    for page,image in zip(pages,image_list):
        width, height = page.size
        if image in ('Rio_de_Janeiro.jpg','Sao_Paulo.jpg','Pernambuco.jpg','Ceara.jpg','Sao_Luis.jpg','Maranhao.jpg','Maceio.jpg','Alagoas.jpg',
                    'Sergipe.jpg','Rio_Grande_do_Sul.jpg','Parana.jpg','Rio_de_Janeiro_SUS.jpg','Sao_Paulo_State.jpg','Amazonas.jpg','Para.jpg'):
            page = page.crop((0, 135, width-2, height-20))
     
        elif image in ( 'Sao_Paulo_Occupation.jpg','Pernambuco_Occupation.jpg','Ceara_Occupation.jpg','Sao_Luis_Occupation.jpg',
                        'Maranhao_Occupation.jpg','Alagoas_Occupation.jpg','Sergipe_Occupation.jpg','Rio_Grande_do_Sul_Occupation.jpg',
                        'Parana_Occupation.jpg','Santa_Catarina_Occupation.jpg','Maceio_Occupation.jpg','Parana_Occupation.jpg',
                        'Deaths_State_3.jpg','Deaths_State_4.jpg','Rio_de_Janeiro_Occupation.jpg','Rio_de_Janeiro_Fila.jpg','Cities_3.jpg',
                        'Cities_4.jpg','Sao_Paulo_State_Occupation.jpg','Top_10_Cases.jpg','Top_10_Deaths.jpg','Cases_After_Lockdown.jpg',
                        'Deaths_After_Lockdown.jpg','Sao_Paulo_Interior.jpg','Sao_Paulo_Interior_2.jpg','Rio_de_Janeiro_Interior.jpg',
                        'Rio_de_Janeiro_Interior_2.jpg','Ceara_Interior.jpg','Ceara_Interior_2.jpg','Amazonas_Interior.jpg',
                        'Amazonas_Interior_2.jpg','Brazil_RT.jpg','Traffic_change.jpg','Para_Occupation.jpg','Para_Interior.jpg',
                        'Para_Interior_2.jpg'):
            page = page.crop((0, 22, width, height-20))
            
        elif image in ('Hapvida_3.jpg'):
            page = page.crop((0, 135, width-438, height-20))
        
        elif image in ('Santa_Catarina.jpg'):
            page = page.crop((0, 135, width-661, height-20))
        
        elif image in ( 'Cases_State.jpg','Deaths_State.jpg','Cases_by_Country.jpg','After_1000th.jpg','After_50th.jpg','Tests.jpg','Cities.jpg',
                        'Cities_2.jpg','Cases_State_2.jpg','Deaths_State_2.jpg'):
            page = page.crop((0, 135, width-2, height-20))
        
        elif image in ('States_Comparison.jpg'):
            page = page.crop((0, 122, width-2, height-20))
        
        elif image in ('Cities_Comparison.jpg'):
            page = page.crop((0, 116, width-2, height-20))
        
        elif image in ('SRAG.jpg'):
            page = page.crop((0, 164, width-18, height-162))
        
        else:
            page = page.crop((0, 185, width-2, height-216))
        
        page.save(outdir + image, format='JPEG', subsampling=0, quality=100)
        images[image] = outdir + image
    
    return images

def _Title(str_title):
    html_title = f'<h1 style="font-family:verdana"><u>{str_title.translate(table)}</u></h1>'
    return html_title

def _SubTitle(str_subtitle):
    html_subtitle = f'<font color="red"><u><h2 style="font-family:verdana">{str_subtitle.translate(table)}</h2></u></font>'.replace('0TH','0<sup>TH</sup>')
    return html_subtitle

def _SubSubTitle(str_subsubtitle):
    html_subsubtitle = f'<font color="#1f497d"><u><h3 style="font-family:verdana">{str_subsubtitle.translate(table)}</h3></u></font>'
    return html_subsubtitle

def _Image(image_str,images):
    image_file = images[image_str]

    return yagmail.inline(image_file)

begin_message = ' '.join(f'''<font color="#1f497d">
            <p>Prezados,
            <BR>
            Segue a {'atualização'.translate(table)} {'diária'.translate(table)} sobre a {'evolução'.translate(table)} do COVID-19 consolidado {'até'.translate(table)} o dia {str_date}.
            </p>
            <p><b><u>{'Vocês'.translate(table)} podem ter acesso ao painel completo com os dados sempre atualizados {'através'.translate(table)} do link:</b></u>
            <BR><BR>
            <a href="https://tinyurl.com/votsjp2">https://tinyurl.com/votsjp2</a>
            <BR><BR>
            <b>{'Usuário'.translate(table)}:</b> administrativo@torkcapital.com.br
            <BR>
            <b>Senha:</b> Trocar10
            </p>
            </font>
            <hr>'''.split())

if __name__=="__main__":
    images = parsePDF(indir+_file)
    yag = yagmail.SMTP(GMAIL_USERNAME, GMAIL_PASSWORD)
    yag.send(to = SENDER_EMAIL, 
            subject = subject_str, 
            contents = [
            begin_message,
            
            _Title('BRAZIL'),
            '<p>[Brazil]</p>',
            _Image('Brazil.jpg',images),
            _Image('Brazil_2.jpg',images),
            
            _SubTitle('BRAZIL – STATES'),
            '<p>[Brazil States]</p>',
            _SubSubTitle('TOTAL CASES BY STATE'),
            _Image('Cases_State.jpg',images),
            _Image('Cases_State_2.jpg',images),
            _SubSubTitle('DEATHS BY STATE'),
            _Image('Deaths_State_3.jpg',images),
            _Image('Deaths_State_4.jpg',images),
            _SubSubTitle('DEATHS PER MILLION BY STATE'),
            _Image('Deaths_State.jpg',images),
            _Image('Deaths_State_2.jpg',images),
            _SubSubTitle('DEATHS PER MILLION - BRAZIL x WORLD STATES'),
            _Image('States_Comparison.jpg',images),
            
            _SubTitle('BRAZIL – CITIES'),
            '<p>[Brazil Cities]</p>',
            _SubSubTitle('DEATHS BY CITY'),
            _Image('Cities_3.jpg',images),
            _Image('Cities_4.jpg',images),
            _SubSubTitle('DEATHS PER MILLION BY CITY'),
            _Image('Cities.jpg',images),
            _Image('Cities_2.jpg',images),
            
            _SubTitle('BRAZIL – EVOLUTION CAPITAL X INTERIOR'),
            _SubSubTitle('ESTADO DO RIO DE JANEIRO'),
            _Image('Rio_de_Janeiro_Interior.jpg',images),
            _Image('Rio_de_Janeiro_Interior_2.jpg',images),
            
            _SubSubTitle('ESTADO DE SÃO PAULO'),
            _Image('Sao_Paulo_Interior.jpg',images),
            _Image('Sao_Paulo_Interior_2.jpg',images),
            
            _SubSubTitle('ESTADO DO CEARÁ'),
            _Image('Ceara_Interior.jpg',images),
            _Image('Ceara_Interior_2.jpg',images),
            
            _SubSubTitle('ESTADO DO AMAZONAS'),
            _Image('Amazonas_Interior.jpg',images),
            _Image('Amazonas_Interior_2.jpg',images),
            
            _SubSubTitle('ESTADO DO PARA'),
            _Image('Para_Interior.jpg',images),
            _Image('Para_Interior_2.jpg',images),
            
            _Title('HOSPITALIZATION EVOLUTION'),
            '<p>[HOSPITALIZATION EVOLUTION]</p>',
            _SubTitle('CIDADE DO RIO DE JANEIRO - HOSPITAIS MUNICIPAIS'),
            _Image('Rio_de_Janeiro.jpg',images),            
            _SubTitle('CIDADE DO RIO DE JANEIRO - REDE SUS'),
            _Image('Rio_de_Janeiro_SUS.jpg',images),
            _SubTitle('CIDADE DO RIO DE JANEIRO - ICU OCCUPATION (SUS) AND NEW DEATHS'),
            _Image('Rio_de_Janeiro_Occupation.jpg',images),
            _SubSubTitle('DADOS DE NOTICIÁRIOS'),
            '<p>[HOSPITALIZATION EVOLUTION]</p>',
            _SubTitle('CIDADE DO RIO DE JANEIRO - FILA DE ESPERA POR LEITOS'),
            _Image('Rio_de_Janeiro_Fila.jpg',images),
            
            _SubTitle('ESTADO DE SÃO PAULO'),
            _Image('Sao_Paulo_State.jpg',images),
            _SubTitle('ESTADO DE SÃO PAULO - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Sao_Paulo_State_Occupation.jpg',images),
            
            _SubTitle('CIDADE DE SÃO PAULO - HOSPITAIS MUNICIPAIS'),
            _Image('Sao_Paulo.jpg',images),
            _SubTitle('CIDADE DE SÃO PAULO - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Sao_Paulo_Occupation.jpg',images),
            
            _SubTitle('PERNAMBUCO'),
            _Image('Pernambuco.jpg',images),
            _SubTitle('PERNAMBUCO - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Pernambuco_Occupation.jpg',images),
            
            _SubTitle('CEARÁ'),
            _Image('Ceara.jpg',images),
            _SubTitle('CEARÁ - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Ceara_Occupation.jpg',images),
            
            _SubTitle('SÃO LUÍS / MA'),
            _Image('Sao_Luis.jpg',images),
            _SubTitle('SÃO LUÍS / MA - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Sao_Luis_Occupation.jpg',images),
            
            _SubTitle('MARANHÃO'),
            _Image('Maranhao.jpg',images),
            _SubTitle('MARANHÃO - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Maranhao_Occupation.jpg',images),
            
            _SubTitle('PARÁ'),
            _Image('Para.jpg',images),
            _SubTitle('PARÁ - ICU OCCUPATION AND NEW DEATHS'),
            _Image('Para_Occupation.jpg',images),
            
            _SubTitle('AMAZONAS'),
            _Image('Amazonas.jpg',images),
            
            _SubTitle('ÍNDICE DE ISOLAMENTO SOCIAL'),
            '<p>[Visao Geral]</p>'
            '<a href="https://www.inloco.com.br/covid-19">https://www.inloco.com.br/covid-19</a>',
            
            _SubTitle('TAXA DE CONTÁGIO*'),
            _Image('Brazil_RT.jpg',images),
            '<b>*Indica quantas pessoas são contaminadas, em média, por cada caso confirmado de Covid-19.</b>',
            
            _SubTitle('REDUÇÃO DO TRÁFEGO DE VEÍCULOS*'),
            _Image('Traffic_change.jpg',images),
            '<b>*Data de início sendo a partir da segunda semana de março.</b>',
            
            _SubTitle('BRAZIL – SRAG'),
            _Image('SRAG.jpg',images),
            
            _Title('WORLD'),
            '<p>[World]</p>',
            _Image('World.jpg',images),
            _Image('World_2.jpg',images),
            _SubSubTitle('TOP 10 COUNTRY CASES X OTHER COUNTRIES'),
            _Image('Top_10_Cases.jpg',images),
            _SubSubTitle('TOP 10 COUNTRY DEATHS X OTHER COUNTRIES'),
            _Image('Top_10_Deaths.jpg',images),
            _SubTitle('TOTAL CASES BY COUNTRY'),
            _Image('Cases_by_Country.jpg',images),
            _SubTitle('EVOLUTION AFTER 1000TH CASE'),
            _Image('After_1000th.jpg',images),
            '<b>*Brazil last day</b>',
            _SubTitle('EVOLUTION AFTER 50TH DEATH'),
            _Image('After_50th.jpg',images),
            '<b>*Brazil last day</b>',
            _SubTitle('EVOLUTION AFTER LOCKDOWN ENDS'),
            '<p>[Lockdown]</p>',
            _Image('Cases_After_Lockdown.jpg',images),
            _Image('Deaths_After_Lockdown.jpg',images)
            ])
    print('E-mail sent!')