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
                'Rio_de_Janeiro_deaths.jpg',
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
                'Maceio.jpg',
                'Maceio_Occupation.jpg',
                'Alagoas.jpg',
                'Alagoas_Occupation.jpg',
                'Sergipe.jpg',
                'Sergipe_Occupation.jpg',
                'Rio_Grande_do_Sul.jpg',
                'Rio_Grande_do_Sul_Occupation.jpg',
                'Parana.jpg',
                'Parana_Occupation.jpg',
                'Santa_Catarina.jpg',
                'Santa_Catarina_Occupation.jpg',
                'Hapvida.jpg',
                'Hapvida_2.jpg',
                'Hapvida_3.jpg',
                'Brazil.jpg',
                'Brazil_2.jpg',
                'Cases_State.jpg',
                'Deaths_State.jpg',
                'States_Comparison.jpg',
                'Cities.jpg',
                'Cities_2.jpg',
                'Cities_Comparison.jpg',
                'SRAG.jpg',
                'World.jpg',
                'World_2.jpg',
                'Cases_by_Country.jpg',
                'After_100th.jpg',
                'After_50th.jpg',
                'Tests.jpg',
                'United States.jpg',
                'United States_2.jpg',
                'Italy.jpg',
                'Italy_2.jpg',
                'Spain.jpg',
                'Spain_2.jpg',
                'United Kingdom.jpg',
                'United Kingdom_2.jpg',
                'Germany.jpg',
                'Germany_2.jpg',
                'France.jpg',
                'France_2.jpg',
                'Russia.jpg',
                'Russia_2.jpg']
    pages = convert_from_path(_file,size=1300)

    for page,image in zip(pages,image_list):
        width, height = page.size
        if image in ('Rio_de_Janeiro.jpg','Sao_Paulo.jpg','Pernambuco.jpg','Ceara.jpg','Sao_Luis.jpg','Maranhao.jpg','Maceio.jpg','Alagoas.jpg',
                    'Sergipe.jpg','Rio_Grande_do_Sul.jpg','Parana.jpg','Rio_de_Janeiro_SUS.jpg'):
            page = page.crop((0, 135, width-2, height-20))
     
        elif image in ( 'Sao_Paulo_Occupation.jpg','Pernambuco_Occupation.jpg','Ceara_Occupation.jpg','Sao_Luis_Occupation.jpg',
                        'Maranhao_Occupation.jpg','Alagoas_Occupation.jpg','Sergipe_Occupation.jpg','Rio_Grande_do_Sul_Occupation.jpg',
                        'Parana_Occupation.jpg','Santa_Catarina_Occupation.jpg','Hapvida_3.jpg','Maceio_Occupation.jpg','Parana_Occupation.jpg'):
            page = page.crop((0, 0, width, height))
        
        elif image in ('Santa_Catarina.jpg'):
            page = page.crop((0, 135, width-661, height-20))
        
        elif image in ( 'Cases_State.jpg','Deaths_State.jpg','Cases_by_Country.jpg','After_100th.jpg','After_50th.jpg','Tests.jpg','Cities.jpg',
                        'Cities_2.jpg'):
            page = page.crop((0, 135, width-2, height-20))
        
        elif image in ('States_Comparison.jpg'):
            page = page.crop((0, 122, width-2, height-20))
        
        elif image in ('Cities_Comparison.jpg'):
            page = page.crop((0, 116, width-2, height-20))
        
        elif image in ('SRAG.jpg'):
            page = page.crop((0, 164, width-28, height-162))
        
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
            _Title('ÍNDICE DE ISOLAMENTO SOCIAL'),
            '<p>[Visao Geral]</p>'
            '<a href="https://www.inloco.com.br/covid-19">https://www.inloco.com.br/covid-19</a>',
            
            _Title('HOSPITALIZATION EVOLUTION'),
            '<p>[HOSPITALIZATION EVOLUTION]</p>',
            _SubTitle('RIO DE JANEIRO / RJ - HOSPITAIS MUNICIPAIS'),
            _Image('Rio_de_Janeiro.jpg',images),
            
            _SubTitle('RIO DE JANEIRO / RJ - REDE SUS'),
            _Image('Rio_de_Janeiro_SUS.jpg',images),
            
            _SubTitle('SÃO PAULO / SP - HOSPITAIS MUNICIPAIS'),
            _Image('Sao_Paulo.jpg',images),
            _SubTitle('SÃO PAULO / SP - ICU OCCUPATION'),
            _Image('Sao_Paulo_Occupation.jpg',images),
            
            _SubTitle('PERNAMBUCO'),
            _Image('Pernambuco.jpg',images),
            _SubTitle('PERNAMBUCO - ICU OCCUPATION'),
            _Image('Pernambuco_Occupation.jpg',images),
            
            _SubTitle('CEARÁ'),
            _Image('Ceara.jpg',images),
            _SubTitle('CEARÁ - ICU OCCUPATION'),
            _Image('Ceara_Occupation.jpg',images),
            
            _SubTitle('SÃO LUÍS / MA'),
            _Image('Sao_Luis.jpg',images),
            _SubTitle('SÃO LUÍS / MA - ICU OCCUPATION'),
            _Image('Sao_Luis_Occupation.jpg',images),
            
            _SubTitle('MARANHÃO'),
            _Image('Maranhao.jpg',images),
            _SubTitle('MARANHÃO - ICU OCCUPATION'),
            _Image('Maranhao_Occupation.jpg',images),
            
            _SubTitle('MACEIÓ'),
            _Image('Maceio.jpg',images),
            _SubTitle('MACEIÓ - ICU OCCUPATION'),
            _Image('Maceio_Occupation.jpg',images),
            
            _SubTitle('ALAGOAS'),
            _Image('Alagoas.jpg',images),
            _SubTitle('ALAGOAS - ICU OCCUPATION'),
            _Image('Alagoas_Occupation.jpg',images),
            
            _SubTitle('SERGIPE'),
            _Image('Sergipe.jpg',images),
            _SubTitle('SERGIPE - ICU OCCUPATION'),
            _Image('Sergipe_Occupation.jpg',images),
            
            _SubTitle('RIO GRANDE DO SUL'),
            _Image('Rio_Grande_do_Sul.jpg',images),
            '<b>* Total ICU capacity (not the capacity of covid-exclusive ICU beds)</b>',
            _SubTitle('RIO GRANDE DO SUL - ICU OCCUPATION'),
            _Image('Rio_Grande_do_Sul_Occupation.jpg',images),
            '<b>* Total ICU capacity (not the capacity of covid-exclusive ICU beds)</b>',
            
            _SubTitle('PARANÁ'),
            _Image('Parana.jpg',images),
            _SubTitle('PARANÁ - ICU OCCUPATION'),
            _Image('Parana_Occupation.jpg',images),
            
            _SubTitle('SANTA CATARINA'),
            _Image('Santa_Catarina.jpg',images),
            _SubTitle('SANTA CATARINA - ICU OCCUPATION'),
            _Image('Santa_Catarina_Occupation.jpg',images),
            
            _SubTitle('HAPVIDA'),
            _Image('Hapvida.jpg',images),
            _Image('Hapvida_2.jpg',images),
            _SubTitle('HAPVIDA - ICU OCCUPATION'),
            _Image('Hapvida_3.jpg',images),
            
            _Title('BRAZIL'),
            '<p>[Brazil]</p>',
            _Image('Brazil.jpg',images),
            _Image('Brazil_2.jpg',images),
            
            _SubTitle('BRAZIL – STATES'),
            '<p>[Brazil States]</p>',
            _Image('Cases_State.jpg',images),
            _Image('Deaths_State.jpg',images),
            _SubSubTitle('BRAZIL STATES vs OTHER WORLD STATES'),
            _Image('States_Comparison.jpg',images),
            
            _SubTitle('BRAZIL – CITIES'),
            '<p>[Brazil Cities]</p>',
            _Image('Cities.jpg',images),
            _Image('Cities_2.jpg',images),
            _SubSubTitle('BRAZIL CITIES vs OTHER CITIES'),
            _Image('Cities_Comparison.jpg',images),
            
            _SubTitle('BRAZIL – SRAG'),
            _Image('SRAG.jpg',images),
            
            _Title('WORLD'),
            '<p>[World]</p>',
            _Image('World.jpg',images),
            _Image('World_2.jpg',images),
            _SubTitle('TOTAL CASES BY COUNTRY'),
            _Image('Cases_by_Country.jpg',images),
            _SubTitle('EVOLUTION AFTER 100TH CASE'),
            _Image('After_100th.jpg',images),
            '<b>*Brazil last day</b>',
            _SubTitle('EVOLUTION AFTER 50TH DEATH'),
            _Image('After_50th.jpg',images),
            '<b>*Brazil last day</b>',
            
            _SubTitle('TESTS'),
            _Image('Tests.jpg',images),
            
            _Title('OTHER COUNTRIES'),
            _SubTitle('UNITED STATES'),
            '<p>[United States]</p>',
            _Image('United States.jpg',images),
            _Image('United States_2.jpg',images),
            
            _SubTitle('ITALY'),
            '<p>[Italy]</p>',
            _Image('Italy.jpg',images),
            _Image('Italy_2.jpg',images),
            
            _SubTitle('SPAIN'),
            '<p>[Spain]</p>',
            _Image('Spain.jpg',images),
            _Image('Spain_2.jpg',images),
            
            _SubTitle('UNITED KINGDOM'),
            '<p>[United Kingdom]</p>',
            _Image('United Kingdom.jpg',images),
            _Image('United Kingdom_2.jpg',images),
            
            _SubTitle('GERMANY'),
            '<p>[Germany]</p>',
            _Image('Germany.jpg',images),
            _Image('Germany_2.jpg',images),
            
            _SubTitle('FRANCE'),
            '<p>[France]</p>',
            _Image('France.jpg',images),
            _Image('France_2.jpg',images),
            
            _SubTitle('RUSSIA'),
            '<p>[Russia]</p>',
            _Image('Russia.jpg',images),
            _Image('Russia_2.jpg',images)
            ])
    print('E-mail sent!')