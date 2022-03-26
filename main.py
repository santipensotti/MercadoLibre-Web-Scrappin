import pandas as pd
from bs4 import BeautifulSoup as soup 
from urllib.request import Request, urlopen
import pandas as pd
import concurrent.futures
import numpy as np



df = pd.read_excel("deptos.xlsx", engine='openpyxl')


lista =[]
url = []

def getDeptos (number):
    """"
    Hacemos web scrapping a la parte de inmuebles de mercado libre, accedemos a la pagina y de ahi extraemos los datos que queremos y lo cargamos en un diccionario
    Chequeamos que los datos no sean nulos asi se puede pasar a texto
    """
    pag_url = f"https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/capital-federal/_Desde_{number}_NoIndex_True"
    req = Request(pag_url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    pag_soup = soup(webpage,"html.parser")

    url_depto =pag_soup.findAll("div", {"class", "andes-card andes-card--flat andes-card--default ui-search-result ui-search-result--res andes-card--padding-default andes-card--animated"})
    for div in url_depto:
        link = div.find("a")["href"]
        url.append(link)

    
def numero():
    """
    Queremos saber cuantas paginas tiene en total mercado libre, accedemos a la primera pagina y de ahi con soup accedemos al div donde esta la cantidad de paginas
    """
    pag_url = "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/capital-federal/_NoIndex_True"
    req = Request(pag_url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    pag_soup = soup(webpage,"html.parser")
    numero = pag_soup.find('li',{'class',"andes-pagination__page-count"}).text
    numero= numero[3:]
    x = (int(numero) - 1) *48 +1
    return x

x = numero() + 1

for i in range(1,x,48):
    # Mercado libre reccorre las paginas de 48 en 48, ej pag = 1, pag = 49. Asi que reccoremos la cantidad de paginas que haya (x) de a 48 y llamamos a la funcion
    # getDeptos para poder conseguir la informacion de todas las paginas
    getDeptos(i)

def caracteristicas(link):
    """
    Con los url de los departamentos previamente conseguidos, recorremos cada url donde se extraen los datos principales para luego ser guardados en un Excel

    """
    link = link
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    pag_soup = soup(webpage,"html.parser")
    barrios = pag_soup.find_all('a',{'class','andes-breadcrumb__link'})
    barrio = barrios[5].text
    titulos =pag_soup.find_all('th', {'class','andes-table__header andes-table__header--left ui-pdp-specs__table__column ui-pdp-specs__table__column-title'})
    expensas =pag_soup.find_all('span', {'class','andes-table__column--value'})
    b = 0

    precio = pag_soup.find("span",{"class","andes-money-amount__fraction"}).text
    precio = precio.replace(".","")
    simbolo = pag_soup.find('span',{'class','andes-money-amount__currency-symbol'}).text
    if simbolo == '$':
        pass
    else: 
        precio = int(precio) * 200
    
    carac = {'Link':link, 'Barrio': barrio, "Precio":precio}
    for i in titulos:
        texto = i.text
        valor = expensas[b].text
        carac[texto] = valor
        b += 1
    lista.append(carac)


with concurrent.futures.ThreadPoolExecutor() as executor:
    #Ejecuto varios procesos a la vez asi se reduce el tiempo de ejecucion
    executor.map(caracteristicas, url)

df = pd.DataFrame(lista)
nombre_archivo = "prueba.xlsx"

def limpieza (archivo):
    """"
    Los datos son limpiados, se extraen los tipos de datos string de ciertas columnas y se dejan como un tipo int para que puedan ser usados como analisis
    Se crea una columna nuevo precio m2 donde se divide la superficie cubierta por los m2
    """
    valores =df.columns.values.tolist()

    df["Superficie cubierta"] = df["Superficie cubierta"].str.extract("(\d+)")
    df["Expensas"] = df["Expensas"].str.extract("(\d+)")
    df["Antigüedad"] = df["Antigüedad"].str.extract("(\d+)")
    df["Superficie total"] = df["Superficie total"].str.extract("(\d+)")
    df["Superficie total"] = df["Superficie total"].replace(np.nan, 0)

    df_copy = df.loc[:,valores]

    df_copy["Superficie total"] = df["Superficie total"].astype(str).astype(int)
    df_copy["Antigüedad"] = df["Antigüedad"].dropna().astype(str).astype(int)
    precio = df_copy['Precio'].values.tolist()
    m2 = df_copy["Superficie cubierta"].values.tolist()
    lista_precio = []

    for i in range(len(precio)):
        precio_m2 = int(precio[i]) / int(m2[i])
        lista_precio.append(precio_m2)
    
    df_copy["Precio M2"] = lista_precio
    df_copy.to_excel(archivo)

limpieza(nombre_archivo)


