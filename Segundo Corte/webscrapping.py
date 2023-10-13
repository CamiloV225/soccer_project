from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd

driver = webdriver.Chrome()


def cookie():
  try:
    iframe = driver.find_element(By.ID,'sp_message_iframe_851932')
    driver.switch_to.frame(iframe)
    element = driver.find_element(By.XPATH,'.//*[@id="notice"]/div[3]/div[3]/button').click()
    driver.switch_to.default_content()
  except:
    None

def scrapping():
  players=[]
  jugadores = driver.find_elements(By.XPATH,'.//table[@class="items"]/tbody/tr')
  for i in range(13,20):
      print(f'################Pagina {i}################')
      driver.get(f"https://www.transfermarkt.co/spieler-statistik/wertvollstespieler/marktwertetop?page={i}")
      time.sleep(2)
      driver.current_window_handle
      time.sleep(2)
      removedor = cookie()
      jugadores = driver.find_elements(By.XPATH,'.//table[@class="items"]/tbody/tr')
      for jugador in jugadores:
            player = []
            print('NUEVO JUGADOR')
            time.sleep(1)
            nombre = jugador.find_element(By.XPATH,'.//td/table[@class="inline-table"]/tbody/tr/td[@class="hauptlink"]/a').text  
            valor = jugador.find_element(By.XPATH,'.//td[@class="rechts hauptlink"]/a').text
            
            time.sleep(3)
            try:
                info = jugador.find_element(By.XPATH,'.//table[@class="inline-table"]/tbody/tr/td[@class="hauptlink"]/a[@href]')
                stringurl = info.get_attribute("href")
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(stringurl)
                time.sleep(1.5)
                try:
                    removedor = cookie()
                except Exception as f:
                    pass
                tabla = driver.find_element(By.XPATH, './/main/div[3]/div[1]/div[2]/div/div[2]/div')
                span = tabla.find_elements(By.XPATH, '//span')
            #    try: 
            #        trophies=[]
            #        container = driver.find_element(By.XPATH, './/main/header/div[2]')            
            #        trofeos = container.find_elements(By.XPATH, '//a')
            #        for trofeo in range(0, len(trofeos)-1):
            #            title = trofeo.get_attribute('title')
            #            trophies.append(title)
            #            print(f'Trofeos: {title}')
            #    except:
            #        pass
                time.sleep(1)
                try:
                    for dato in range(0, len(span)):
                        player = []
                        time.sleep(1)

                    
                            #print(span[dato].text)
                        result = span[dato].text
                    
                        if result == 'Fecha de nacimiento:':
                            fecha = span[dato + 1].text
                            print(f'Nombre: {nombre}')

                            print(f'Valor: {valor}')
                            
                            print(f'Fecha: {fecha}')
                        if result == 'Lugar de nacimiento:':
                            lugar = span[dato + 1].text
                            print(f'Lugar de N: {lugar}')
                        if result == 'Edad:':
                            edad = span[dato + 1].text
                            print(f'Edad: {edad}')
                        if result == 'Altura:':
                            altura = span[dato + 1].text
                            print(f'Altura: {altura}')
                        if result == 'Posición:':
                            posicion = span[dato + 1].text
                            print(f'Posicion: {posicion}')
                        if result == 'Pie:':
                            pie = span[dato + 1].text
                            print(f'Pie Favorito: {pie}')
                        if result == 'Club actual:':
                            club = span[dato + 1].text
                            print(f'Club: {club}')
                        if result == 'Fichado:':
                            fichado = span[dato + 1].text
                            print(f'Fichado: {fichado}')
                        if result == 'Contrato hasta:':
                            contrato = span[dato + 1].text
                            print(f'Contrato: {contrato}')
                        elif result == 'Redes sociales:':
                            break
                    player = [nombre,valor, fecha, lugar, edad, altura, posicion, pie, club, fichado, contrato]
                    players.append(player)
                    print(player)
                    df=pd.DataFrame(players, columns=['nombre','valor', 'fecha', 'lugar', 'edad', 'altura', 'posicion', 'pie', 'club', 'fichado', 'contrato'])
                    df.to_csv('Players2.csv')
                    driver.close() 
                    driver.switch_to.window(driver.window_handles[0])
                except Exception as e:
                    pass
            
            except:
                pass
        
            

if __name__ == "__main__":
  scrapping()

"""  try:
                    player[1] = ''
                    if result in ['Nombre en país de origen:','Nombre completo:' ]:
                        if result == 'Nombre en país de origen:':
                            nombrecompleto = span[dato + 1].text
                            print(f'Nombre: {nombre}')
                            print(f'Nombre Completo: {nombrecompleto}')
                            print(f'Valor: {valor}')
                        elif result == 'Nombre completo:':
                            nombrecompleto = span[dato + 1].text
                            print(f'Nombre Completo: {nombrecompleto}')
                except:
                    nombrecompleto = None """
            