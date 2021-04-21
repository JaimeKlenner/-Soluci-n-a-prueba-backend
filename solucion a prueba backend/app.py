from flask import Flask, request
import requests
from bs4 import BeautifulSoup
import sqlite3
import threading
import time

app = Flask(__name__)

scrapers_running = {}


@app.route('/api/scrapers/', methods=['DELETE'])
def delete():
    data = request.get_json()
    response_dict = {}
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    query = ("""SELECT * FROM scrapers WHERE id='%s'""" % (data['id']))
    c.execute(query)
    query_value = c.fetchall()

    if (query_value):
        scrapers_running[query_value[0][2]]['run'] = False
        query = """DELETE FROM scrapers WHERE id = '%s'""" % (data['id'])
        c.execute(query)
        response_dict['msg'] = "Scraper deleted"
        conn.commit()
        conn.close()

        return response_dict, 200

    else:
        response_dict['error'] = "Scraper not found"
        conn.commit()
        conn.close()

        return response_dict, 400

    

@app.route('/api/scrapers/', methods=['POST'])
def post():

    data = request.get_json()
    response_dict = {}
    
    html_text = requests.get('https://coinmarketcap.com/').text
    soup = BeautifulSoup(html_text,'lxml')
    table = soup.find_all('tr')
    i = 1
    crypto_found = False
    crypto_value = 0

    while (i<len(table)):
        if(i<11):
            try:
                if(table[i].find('p',class_='sc-1eb5slv-0 iJjGCS').text == data['currency']):
                    crypto_found = True
                    crypto_value = float(table[i].find_all('td')[3].div.a.text[1:].replace(",",""))

            except Exception as e:
                print(e)
        else:
            try:
                if(table[i].find('a',class_='cmc-link').find_all('span')[1].text == data['currency']):
                    crypto_found = True
                    crypto_value = float(table[i].find_all('td')[3].span.text[1:].replace(",",""))

            except Exception as e:
                print(e)

        i+=1

    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()

    if(crypto_found):       
        query = ("""SELECT * FROM scrapers WHERE currency='%s'""" % (data['currency']))
        c.execute(query)
        query_values = c.fetchall()
        
        if(query_values):
            conn.close()
            response_dict['error'] = "Currency already stored"

            return response_dict, 400

        else:
            query = ("INSERT INTO scrapers(currency, frequency, value) VALUES (?, ?, ?) ")
            c.execute(query,(data['currency'],data['frequency'],crypto_value))
            conn.commit()
            query = ("""SELECT id, created_at FROM scrapers WHERE currency = '%s'""" % (data['currency']))
            c.execute(query)
            query_values = c.fetchall()

        conn.close
        scrapers_running[data['currency']] = {'run': True,
                                              'frequency': data['frequency']}

        threading.Thread(target=create_scraper,args=[data['currency']]).start()

    else:
        conn.close()
        response_dict['error'] = "Cryptocurrency not found"
        return response_dict, 400
    
    response_dict['id'] = query_values[0][0]
    response_dict['created_at'] = query_values[0][1]
    response_dict['currency'] = data['currency']
    response_dict['frequency'] = data['frequency']
    
    return response_dict, 200

@app.route('/api/scrapers/', methods=['GET'])
def get():
    response_dict = {}
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    query = ("""SELECT * FROM scrapers""")
    c.execute(query)
    query_value = c.fetchall()
    scrapers_list = []

    for i in range(len(query_value)):
        scrapers_list.append({"id": query_value[i][0],
                              "created_at" : query_value[i][1],
                              "currency" : query_value[i][2],
                              "frequency" : query_value[i][3],
                              "value_updated_at" : query_value[i][4],
                              "value" : query_value[i][5]   })

    conn.close()
    response_dict = {}
    response_dict['Scrapers'] = scrapers_list
    return response_dict

@app.route('/api/scrapers/', methods=['PUT'])
def put():
    data = request.get_json()
    response_dict = {}
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    query = ("""SELECT * FROM scrapers WHERE id ='%s'""" % (data['id']))
    c.execute(query)
    query_value = c.fetchall()

    if(query_value):
        query = """UPDATE scrapers SET value_updated_at = DATETIME('now'), frequency = '%s' WHERE id = '%s'""" % (data['frequency'],data['id'])
        c.execute(query)
        conn.commit()
        query = """SELECT currency FROM scrapers WHERE id = '%s'""" % (data['id'])
        c.execute(query)
        query_value = c.fetchall()
        scrapers_running[query_value[0][0]]['frequency'] = data['frequency']
        conn.close()
        response_dict['msg'] = "Scraper updated!"

        return response_dict, 200

    else:
        conn.close()
        response_dict['error'] = "ID not found"

        return response_dict, 400    

def create_scraper(currency_name):

    query = ""
    crypto_value = 0

    while(scrapers_running[currency_name]['run']):
        html_text = requests.get('https://coinmarketcap.com/').text
        soup = BeautifulSoup(html_text,'lxml')
        table = soup.find_all('tr')
        conn = sqlite3.connect('scraper.db')
        c = conn.cursor()
        i = 1

        while (i<len(table)):

            if(i<11):
                try:
                    if(table[i].find('p',class_='sc-1eb5slv-0 iJjGCS').text == currency_name):
                        crypto_value = float(table[i].find_all('td')[3].div.a.text[1:].replace(",",""))
                        query = """UPDATE scrapers SET value_updated_at = DATETIME('now'), value = '%s' WHERE currency = '%s'""" % (crypto_value,currency_name)

                except Exception as e:
                    print(e)
            else:
                try:
                    if(table[i].find('a',class_='cmc-link').find_all('span')[1].text == currency_name):
                        crypto_value = float(table[i].find_all('td')[3].span.text[1:].replace(",",""))
                        query = """UPDATE scrapers SET value_updated_at = DATETIME('now'), value = '%s' WHERE currency = '%s'""" % (crypto_value,currency_name)

                except Exception as e:
                    print(e)

            i+=1
        
        c.execute(query)
        conn.commit()
        conn.close()
        time.sleep(scrapers_running[currency_name]['frequency'])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)


    