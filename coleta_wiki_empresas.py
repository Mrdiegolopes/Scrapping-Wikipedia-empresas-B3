
import requests
import pandas as pd
import time
from urllib.parse import quote
import re
from bs4 import BeautifulSoup 

WIKI_API_URL = "https://pt.wikipedia.org/w/api.php"
USER_AGENT = "B3CompanyFinder/2.0 (usuario@gmail.com)"

def clean_company_name(name):
    removals = [
        'S.A.', 'S.A', 'SA', 'S/A', '.com', 'COM',
        'Incorporação', 'Empreendimentos', 'Participações',
        'Brasil', 'Brazil', 'Holdings', 'Grupo', 'Ltda',
        'Distribuidora', 'Companhia', 'Engenharia'
    ]
    for term in removals:
        name = name.replace(term, '')
    return re.sub(r'\s+', ' ', name).strip()

def search_wikipedia_api(title):
    # busca a melhor página na Wikipedia via api
    params = {
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'srsearch': title,
        'srlimit': 1, # acessa o primeiro resultado da busca
        'utf8': 1 # tratamento correto de caracteres
    }
    headers = {'User-Agent': USER_AGENT}
    response = requests.get(WIKI_API_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()

    if data.get('query', {}).get('search'):
        page_title = data['query']['search'][0]['title']
        return page_title
    return None

def get_page_html(page_title):
    params = {
        'action': 'parse',
        'format': 'json',
        'page': page_title,
        'prop': 'text',
        'utf8': 1
    }
    headers = {'User-Agent': USER_AGENT}
    response = requests.get(WIKI_API_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()

    html = data.get('parse', {}).get('text', {}).get('*', '')
    return html

def extract_infobox_and_text(html):
    soup = BeautifulSoup(html, 'html.parser')

    # Infobox
    infobox = soup.find('table', {'class': 'infobox'})
    infobox_text = ""
    if infobox:
        infobox_text = infobox.get_text(separator=" | ", strip=True)

  #texto
  content_div = soup.find('div', {'class': 'mw-parser-output'})
    text = ""
    if content_div:
        for elem in content_div.find_all(['p', 'ul', 'ol'], recursive=False):
            txt = elem.get_text(separator=" ", strip=True)
            if txt:
                text += txt + "\n"

    return infobox_text, text

def busca_pagina_titulo(nome_empresa):
    cleaned = clean_company_name(nome_empresa)
    tentativas = [
        f"{nome_empresa} (empresa)",
        f"{nome_empresa} empresa",
        nome_empresa,
        cleaned,
        f"{cleaned} empresa",
        f"{nome_empresa} Brasil"
    ]

    for tentativa in tentativas:
        print(f"Buscando: {tentativa}")
        page_title = search_wikipedia_api(tentativa)
        if page_title:
            return page_title
        time.sleep(0.5)

    return None

def main():
    try:
        df = pd.read_csv('/content/acoes-listadas-b3 (1).csv') #arquvios que contem os nomes e tickers das respectivas empresas 
    except FileNotFoundError:
        print("Arquivo não encontrado.")
        return

    col_nome = 'Nome' if 'Nome' in df.columns else df.columns[0]
    col_ticker = 'Ticker' if 'Ticker' in df.columns else df.columns[1]
    resultados = []

    with open("empresas_b3_corpus.txt", "w", encoding="utf-8") as f_txt:
        for idx, row in df.iterrows():
            nome = str(row[col_nome]).strip()
            ticker = str(row[col_ticker]).strip()

            print(f"\n[{idx+1}/{len(df)}] Processando: {nome} ({ticker})")

            page_title = busca_pagina_titulo(nome)

            if not page_title:
                print(f"Página não encontrada para {nome}")
                resultados.append({
                    "Nome": nome,
                    "Ticker": ticker,
                    "Link": "",
                    "Infobox": "",
                    "Texto": ""
                })
                continue

            url = f"https://pt.wikipedia.org/wiki/{quote(page_title.replace(' ', '_'))}"
            html = get_page_html(page_title)
            infobox, texto = extract_infobox_and_text(html)

            f_txt.write(f"===== {nome} ({ticker}) =====\n")
            f_txt.write(f"Link: {url}\n\n")
            f_txt.write("[INFOBOX]\n")
            f_txt.write(f"{infobox}\n\n")
            f_txt.write("[TEXTO]\n")
            f_txt.write(f"{texto}\n\n")

            resultados.append({
                "Nome": nome,
                "Ticker": ticker,
                "Link": url,
                "Infobox": infobox,
                "Texto": texto
            })

            time.sleep(1)

    df_out = pd.DataFrame(resultados)
    df_out.to_csv("empresas_b3_corpus.csv", index=False, encoding="utf-8-sig")

    print("empresas_b3_corpus.txt (corpus completo)")



if __name__ == "__main__":
    main()
