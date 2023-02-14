# Importação de Bibliotecas
import os
import sys
#import json
#from snscrape.modules import twitter

################################################################
# Estas variáveis serão passadas através de Parâmetros  (sys.argv[x])
max_linhas = sys.argv[4]
#hashtag = 'gremio'
hashtag = sys.argv[1]
desde = sys.argv[2]
ate = sys.argv[3]
################################################################ 

ambiente = "/usr/local/spark/resources"
diretorio = '/data/brz/json/'

# Comando scrape ()
comando = f'snscrape --jsonl --max-results {max_linhas} twitter-search \"#{hashtag} lang:pt since:{desde} until:{ate}\" > {ambiente}{diretorio}{hashtag}-tweets.json'
print(comando)

# gera arquivo
os.system(comando)

'''
output_filename = f'{hashtag}-tweets.json'
with open(output_filename, 'w') as f:
    scraper =  twitter.TwitterHashtagScraper(f'{hashtag} since:{desde}')
    i = 0
    for i, tweet in enumerate(scraper.get_items(), start = 1):
        tweet_json = json.loads(tweet.json())
        f.write(tweet.json())
        f.write('\n')
        f.flush()
        if max_linhas and i > max_linhas:
            break
'''
print('====================================')
print(f'Busca #{hashtag} Finalizada!')
print('====================================')
