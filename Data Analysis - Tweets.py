# Databricks notebook source
import pyspark.sql.functions as F

tts_Lula = '/FileStore/shared_uploads/lalencar9@gmail.com/Dados_Lula.csv'
tts_Bolsonaro = '/FileStore/shared_uploads/lalencar9@gmail.com/Dados_Bolsonaro.csv'


Schema = "tweet string, datahora string, device string, Localizacao string"




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### O Código em python usado para streamar os tweets em Tempo real
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC import tweepy
# MAGIC import re
# MAGIC from Keys import * #arquivo em python contendo as chaves de acesso do API do twitter
# MAGIC import csv
# MAGIC 
# MAGIC client = tweepy.Client(bearer_token,API_key,API_secret,access_key,access_secret)
# MAGIC 
# MAGIC auth = tweepy.OAuth1UserHandler(API_key, API_secret, access_key, access_secret)
# MAGIC 
# MAGIC api = tweepy.API(auth)
# MAGIC 
# MAGIC termos_procurados = ["Lula"]
# MAGIC def remove_non_ascii_2(string):
# MAGIC     return string.encode('utf-8', errors='ignore').decode('utf-8')
# MAGIC     #Essa função remove todos os caracteres não ascii de um string
# MAGIC 
# MAGIC def remove_emoji(string):
# MAGIC     #essa função remove os emojis e caracteres especiais de uma string via seu codigo unicode
# MAGIC     emoji_pattern = re.compile("["
# MAGIC                                u"\U0001F600-\U0001F64F"  # emoticons
# MAGIC                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
# MAGIC                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
# MAGIC                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
# MAGIC                                u"\U00002500-\U00002BEF"  # chinese char
# MAGIC                                u"\U00002702-\U000027B0"
# MAGIC                                u"\U00002702-\U000027B0"
# MAGIC                                u"\U000024C2-\U0001F251"
# MAGIC                                u"\U0001f926-\U0001f937"
# MAGIC                                u"\U00010000-\U0010ffff"
# MAGIC                                u"\u2640-\u2642"
# MAGIC                                u"\u2600-\u2B55"
# MAGIC                                u"\u200d"
# MAGIC                                u"\u23cf"
# MAGIC                                u"\u23e9"
# MAGIC                                u"\u231a"
# MAGIC                                u"\ufe0f"  # dingbats
# MAGIC                                u"\u3030"
# MAGIC                                u'\u0142'
# MAGIC                                u'\u20e3'
# MAGIC                                u'\u203c'
# MAGIC                                u'\u02da'
# MAGIC                                u'\u039f'
# MAGIC                                u'\u21c0'
# MAGIC                                u'\u0336'
# MAGIC                                u'\u015b'
# MAGIC                                u'\u2192'
# MAGIC                                u'\u2234'
# MAGIC                                u'\u2070'
# MAGIC                                u'\u208d'
# MAGIC                                u'\u208e'
# MAGIC                                u'\u2206'
# MAGIC                                u'\u0302'
# MAGIC                                u'\u2066'
# MAGIC                                u'\u2077'
# MAGIC                                u'\u141f'
# MAGIC                                u'\u10e7'
# MAGIC                                u'\u2069'
# MAGIC                                u'\u203f'
# MAGIC                                u'\u22c6'
# MAGIC                                u'\u2049'
# MAGIC                                u'\u2139'
# MAGIC                                u'\u0137'
# MAGIC                                u'\u0113'
# MAGIC                                u'\u23ef'
# MAGIC                                u'\u23f0'
# MAGIC                                u'\u1dbb'
# MAGIC                                u'\u013a'
# MAGIC                                u'\u221a'
# MAGIC                                u'\u0a82'
# MAGIC                                u'\u2307'
# MAGIC                                "]+", flags=re.UNICODE)
# MAGIC     return emoji_pattern.sub(r'', string)
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC class MyStream(tweepy.StreamingClient):
# MAGIC         
# MAGIC     def on_connect(self):
# MAGIC         print("Conectado")
# MAGIC         
# MAGIC     def on_tweet(self, tweet):
# MAGIC         if tweet.referenced_tweets == None: #ignora todos os tweets que sejam retweets
# MAGIC             if tweet.lang != 'und':#ignora todos os tweets que a linguagem é definida como undefined pelo twitter
# MAGIC                 texto = remove_non_ascii_2(tweet.text)#pega o texto do tweet do objeto tweet
# MAGIC                 textoD = remove_emoji(texto)
# MAGIC                 horas = tweet.created_at #pega as horas em que tweet foi feito do objeto tweet
# MAGIC                 device = tweet.source #pega o dispositivo em qual o tweet foi feito do objeto tweet
# MAGIC                 user = api.get_user(user_id=tweet.author_id)#pega o perfil do usuário usando o ID do autor do tweet
# MAGIC                 local = remove_non_ascii_2(user.location)#pega a localização do perfil buscado acima
# MAGIC                 localD = remove_emoji(local)
# MAGIC 
# MAGIC                 dados = [textoD,horas,device,localD]#Coloca todos as informações acima em uma lista
# MAGIC                 with open("Dados_Lula.csv","a",newline="") as f:#abre um csv em append
# MAGIC                     writer = csv.writer(f)
# MAGIC                     writer.writerow(dados)#escreve os dados da lista no csv
# MAGIC             
# MAGIC 
# MAGIC stream = MyStream(bearer_token=bearer_token)
# MAGIC 
# MAGIC 
# MAGIC for termo in termos_procurados:
# MAGIC     stream.add_rules(tweepy.StreamRule(termo))
# MAGIC 
# MAGIC def main():
# MAGIC     stream.filter(tweet_fields=["referenced_tweets","created_at","source","author_id","lang"])
# MAGIC 
# MAGIC 
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazendo a Leitura dos CSVs gerados pelo Código acima

# COMMAND ----------

LulaDF = (spark.read
          .option("enconding","ISO-8859-1")
          .option("escape", "\"")
          .option("multiline", True)
          .schema(Schema)
          .csv(tts_Lula)
          
         )

display(LulaDF)

# COMMAND ----------

BolsonaroDF = (spark.read
          .option("escape", "\"")
          .option("multiline", True)
          .option("enconding","ISO-8859-1")
          .schema(Schema)
          .csv(tts_Bolsonaro)
          
         )

display(BolsonaroDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comparando os números de tweets de cada dispositivo

# COMMAND ----------

lulaDev = (LulaDF.groupBy("device").count()).sort(F.col("count").desc()).limit(5)

display(lulaDev)

# COMMAND ----------

BolsonaroDev = (BolsonaroDF.groupBy("device").count()).sort(F.col("count").desc()).limit(5)

display(BolsonaroDev)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Analisando os dados
# MAGIC 
# MAGIC Como podemos ver a diferença entre os dispositivos para os dois candidatos não é tão grande, sendo o Android o número 1 de tweets para ambos.
# MAGIC 
# MAGIC Única diferença seria o Twibbon em quinto lugar, sendo o Twibbon uma plataforma para edição da foto de perfil para apoiar uma causa em especifico, nesse caso o candidato Lula

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Formatando e analisando as informações de data e hora

# COMMAND ----------

LulaHorasDF = (LulaDF.withColumn("datahora", F.from_utc_timestamp(F.col('datahora'),"GMT-3"))
              .withColumn("Horas",F.date_format("datahora","HH"))
              )

display(LulaHorasDF)

# COMMAND ----------

BolsonaroHorasDF = (BolsonaroDF.withColumn("datahora", F.from_utc_timestamp(F.col('datahora'),"GMT-3"))
              .withColumn("Horas",F.date_format("datahora","HH"))
              )

display(BolsonaroHorasDF)

# COMMAND ----------

LulaHorasDF1 = ((LulaHorasDF.groupBy("Horas").count())
              .sort(F.col("count").desc())
             )

display(LulaHorasDF1)

# COMMAND ----------

BolsonaroHorasDF1 = ((BolsonaroHorasDF.groupBy("Horas").count())
              .sort(F.col("count").desc())
             )

display(BolsonaroHorasDF1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analisando os dados de horário
# MAGIC 
# MAGIC Numa analise mais por cima dos dados, o tweets relacionados ao candidato Lula foram majoritariamente feito as 11 da manhã e de menos frequentemente de tarde às 15h e 16h da tarde.
# MAGIC 
# MAGIC Já os tweets relacionados ao candidato Bolsonaro foram feitos de forma praticamente igualitária às 16h e 13h respectivamente.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Formatando as informações de localização

# COMMAND ----------

LulaLocaDF = ((LulaDF.groupBy("Localizacao").count())
              .sort(F.col("count").desc())
             )

display(LulaLocaDF)

# COMMAND ----------

BolsonaroLocaDF = ((BolsonaroDF.groupBy("Localizacao").count())
              .sort(F.col("count").desc())
             )

display(BolsonaroLocaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analisando as informações de localização
# MAGIC 
# MAGIC Em ambas as fontes de dados, desconsiderando a primeira linha que representa todos os usuários que não tem sua localização em seus perfis, os tweets foram majoritariamente feitos do Brasil e mais especificamente de estados brasileiros como por exemplo: São Paulo, Rio de Janeiro, Bélem, Manaus, Recife e etc.
