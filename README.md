DOCUMENTAÇÃO

Goal: Predict arrival time to terminal of Rio de Janeiro's municipal bus with data gathered in batch-time (5 to 5 minutes). Utilized: Spark, MongoDB

spark-mongo-script

FUNÇÕES
- makehistory: coleta todo os dados históricos, faz o tratamento e a criação de novas variáveis e por fim salva em uma nova coleção no banco de dados. 
- run_funcs: Executa as funções makehistory_by_line() e makehistory_by_bus() para todos os ônibus dentro da linha escolhida.
- get_distance: Dadas coordenadas geográficas de 2 pontos, calcular distância euclidiana entre eles.
- makehistory_by_line: Dadas informações de uma determinada linha de ônibus, criar as variáveis TIME_OF_DAY, DISTANCE_IS e DISTANCE_FS.
- makehistory_by_bus: Dado um Dataframe gerado pela makehistory_by_line, para cada ônibus circulante na linha, criar as variáveis TERMINAL e DELTA_TIME.

FEATURES:
VELOCIDADE - Velocidade instantânea do veículo no momento da coleta. Variável original. Hipótese: A velocidade do veículo influencia em quanto tempo ele vai chegar ao seu destino. Consideração: A velocidade média durante a viagem seria uma melhor aproximação, visto que a velocidade instantânea pode ter sido coletada em qualquer momento aleatório durante a viagem, inclusive em paradas de farol.
LATITUDE - Latitude do veículo no momento da coleta. Variável original.
LONGITUDE - Longitude do veículo no momento da coleta. Variável original.
TIME_OF_DAY - Horário do dia. Criada a partir de DATAHORA. Hipótese: O horário do dia em que a viagem ocorreu influencia no tempo de chegada do ônibus, visto que existem horários de pico e horários mais vazios.
WEEKDAY - Dia da semana da viagem. Hipótese: o itinerário do ônibus pode variar conforme o dia da semana, especialmente em finais de semana, quando o itinerário tende a ser diferente.
DISTANCE_IS - Distância euclidiana da estação inicial. Criada a partir da LATITUDE, LONGITUDE, LATITUDE_IS e LONGITUDE_IS. Hipótese: A distância da sua origem/destino importa para calcular o tempo em que ele chegará. Consideração: A variável é linear e o trajeto é longo e não linear, então uma aproximação maior seria dividir o trajeto em pequenas partes para que a trajetória seja mais semelhante à linear.
DISTANCE_FS - Distância euclidiana da estação final. Criada a partir da LATITUDE, LONGITUDE, LATITUDE_IS e LONGITUDE_IS. Hipótese: A distância da sua origem/destino importa para calcular o tempo em que ele chegará.
TERMINAL - Indica se o ônibus está ou não em seu terminal (0 ou 1). Criada a partir de VELOCIDADE, DISTANCE_IS, DISTANCE_FS, EPOCH_TIME, VAR_EPOCH_TIME(variação de tempo com relação ao registro anterior), VAR_ET_LEAD(variação de tempo deslocada para cima). Hipótese: Se o ônibus está no terminal, então seu tempo de chegada ao terminal deve ser zero. Em torno de 20% dos dados indicam que o ônibus está no terminal. Consideração: Essa variável foi criada a partir de regras fixas, portanto são uma aproximação da realidade. Para aproximar a variável da realidade, pode-se testar modificando MAX_VELOCITY e MAX_DISTANCE.

TARGET
DELTA_TIME - Variação de tempo até chegar no terminal, em segundos.

PARÂMETROS DE DECISÃO
MAX_VELOCITY - Influencia na variável TERMINAL. A velocidade máxima para que o registro possa ser considerado no terminal. Padrão: 30km/h
MAX_DISTANCE - Influencia na variável TERMINAL. A distância máximas dos terminal inicial/final para que o registro possa ser considerado no terminal. Padrão: 1.0 km
MAX_VAR_TIME - Influencia na variável TERMINAL. A máxima variação de tempo com relação ao registro anterior para que o registro possa ser considerado em uma outra viagem. Padrão: 30min
MIN_TRIP - Influencia no Dataframe final. O número mínimo de viagens para que o conjunto de registros se caracterize em uma viagem. Padrão: 2
MIN_BUS - Influencia no Dataframe da linha. O número mínimo de registros de um BUS_ID para que o conjunto seja caracterizado como sufiente para análise. Padrão: 4

regression-script

#FUNÇÕES
get_bus_id - Lista as ordens dos últimos ônibus e seu respectivo tempo de coleta. É necessário especificar a linha desejada.
train_history - Treina os dados históricos e gera um modelo. É necessário especificar a linha desejada.
get_real_time - Gera a previsão de quanto tempo é necessário para o ônibus chegar ao terminal. É necessário especificar a linha desejada e a ordem do ônibus escolhido.
offline_evaluation - Treina, testa e avalia os dados segundo a métrica erro médio absoluto (em segundos)
get_features_by_line - Similar à makehistory_by_line
get_features_by_bus - Similar à makehistory_by_bus, porém cria um "vector assembler" das features e seleciona o último ônibus do dataframe.

#TESTES PARA VERIFICAR SE CADA UMA DAS FUNÇÕES ESTÁ FUNCIONANDO INDIVIDUALMENTE
df = get_features_by_line('864', -22.901986, -43.555818, -22.874226, -43.468544, 4)
df = get_features_by_bus(get_features_by_line('864', -22.901986, -43.555818, -22.874226, -43.468544, 4), 'D86200', 30.0, 1.0)       
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://cassio.cluster.teste.com/test.linha864_history").load()
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://cassio.cluster.teste.com/test.rio_bus_linha_864").load()
train_history(864)
df864 = makehistory_by_line('864', -22.901986, -43.555818, -22.874226, -43.468544, 4)
df565 = makehistory_by_line('565', -22.915587, -43.361235, -22.978734, -43.223393, 4)
df232 = makehistory_by_line('232', -22.909037, -43.170912, -22.903672, -43.290264, 4)
df639 = makehistory_by_line('639', -22.811110, -43.329860, -22.920703, -43.225008, 4)

offline_evaluation(864) - MAE 529 - COUNT 2283 - COUNT FONTE 16594
offline_evaluation(565) - MAE 1575 - COUNT 2465  - COUNT FONTE 14128
offline_evaluation(232) - MAE 564 - COUNT 1813 - COUNT FONTE 12383
offline_evaluation(639) - MAE 2516 - COUNT 2464  - COUNT FONTE 12002