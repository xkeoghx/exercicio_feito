import boto3, json, traceback, sys
from collections import OrderedDict

_SQS_CLIENT = None
queue_name="valid-events-queue"

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila

    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")



def open_schema(input: dict):
    '''
    Função recursiva para extrair e retornar o schema esperado
    Utiliza a entrada(input) que deve ser importado do schema.json

    :param input: dicionario contendo o schema esperado  (dict)    
    :return: estrutura (dict)
    '''
    tipo_campo = OrderedDict()
    data_type = input["properties"]

    for chave in data_type:
        if data_type[chave]["type"] == "object":
            tipo_campo = tipo_campo | open_schema(data_type[chave]) 
        else:
            tipo_campo[chave] = data_type[chave]["type"]
    
    return tipo_campo


def determina_tipo(input: object):
    '''
    Função suporte para determinar e retornar o datatype da entrada(input)
    
    :param input: entrada que necessita determinar o datatype  (object)    
    :return: o datatype detectado a partir da entrada (str)
    '''
    if type(input) is str:
        return 'string'
    elif type(input) is int:
        return 'integer'
    elif type(input) is float:
        return 'numeric'
    elif type(input) is bool:
        return 'boolean'
    else:
        return 'object'


def detect_schema(event: dict):
    '''
    Função suporte para determinar e retornar o datatype de cada campo da entrada(event)
    
    :param event: entrada que necessita determinar o datatype  (object)    
    :return: o datatype detectado a partir da entrada (str)
    '''
    tipo_campo = OrderedDict()

    for field in event:
        if determina_tipo(event[field]) == "object":
            tipo_campo = tipo_campo | detect_schema(event[field])
        else:
            tipo_campo[field] = determina_tipo(event[field])
    
    return tipo_campo


def handler(event: dict):
    '''
    #  Função principal que é sensibilizada para cada evento

    Função que tem como objetivo garantir que o evento de entrada siga 
    os padroes definido pelo arquivo schema.json.
    Caso esteja no padrão pode enviar para a fila 

    1. Tipo do campo do evento deve bater com o do schema.
    2. Não deve aceitar campos não cadastrados no schema.
    3. A ordem dos campos deve ser mantida a apresentada no schema.
    '''

    try:

        with open('schema.json', 'r') as f:
            data = json.load(f)

        if detect_schema(event) == open_schema(data):
            print("Evento passou no teste")
            send_event_to_queue(event, queue_name)
        else:
            print("Evento falhou no teste")
    
    except:
        print(traceback.format_exc())
        