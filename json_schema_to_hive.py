import json

_ATHENA_CLIENT = None


def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def open_schema(input: dict):
    query_campos = ""
    data_type = input["properties"]

    for chave in data_type:
        if data_type[chave]["type"] == "object":             
            return query_campos + open_schema(data_type[chave])
        else:
            query_campos += "\n    "
            query_campos += f"{chave} {data_type[chave]['type']} COMMENT '{data_type[chave]['title']}',"
    
    return query_campos

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    with open('schema.json', 'r') as f:
        data = json.load(f)
    

    query = f"CREATE TABLE IF NOT EXISTS teste ({open_schema(data)[:-1]})\n STORED AS PARQUET;"    
    
    create_hive_table_with_athena(query)




