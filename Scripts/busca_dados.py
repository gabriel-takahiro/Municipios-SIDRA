import concurrent.futures
import requests
import math
import os
import pandas as pd

def processar_anos(url, info, anos, contador):
    response = requests.get(url.replace('p/all', f'p/{",".join(anos)}'))
    if response.status_code == 200:
        dados_json = response.json()

        if len(dados_json) > 1:
            dados_json = dados_json[1:]
            chaves_mantidas = ['D1C', 'D3C', 'V']
            dados_filtrados = [{chave: json[chave] for chave in chaves_mantidas} for json in dados_json]
            df = pd.DataFrame(dados_filtrados)
            df = df.rename(columns={'V': 'Resultado', 'D1C': 'Código do município', 'D3C': 'Ano'})
            df['Resultado'] = pd.to_numeric(df['Resultado'], errors='coerce')
            df = df.dropna(subset=['Resultado'])
            df.insert(1, 'Indicador', info)

            contador.value += 1
            porcentagem = round(100 * contador.value / contador.total, 2)
            porcentagem_completa = int(str(porcentagem/10)[0]) if porcentagem != 100 else 10
            os.system('cls')
            print(f'Operação: {porcentagem} % concluída \n[{"■" * porcentagem_completa*4}{"□" * (40 - porcentagem_completa*4)}]')

            return df
        else:
            print(f'Não há dados para os anos {", ".join(anos)}')
            return None
    else:
        print(f'Código de status: {response.status_code}')
        return None


def busca_sidra(dados):
    df_geral = pd.DataFrame(columns={})
    total_tarefas = sum(math.ceil(len(d['anos']) / 5) for d in dados)

    class Contador:
        def __init__(self, total):
            self.value = 0
            self.total = total

    contador = Contador(total_tarefas)

    os.system('cls')
    print(f'Operação: 00.00 % concluída \n[{"□" * 40}]')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futuros = []  # Lista para armazenar os futuros das tarefas
        for d in dados:
            info = d['info']
            url = d['url']
            anos = d['anos']
            
            # Divide a lista de anos em pedaços de tamanho máximo 5
            for i in range(0, len(anos), 5):
                chunk = anos[i:i+5]
                
                # Mapeia a função processar_anos para cada pedaço de anos em paralelo
                futuro = executor.submit(processar_anos, url, info, chunk, contador)
                futuros.append(futuro)  # Adiciona o futuro à lista
            
        # Espera pela conclusão de todos os futuros e coleta os resultados
        for futuro in concurrent.futures.as_completed(futuros):
            df = futuro.result()  # Obtém o resultado do futuro
            if df is not None:
                df_geral = pd.concat([df_geral, df], ignore_index=True)

    return df_geral



def main():
    anos = [str(ano) for ano in range(2002, 2022)]
    anos_censos = ['1970', '1980', '1991', '2000', '2010']

    dados = [{'info': 'População (Total)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/0/d/v93%200', 'anos': anos_censos},
            {'info': 'População (Homens)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/4/c1/0/c58/0/d/v93%200', 'anos': anos_censos},
            {'info': 'População (Mulheres)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/5/c1/0/c58/0/d/v93%200', 'anos': anos_censos},
            {'info': 'População (Rural)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/2/c58/0/d/v93%200', 'anos': anos_censos},
            {'info': 'População (Urbana)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/1/c58/0/d/v93%200', 'anos': anos_censos},
            {'info': 'População (0 a 4 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1140/d/v93%200', 'anos': anos_censos},
            {'info': 'População (5 a 9 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1141/d/v93%200', 'anos': anos_censos},
            {'info': 'População (10 a 14 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1142/d/v93%200', 'anos': anos_censos},
            {'info': 'População (15 a 19 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1143/d/v93%200', 'anos': anos_censos},
            {'info': 'População (20 a 24 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1144/d/v93%200', 'anos': anos_censos},
            {'info': 'População (25 a 29 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1145/d/v93%200', 'anos': anos_censos},
            {'info': 'População (30 a 34 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1146/d/v93%200', 'anos': anos_censos},
            {'info': 'População (35 a 39 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1147/d/v93%200', 'anos': anos_censos},
            {'info': 'População (40 a 44 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1148/d/v93%200', 'anos': anos_censos},
            {'info': 'População (45 a 49 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1149/d/v93%200', 'anos': anos_censos},
            {'info': 'População (50 a 54 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1150/d/v93%200', 'anos': anos_censos},
            {'info': 'População (55 a 59 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1151/d/v93%200', 'anos': anos_censos},
            {'info': 'População (60 a 64 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1152/d/v93%200', 'anos': anos_censos},
            {'info': 'População (65 a 69 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1153/d/v93%200', 'anos': anos_censos},
            {'info': 'População (70 a 74 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1154/d/v93%200', 'anos': anos_censos},
            {'info': 'População (75 a 79 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/1155/d/v93%200', 'anos': anos_censos},
            {'info': 'População (80 anos ou mais)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/2503/d/v93%200', 'anos': ['1970', '1980', '1991']},
            {'info': 'População (80 anos ou mais)', 'url': 'https://apisidra.ibge.gov.br/values/t/200/n6/all/u/y/v/allxp/p/all/c2/0/c1/0/c58/6802%206803%2092963%2092964%2092965/d/v93%200', 'anos': ['2000', '2010']},
            {'info': 'População (Total)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/100362/c286/113635', 'anos': ['2022']},
            {'info': 'População (Homens)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/4/c287/100362/c286/113635', 'anos': ['2022']},
            {'info': 'População (Mulheres)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/5/c287/100362/c286/113635', 'anos': ['2022']},
            {'info': 'População (0 a 4 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93070/c286/113635', 'anos': ['2022']},
            {'info': 'População (5 a 9 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93084/c286/113635', 'anos': ['2022']},
            {'info': 'População (10 a 14 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93085/c286/113635', 'anos': ['2022']},
            {'info': 'População (15 a 19 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93086/c286/113635', 'anos': ['2022']},
            {'info': 'População (20 a 24 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93087/c286/113635', 'anos': ['2022']},
            {'info': 'População (25 a 29 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93088/c286/113635', 'anos': ['2022']},
            {'info': 'População (30 a 34 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93089/c286/113635', 'anos': ['2022']},
            {'info': 'População (35 a 39 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93090/c286/113635', 'anos': ['2022']},
            {'info': 'População (40 a 44 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93091/c286/113635', 'anos': ['2022']},
            {'info': 'População (45 a 49 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93092/c286/113635', 'anos': ['2022']},
            {'info': 'População (50 a 54 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93093/c286/113635', 'anos': ['2022']},
            {'info': 'População (55 a 59 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93094/c286/113635', 'anos': ['2022']},
            {'info': 'População (60 a 64 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93095/c286/113635', 'anos': ['2022']},
            {'info': 'População (65 a 69 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93096/c286/113635', 'anos': ['2022']},
            {'info': 'População (70 a 74 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93097/c286/113635', 'anos': ['2022']},
            {'info': 'População (75 a 79 anos)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/93098/c286/113635', 'anos': ['2022']},
            {'info': 'População (80 anos ou mais)', 'url': 'https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/6794/c287/6653%2049108%2049109%2060040%2060041/c286/113635', 'anos': ['2022']},
            {'info': 'PIB (Total)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/all/d/v37%200', 'anos': anos},
            {'info': 'PIB (Impostos)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/543/p/all/d/v543%200', 'anos': anos},
            {'info': 'PIB (Agropecuária)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/513/p/all/d/v513%200', 'anos': anos},
            {'info': 'PIB (Indústria)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/517/p/all/d/v517%200', 'anos': anos},
            {'info': 'PIB (Serviços)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/6575/p/all/d/v6575%200', 'anos': anos},
            {'info': 'PIB (Administração)', 'url': 'https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/525/p/all/d/v525%200', 'anos': anos}]

    indicadores = [{'Indicador': 'População (Total)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (Homens)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (Mulheres)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (Rural)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (Urbana)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (0 a 4 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (5 a 9 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (10 a 14 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (15 a 19 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (20 a 24 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (25 a 29 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (30 a 34 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (35 a 39 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (40 a 44 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (45 a 49 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (50 a 54 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (55 a 59 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (60 a 64 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (65 a 69 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (70 a 74 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (75 a 79 anos)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'População (80 anos ou mais)', 'Unidade de medida': 'Pessoas'},
               {'Indicador': 'PIB (Total)', 'Unidade de medida': 'Mil Reais'},
               {'Indicador': 'PIB (Impostos)', 'Unidade de medida': 'Mil Reais'},
               {'Indicador': 'PIB (Agropecuária)', 'Unidade de medida': 'Mil Reais'},
               {'Indicador': 'PIB (Indústria)', 'Unidade de medida': 'Mil Reais'},
               {'Indicador': 'PIB (Serviços)', 'Unidade de medida': 'Mil Reais'},
               {'Indicador': 'PIB (Administração)', 'Unidade de medida': 'Mil Reais'}]

    df_indicadores = pd.DataFrame(indicadores)
    df_dados = busca_sidra(dados)

    script_dir = os.path.dirname(os.path.realpath(__file__))
    output_dir = os.path.join(script_dir, '..', 'Dados')

    df_dados.to_csv(os.path.join(output_dir, 'dados_sidra.csv'), sep=';', index=False)
    df_indicadores.to_csv(os.path.join(output_dir, 'indicadores.csv'), sep=';', index=False)



if __name__ == '__main__':
    main()