from sec_cik_mapper import StockMapper
from pathlib import Path

mapper = StockMapper()

# Ler os tickers do arquivo
with open('tickers.txt', 'r') as f:
    tickers = [line.strip() for line in f if line.strip()]

# Obter CIKs para cada ticker usando o dicionário, não a função
ciks = []
for ticker in tickers:
    try:
        # ticker_to_cik é um dicionário, então acessamos como dict[ticker]
        cik = mapper.ticker_to_cik.get(ticker.upper())
        if cik:
            ciks.append(str(cik).zfill(10))  # Formatar como 10 dígitos com zeros à esquerda
        else:
            ciks.append(f"CIK não encontrado para {ticker}")
            print(f"Aviso: CIK não encontrado para {ticker}")
    except Exception as e:
        ciks.append(f"Erro para {ticker}: {str(e)}")
        print(f"Erro para {ticker}: {str(e)}")

# Escrever os CIKs no arquivo de saída
with open('CIK.txt', 'w') as f:
    for cik in ciks:
        f.write(cik + '\n')

print(f"Processamento concluído! {len([c for c in ciks if 'não encontrado' not in c and 'Erro' not in c])} CIKs válidos salvos em CIK.txt")