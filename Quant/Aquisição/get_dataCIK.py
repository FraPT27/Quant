import requests
import json
from datetime import datetime, timedelta
import time
import os
import re

def get_company_tickers():
    """
    Busca o mapeamento de tickers para CIKs usando a API do EDGAR
    """
    headers = {
        'User-Agent': 'Your Name your.email@domain.com',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro ao buscar tickers: {e}")
        return {}

def get_cik_from_ticker(ticker):
    """
    Converte ticker para CIK usando a API do EDGAR
    """
    tickers_data = get_company_tickers()
    
    for company in tickers_data.values():
        if company['ticker'] == ticker.upper():
            return str(company['cik_str']).zfill(10)
    
    return None

def get_filings_edgar(cik, filing_types, years_back=2):
    """
    Busca filings (10-Q e 10-K) para um CIK usando a API do EDGAR
    """
    headers = {
        'User-Agent': 'Your Name your.email@domain.com',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        cik_padded = str(cik).zfill(10)
        cik_short = cik_padded.lstrip('0')
        
        print(f"Processando CIK: {cik_padded}")
        
        # URL da API de submissions do EDGAR
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        
        response = requests.get(submissions_url, headers=headers)
        response.raise_for_status()
        submissions_data = response.json()
        
        # Extrair informações da empresa
        company_name = submissions_data.get('name', 'Unknown Company')
        tickers = submissions_data.get('tickers', [])
        ticker = tickers[0] if tickers else "NO_TICKER"
        
        print(f"Empresa: {company_name}")
        print(f"Ticker: {ticker}")
        
        # Criar pasta para a empresa
        safe_company_name = re.sub(r'[<>:"/\\|?*]', '_', company_name)
        company_folder = f"dataCIK/{cik_padded}_{ticker}_{safe_company_name}"
        os.makedirs(company_folder, exist_ok=True)
        
        # Buscar filings recentes
        recent_filings = submissions_data.get('filings', {}).get('recent', {})
        forms = recent_filings.get('form', [])
        accession_numbers = recent_filings.get('accessionNumber', [])
        filing_dates = recent_filings.get('filingDate', [])
        primary_documents = recent_filings.get('primaryDocument', [])
        
        # Calcular data limite
        cutoff_date = datetime.now() - timedelta(days=years_back * 365)
        
        found_filings = []
        
        for i, form in enumerate(forms):
            if form in filing_types:
                filing_date = filing_dates[i]
                filing_datetime = datetime.strptime(filing_date, '%Y-%m-%d')
                
                # Verificar se está dentro do período
                if filing_datetime >= cutoff_date:
                    accession_number = accession_numbers[i].replace('-', '')
                    primary_doc = primary_documents[i]
                    
                    
                    
                    filing_info = {
                        'accession_number': accession_number,
                        'filing_date': filing_date,
                        'primary_document': primary_doc,
                        'form_type': form,
                        'company_name': company_name,
                        'ticker': ticker,
                        'cik': cik_padded
                    }
                    found_filings.append(filing_info)
        
        # Ordenar por data (mais recente primeiro)
        found_filings.sort(key=lambda x: x['filing_date'], reverse=True)
        
        print(f"Encontrados {len(found_filings)} filings no período")
        
        # Download dos filings
        downloaded_files = []
        for filing in found_filings:
            # Construir URL do documento
            document_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{filing['accession_number']}/{filing['primary_document']}"
            
            print(f"  - {filing['form_type']} de {filing['filing_date']}")
            
            filename = download_document_edgar(document_url, company_folder, filing, headers)
            if filename:
                downloaded_files.append(filename)
            
            # Respeitar rate limiting da API
            time.sleep(0.5)
        
        return {
            'cik': cik_padded,
            'ticker': ticker,
            'company_name': company_name,
            'downloaded_files': downloaded_files,
            'total_filings': len(found_filings)
        }
            
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"❌ CIK {cik} não encontrado na API do EDGAR")
        else:
            print(f"❌ Erro HTTP para CIK {cik}: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro geral para CIK {cik}: {e}")
        return None

def download_document_edgar(url, company_folder, filing_info, headers):
    """
    Faz o download do documento usando a API do EDGAR
    """
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Criar nome do arquivo
        filename = f"{company_folder}/{filing_info['ticker']}_{filing_info['form_type']}_{filing_info['filing_date']}.txt"
        
        # Salvar o conteúdo
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"    ✅ Salvo: {filename} ({len(response.text)} caracteres)")
        
        return filename
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"    ❌ Documento não encontrado: {url}")
        else:
            print(f"    ❌ Erro HTTP ao baixar: {e}")
        return None
    except Exception as e:
        print(f"    ❌ Erro ao baixar documento: {e}")
        return None

def process_ciks_from_file_edgar(filename, filing_types=['10-Q', '10-K'], years_back=2):
    """
    Processa uma lista de CIKs a partir de um arquivo usando API EDGAR
    """
    try:
        os.makedirs("dataCIK", exist_ok=True)
        
        with open(filename, 'r') as file:
            lines = [line.strip() for line in file if line.strip()]
        
        print(f"📁 Encontrados {len(lines)} entradas no arquivo")
        
        all_results = []
        for line in lines:
            print(f"\n{'='*50}")
            print(f"🔄 Processando: {line}")
            print(f"{'='*50}")
            
            # Verificar se é CIK ou ticker
            if line.isdigit():
                # É um CIK
                cik = line
                ticker = "UNKNOWN"
            else:
                # É um ticker, converter para CIK
                ticker = line.upper()
                cik = get_cik_from_ticker(ticker)
                if not cik:
                    print(f"❌ Ticker {ticker} não encontrado")
                    continue
                print(f"✅ Ticker {ticker} -> CIK {cik}")
            
            results = get_filings_edgar(cik, filing_types, years_back)
            all_results.append(results)
            
            # Respeitar rate limiting entre empresas
            time.sleep(1)
        
        return all_results
        
    except FileNotFoundError:
        print(f"❌ Arquivo {filename} não encontrado!")
        return None
    except Exception as e:
        print(f"❌ Erro ao processar arquivo: {e}")
        return None

def generate_detailed_summary(results):
    """
    Gera um resumo detalhado do processamento
    """
    print(f"\n{'='*60}")
    print("📊 RESUMO DETALHADO DO PROCESSAMENTO")
    print(f"{'='*60}")
    
    successful = [r for r in results if r is not None and r.get('total_filings', 0) > 0]
    failed = [r for r in results if r is None]
    no_filings = [r for r in results if r is not None and r.get('total_filings', 0) == 0]
    
    total_files = 0
    
    print(f"\n✅ EMPRESAS COM SUCESSO ({len(successful)}):")
    for result in successful:
        if result:
            print(f"   {result['cik']} - {result['company_name']} ({result['ticker']})")
            print(f"      📂 {result['total_filings']} arquivos baixados")
            total_files += result['total_filings']
            
            # Listar arquivos baixados
            for file in result['downloaded_files']:
                print(f"        📄 {os.path.basename(file)}")
    
    if no_filings:
        print(f"\n⚠️  EMPRESAS SEM FILINGS ({len(no_filings)}):")
        for result in no_filings:
            if result:
                print(f"   {result['cik']} - {result['company_name']} ({result['ticker']})")
    
    if failed:
        print(f"\n❌ EMPRESAS COM FALHA ({len(failed)}):")
        # As falhas já foram mostradas durante o processamento
    
    print(f"\n{'='*60}")
    print("📈 ESTATÍSTICAS FINAIS:")
    print(f"{'='*60}")
    print(f"Total de empresas processadas: {len(results)}")
    print(f"Empresas com sucesso: {len(successful)}")
    print(f"Empresas sem filings: {len(no_filings)}")
    print(f"Empresas com falha: {len(failed)}")
    print(f"Total de arquivos baixados: {total_files}")
    print(f"Localização: dataCIK/")

def validate_cik(cik):
    """
    Valida se um CIK existe na API do EDGAR
    """
    headers = {
        'User-Agent': 'Your Name your.email@domain.com',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        cik_padded = str(cik).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        
        response = requests.get(url, headers=headers)
        return response.status_code == 200
    except:
        return False

# Executar
if __name__ == "__main__":
    input_file = "CIK.txt"
    filing_types = ['10-Q', '10-K']
    years_back = 2
    
    print(f"🚀 BUSCANDO FILINGS VIA API EDGAR")
    print(f"Arquivo de entrada: {input_file}")
    print(f"Tipos de filings: {', '.join(filing_types)}")
    print(f"Período: últimos {years_back} ano(s)")
    print(f"📝 Baixando APENAS arquivos .txt")
    print(f"{'='*60}")
    
    # Verificar se o arquivo existe
    if not os.path.exists(input_file):
        print(f"❌ Arquivo {input_file} não encontrado!")
        print("💡 Crie um arquivo CIK.txt com CIKs ou tickers (um por linha)")
    else:
        results = process_ciks_from_file_edgar(input_file, filing_types, years_back)
        
        if results:
            generate_detailed_summary(results)
            print(f"\n✅ CONCLUÍDO! Verifique a pasta dataCIK/")
        else:
            print("❌ Nenhum resultado foi processado.")

    print(f"\n💡 DICAS:")
    print(f"- Para Apple, use CIK: 0000320193")
    print(f"- Para Microsoft, use CIK: 0000789019") 
    print(f"- Para Google, use CIK: 0001652044")
    print(f"- Para Amazon, use CIK: 0001018724")
    print(f"- Ou use diretamente os tickers: AAPL, MSFT, GOOGL, AMZN") 