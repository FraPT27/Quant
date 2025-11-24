import requests
import json
from datetime import datetime, timedelta
import time
import os
import re
import sqlite3
from bs4 import BeautifulSoup

# Tags financeiras expandidas - versão completa
FINANCIAL_TAGS = {
    # Income Statement
    'Revenue': [
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomer',
        'SalesRevenueGoodsNet', 'SalesRevenueServicesNet'
    ],
    'COGS': [
        'CostOfGoodsAndServicesSold', 'CostOfRevenue', 
        'CostOfGoodsSold', 'CostOfSales'
    ],
    'Gross_Profit': ['GrossProfit'],
    'RD': ['ResearchAndDevelopmentExpense', 'ResearchAndDevelopment'],
    'SGA': ['SellingGeneralAndAdministrativeExpense'],
    'Operating_Income': ['OperatingIncomeLoss'],
    'Other_Income': ['NonoperatingIncomeExpense', 'OtherIncomeExpense'],
    'Pretax_Income': [
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxes',
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'
    ],
    'Taxes': ['IncomeTaxExpenseBenefit'],
    'Net_Income': ['NetIncomeLoss', 'ProfitLoss'],
    'EPS': ['EarningsPerShareBasic', 'EarningsPerShareBasicAndDiluted'],
    
    # Balance Sheet
    'Cash': [
        'CashAndCashEquivalentsAtCarryingValue', 
        'Cash', 'CashAndCashEquivalents'
    ],
    'Accounts_Receivable': [
        'AccountsReceivableNetCurrent', 
        'AccountsReceivableNet', 'AccountsReceivable'
    ],
    'Inventory': ['InventoryNet', 'Inventory'],
    'Assets_Current': ['AssetsCurrent'],
    'PPE': [
        'PropertyPlantAndEquipmentNet', 
        'PropertyPlantAndEquipment'
    ],
    'Total_Assets': ['Assets'],
    'Accounts_Payable': [
        'AccountsPayableCurrent', 
        'AccountsPayable'
    ],
    'Liabilities_Current': ['LiabilitiesCurrent'],
    'LongTerm_Debt': [
        'LongTermDebtNoncurrent', 
        'LongTermDebt'
    ],
    'Total_Liabilities': ['Liabilities'],
    
    # Cash Flow
    'Net_Income_CF': ['NetIncomeLoss'],
    'Depreciation': [
        'DepreciationDepletionAndAmortization', 
        'Depreciation'
    ],
    'Net_Cash_Operating': ['NetCashProvidedByUsedInOperatingActivities'],
    'CAPEX': ['PaymentsToAcquirePropertyPlantAndEquipment'],
    'Net_Cash_Investing': ['NetCashProvidedByUsedInInvestingActivities'],
    'Net_Cash_Financing': ['NetCashProvidedByUsedInFinancingActivities'],
    'Net_Cash_Change': [
        'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect',
        'IncreaseDecreaseInCashAndCashEquivalents'
    ]
}

def init_database():
    """Inicializa a base de dados SQLite"""
    conn = sqlite3.connect('financial_data.db')
    cursor = conn.cursor()
    
    # Tabela única simplificada
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_statements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            company_name TEXT NOT NULL,
            ticker TEXT NOT NULL,
            form_type TEXT NOT NULL,
            filing_date DATE NOT NULL,
            quarter INTEGER NOT NULL,
            year INTEGER NOT NULL,
            statement_type TEXT NOT NULL,
            
            -- Financial Metrics
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            unit TEXT,
            context_ref TEXT,
            
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cik, filing_date, statement_type, metric_name, context_ref)
        )
    ''')
    
    # Índices
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_ticker_date ON financial_statements(ticker, filing_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_cik_metric ON financial_statements(cik, metric_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_ticker_quarter ON financial_statements(ticker, year, quarter)')
    
    conn.commit()
    conn.close()
    print("✅ Base de dados inicializada")

def get_company_tickers():
    """Busca o mapeamento de tickers para CIKs"""
    headers = {
        'User-Agent': 'Company Data Analysis contact@company.com',
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
    """Converte ticker para CIK"""
    tickers_data = get_company_tickers()
    
    for company in tickers_data.values():
        if company['ticker'] == ticker.upper():
            return str(company['cik_str']).zfill(10)
    
    return None

def determine_quarter(filing_date, form_type):
    """Determina o quarter baseado na data"""
    date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
    year = date_obj.year
    month = date_obj.month
    
    if form_type == '10-K':
        return 4, year
    
    if 1 <= month <= 3:
        return 1, year
    elif 4 <= month <= 6:
        return 2, year
    elif 7 <= month <= 9:
        return 3, year
    else:
        return 4, year

def extract_values_direct_search(content, tag_variations, context_id="default"):
    """
    Busca direta por valores no conteúdo
    """
    values = {}
    
    for tag in tag_variations:
        # Padrões de busca
        patterns = [
            # Formato: <us-gaap:Tag>valor</us-gaap:Tag>
            f'<us-gaap:{tag}[^>]*>([^<]+)</us-gaap:{tag}>',
            # Formato: <Tag>valor</Tag>  
            f'<{tag}[^>]*>([^<]+)</{tag}>',
            # Formato com namespace diferente
            f'<[^>]*:{tag}[^>]*>([^<]+)</[^>]*:{tag}>',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                try:
                    value_text = match.strip()
                    if not value_text:
                        continue
                    
                    # Limpar e converter valor
                    value_text = value_text.replace(',', '')
                    is_negative = value_text.startswith('(') and value_text.endswith(')')
                    if is_negative:
                        value_text = value_text[1:-1]
                    
                    value = float(value_text)
                    if is_negative:
                        value = -value
                    
                    # Verificar se é um valor significativo (não zero ou muito pequeno)
                    if abs(value) > 0.01:
                        values[tag] = value
                        break  # Usar primeiro valor válido encontrado
                        
                except (ValueError, TypeError):
                    continue
    
    return values

def extract_financial_data_robust(content, filing_info):
    """
    Extração robusta de dados financeiros usando múltiplas estratégias
    """
    print("    🔍 Extraindo dados financeiros...")
    
    all_data = []
    found_metrics = 0
    
    # Para cada categoria financeira
    for statement_type, metrics in [
        ('Income Statement', ['Revenue', 'COGS', 'Gross_Profit', 'RD', 'SGA', 'Operating_Income', 
                             'Other_Income', 'Pretax_Income', 'Taxes', 'Net_Income', 'EPS']),
        ('Balance Sheet', ['Cash', 'Accounts_Receivable', 'Inventory', 'Assets_Current', 
                          'PPE', 'Total_Assets', 'Accounts_Payable', 'Liabilities_Current', 
                          'LongTerm_Debt', 'Total_Liabilities']),
        ('Cash Flow', ['Net_Income_CF', 'Depreciation', 'Net_Cash_Operating', 'CAPEX', 
                      'Net_Cash_Investing', 'Net_Cash_Financing', 'Net_Cash_Change'])
    ]:
        for metric in metrics:
            if metric in FINANCIAL_TAGS:
                tag_variations = FINANCIAL_TAGS[metric]
                values = extract_values_direct_search(content, tag_variations)
                
                for tag_found, value in values.items():
                    data_point = {
                        'cik': filing_info['cik'],
                        'company_name': filing_info['company_name'],
                        'ticker': filing_info['ticker'],
                        'form_type': filing_info['form_type'],
                        'filing_date': filing_info['filing_date'],
                        'quarter': filing_info['quarter'],
                        'year': filing_info['year'],
                        'statement_type': statement_type,
                        'metric_name': metric,
                        'metric_value': value,
                        'unit': 'USD',
                        'context_ref': f"{statement_type}_{metric}"
                    }
                    all_data.append(data_point)
                    found_metrics += 1
    
    print(f"    ✅ Encontrados {found_metrics} métricas financeiras")
    return all_data

def extract_from_html_tables(content, filing_info):
    """
    Extrai dados de tabelas HTML tradicionais (fallback)
    """
    print("    🔍 Procurando em tabelas HTML...")
    
    all_data = []
    soup = BeautifulSoup(content, 'html.parser')
    tables = soup.find_all('table')
    
    # Mapeamento de termos para métricas
    term_mapping = {
        'revenue': 'Revenue',
        'sales': 'Revenue',
        'cost of goods': 'COGS',
        'cost of sales': 'COGS',
        'gross profit': 'Gross_Profit',
        'research and development': 'RD',
        'selling, general': 'SGA',
        'operating income': 'Operating_Income',
        'operating loss': 'Operating_Income',
        'income before taxes': 'Pretax_Income',
        'income tax': 'Taxes',
        'net income': 'Net_Income',
        'earnings per share': 'EPS',
        'cash and cash equivalents': 'Cash',
        'accounts receivable': 'Accounts_Receivable',
        'inventory': 'Inventory',
        'property, plant and equipment': 'PPE',
        'total assets': 'Total_Assets',
        'accounts payable': 'Accounts_Payable',
        'total liabilities': 'Total_Liabilities',
        'long-term debt': 'LongTerm_Debt',
        'net cash provided by operating activities': 'Net_Cash_Operating',
        'net cash used in investing activities': 'Net_Cash_Investing',
        'net cash used in financing activities': 'Net_Cash_Financing',
        'capital expenditures': 'CAPEX'
    }
    
    found_metrics = 0
    
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                cell_text = cells[0].get_text().strip().lower()
                
                for term, metric in term_mapping.items():
                    if term in cell_text:
                        # Procurar valor nas células seguintes
                        for i in range(1, min(4, len(cells))):  # Verificar até 3 colunas
                            value_text = cells[i].get_text().strip()
                            if value_text:
                                try:
                                    # Limpar valor
                                    value_text = re.sub(r'[^\d\.\(\)\-]', '', value_text)
                                    value_text = value_text.replace(',', '')
                                    
                                    is_negative = value_text.startswith('(') and value_text.endswith(')')
                                    if is_negative:
                                        value_text = value_text[1:-1]
                                    
                                    value = float(value_text)
                                    if is_negative:
                                        value = -value
                                    
                                    if abs(value) > 0.01:
                                        # Determinar tipo de statement
                                        if metric in ['Revenue', 'COGS', 'Gross_Profit', 'RD', 'SGA', 
                                                     'Operating_Income', 'Other_Income', 'Pretax_Income', 
                                                     'Taxes', 'Net_Income', 'EPS']:
                                            statement_type = 'Income Statement'
                                        elif metric in ['Cash', 'Accounts_Receivable', 'Inventory', 
                                                       'Assets_Current', 'PPE', 'Total_Assets', 
                                                       'Accounts_Payable', 'Liabilities_Current', 
                                                       'LongTerm_Debt', 'Total_Liabilities']:
                                            statement_type = 'Balance Sheet'
                                        else:
                                            statement_type = 'Cash Flow'
                                        
                                        data_point = {
                                            'cik': filing_info['cik'],
                                            'company_name': filing_info['company_name'],
                                            'ticker': filing_info['ticker'],
                                            'form_type': filing_info['form_type'],
                                            'filing_date': filing_info['filing_date'],
                                            'quarter': filing_info['quarter'],
                                            'year': filing_info['year'],
                                            'statement_type': statement_type,
                                            'metric_name': metric,
                                            'metric_value': value,
                                            'unit': 'USD',
                                            'context_ref': 'HTML_Table'
                                        }
                                        all_data.append(data_point)
                                        found_metrics += 1
                                        break
                                
                                except (ValueError, TypeError):
                                    continue
                        break
    
    print(f"    ✅ Tabelas HTML: {found_metrics} métricas encontradas")
    return all_data

def process_financial_statements(file_path, filing_info):
    """Processa arquivos financeiros com múltiplas estratégias"""
    print(f"    📊 Processando: {os.path.basename(file_path)}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Determinar quarter e year
        quarter, year = determine_quarter(filing_info['filing_date'], filing_info['form_type'])
        filing_info['quarter'] = quarter
        filing_info['year'] = year
        
        all_data = []
        
        # Estratégia 1: Busca direta por tags XBRL/XML
        data1 = extract_financial_data_robust(content, filing_info)
        all_data.extend(data1)
        
        # Estratégia 2: Tabelas HTML (fallback)
        if not all_data:
            data2 = extract_from_html_tables(content, filing_info)
            all_data.extend(data2)
        else:
            # Mesmo que tenha encontrado via XBRL, tentar tabelas HTML também para métricas adicionais
            data2 = extract_from_html_tables(content, filing_info)
            all_data.extend(data2)
        
        # Salvar na base de dados
        if all_data:
            save_to_database(all_data)
            print(f"    💾 Salvos {len(all_data)} registros")
        else:
            print("    ⚠️  Nenhum dado financeiro encontrado")
            
            # Debug: mostrar amostra do conteúdo
            lines = content.split('\n')
            xbrl_lines = [line for line in lines if 'us-gaap:' in line][:5]
            if xbrl_lines:
                print("    🔍 Amostra de linhas com 'us-gaap:':")
                for line in xbrl_lines[:3]:
                    print(f"      {line.strip()[:100]}...")
        
        return len(all_data)
    
    except Exception as e:
        print(f"    ❌ Erro ao processar {file_path}: {e}")
        return 0

def save_to_database(data_points):
    """Salva dados na base de dados"""
    if not data_points:
        return
    
    conn = sqlite3.connect('financial_data.db')
    cursor = conn.cursor()
    
    try:
        saved_count = 0
        for data in data_points:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_statements 
                    (cik, company_name, ticker, form_type, filing_date, quarter, year, 
                     statement_type, metric_name, metric_value, unit, context_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['cik'], data['company_name'], data['ticker'], 
                    data['form_type'], data['filing_date'], data['quarter'], 
                    data['year'], data['statement_type'], data['metric_name'],
                    data['metric_value'], data['unit'], data['context_ref']
                ))
                saved_count += 1
            except Exception as e:
                print(f"    ❌ Erro ao salvar ponto de dados: {e}")
                continue
        
        conn.commit()
        print(f"    💾 Dados guardados: {saved_count}/{len(data_points)} registros")
        
    except Exception as e:
        print(f"    ❌ Erro ao salvar na BD: {e}")
        conn.rollback()
    
    finally:
        conn.close()

def download_filing(url, file_path, headers):
    """Faz download de um filing"""
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        return True
    except Exception as e:
        print(f"    ❌ Erro no download: {e}")
        return False

def get_company_filings(cik, filing_types, years_back=2):
    """Obtém filings de uma empresa"""
    headers = {
        'User-Agent': 'Financial Analysis Tool contact@company.com',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        cik_padded = str(cik).zfill(10)
        cik_short = cik_padded.lstrip('0')
        
        print(f"📋 Processando CIK: {cik_padded}")
        
        # API de submissions
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        response = requests.get(submissions_url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Informações da empresa
        company_name = data.get('name', 'Unknown')
        tickers = data.get('tickers', [])
        ticker = tickers[0] if tickers else "UNKNOWN"
        
        print(f"   🏢 {company_name} ({ticker})")
        
        # Criar pasta
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', company_name)
        company_dir = f"sec_filings/{cik_padded}_{ticker}_{safe_name}"
        os.makedirs(company_dir, exist_ok=True)
        
        # Filings recentes
        filings = data.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        accession_nums = filings.get('accessionNumber', [])
        filing_dates = filings.get('filingDate', [])
        primary_docs = filings.get('primaryDocument', [])
        
        cutoff_date = datetime.now() - timedelta(days=365 * years_back)
        
        downloaded_files = []
        total_data_points = 0
        
        for i in range(len(forms)):
            if forms[i] in filing_types:
                filing_date = filing_dates[i]
                filing_dt = datetime.strptime(filing_date, '%Y-%m-%d')
                
                if filing_dt >= cutoff_date:
                    acc_num = accession_nums[i].replace('-', '')
                    doc_name = primary_docs[i]
                    
                    filing_info = {
                        'accession_number': acc_num,
                        'filing_date': filing_date,
                        'primary_document': doc_name,
                        'form_type': forms[i],
                        'company_name': company_name,
                        'ticker': ticker,
                        'cik': cik_padded,
                        'quarter': None,
                        'year': None
                    }
                    
                    # URL do documento
                    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_num}/{doc_name}"
                    
                    print(f"   📄 {forms[i]} - {filing_date}")
                    
                    # Download
                    file_ext = os.path.splitext(doc_name)[1]
                    filename = f"{company_dir}/{ticker}_{forms[i]}_{filing_date.replace('-', '')}{file_ext}"
                    
                    if download_filing(doc_url, filename, headers):
                        downloaded_files.append(filename)
                        
                        # Processar dados financeiros
                        data_points = process_financial_statements(filename, filing_info)
                        total_data_points += data_points
                    
                    time.sleep(0.5)  # Rate limiting
        
        return {
            'cik': cik_padded,
            'ticker': ticker,
            'company_name': company_name,
            'downloaded_files': downloaded_files,
            'total_filings': len(downloaded_files),
            'total_data_points': total_data_points
        }
        
    except Exception as e:
        print(f"❌ Erro com CIK {cik}: {e}")
        return None

def read_cik_list(filename):
    """Lê lista de CIKs/tickers do arquivo"""
    entries = []
    
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    if line.isdigit():
                        entries.append(('cik', line))
                    else:
                        entries.append(('ticker', line.upper()))
        
        print(f"📁 {len(entries)} entradas carregadas de {filename}")
        return entries
    
    except FileNotFoundError:
        print(f"❌ Arquivo {filename} não encontrado!")
        return []

def process_companies(input_file, filing_types=['10-Q', '10-K'], years_back=2):
    """Processa múltiplas empresas"""
    init_database()
    os.makedirs("sec_filings", exist_ok=True)
    
    entries = read_cik_list(input_file)
    if not entries:
        return None
    
    results = []
    
    for entry_type, value in entries:
        print(f"\n{'='*60}")
        print(f"🔄 Processando: {value} ({entry_type})")
        print(f"{'='*60}")
        
        if entry_type == 'ticker':
            cik = get_cik_from_ticker(value)
            if not cik:
                print(f"❌ Ticker {value} não encontrado")
                continue
            print(f"✅ {value} → CIK {cik}")
        else:
            cik = value
        
        result = get_company_filings(cik, filing_types, years_back)
        results.append(result)
        
        time.sleep(1)  # Rate limiting entre empresas
    
    return results

def show_summary(results):
    """Mostra resumo do processamento"""
    print(f"\n{'='*60}")
    print("📊 RESUMO FINAL")
    print(f"{'='*60}")
    
    valid_results = [r for r in results if r]
    total_files = sum(r['total_filings'] for r in valid_results)
    total_data = sum(r['total_data_points'] for r in valid_results)
    
    print(f"Empresas processadas: {len(valid_results)}")
    print(f"Arquivos baixados: {total_files}")
    print(f"Dados extraídos: {total_data}")
    
    print(f"\n📈 DETALHES:")
    for result in valid_results:
        if result['total_data_points'] > 0:
            print(f"  ✅ {result['ticker']} - {result['company_name']}")
            print(f"     Arquivos: {result['total_filings']}, Dados: {result['total_data_points']}")

def show_database_stats():
    """Mostra estatísticas da base de dados"""
    conn = sqlite3.connect('financial_data.db')
    cursor = conn.cursor()
    
    try:
        print(f"\n{'='*60}")
        print("🗃️  ESTATÍSTICAS DA BASE DE DADOS")
        print(f"{'='*60}")
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM financial_statements")
        total_records = cursor.fetchone()[0]
        print(f"Total de registros: {total_records}")
        
        # Empresas únicas
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_statements")
        unique_companies = cursor.fetchone()[0]
        print(f"Empresas únicas: {unique_companies}")
        
        # Métricas por tipo
        cursor.execute('''
            SELECT statement_type, metric_name, COUNT(*) as count
            FROM financial_statements 
            GROUP BY statement_type, metric_name
            ORDER BY statement_type, count DESC
        ''')
        
        print(f"\n📋 MÉTRICAS POR TIPO:")
        current_type = None
        for stype, metric, count in cursor.fetchall():
            if stype != current_type:
                print(f"\n  {stype}:")
                current_type = stype
            print(f"    {metric}: {count}")
        
        # Quarters recentes
        cursor.execute('''
            SELECT year, quarter, COUNT(*) 
            FROM financial_statements 
            GROUP BY year, quarter 
            ORDER BY year DESC, quarter DESC 
            LIMIT 6
        ''')
        
        print(f"\n📅 QUARTERS RECENTES:")
        for year, quarter, count in cursor.fetchall():
            print(f"  {year} Q{quarter}: {count} registros")
    
    finally:
        conn.close()

# EXECUÇÃO PRINCIPAL
if __name__ == "__main__":
    print("🚀 EXTRATOR DE DADOS FINANCEIROS - SEC EDGAR")
    print("=" * 60)
    
    input_file = "CIK.txt"
    
    if not os.path.exists(input_file):
        print(f"❌ {input_file} não encontrado!")
        print("\n💡 Crie o arquivo CIK.txt com:")
        print("   AAPL")
        print("   MSFT") 
        print("   GOOGL")
        print("   0000320193  # Apple CIK")
        print("   0000789019  # Microsoft CIK")
    else:
        print("⚙️  Configuração:")
        print(f"   Arquivo de entrada: {input_file}")
        print(f"   Período: últimos 2 anos")
        print(f"   Formulários: 10-Q, 10-K")
        print("=" * 60)
        
        results = process_companies(input_file)
        
        if results:
            show_summary(results)
            show_database_stats()
            
            print(f"\n✅ PROCESSAMENTO CONCLUÍDO!")
            print(f"📁 Arquivos em: sec_filings/")
            print(f"🗃️  Base de dados: financial_data.db")
            print(f"\n💡 Consultas SQL:")
            print(f"   SELECT * FROM financial_statements WHERE ticker = 'AAPL';")
            print(f"   SELECT metric_name, metric_value FROM financial_statements WHERE ticker = 'MSFT' AND statement_type = 'Income Statement';")