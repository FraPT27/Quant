import requests
import sqlite3
from datetime import datetime

# Configuração
NUM_QUARTERS = 8  # Número de quarters a mostrar
HEADERS = {
    'User-Agent': 'SeuNome seu@email.com',  # IMPORTANTE: Substituir com seu email
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

def get_company_facts(cik):
    """Obtém os dados financeiros da empresa do SEC API"""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter dados: {e}")
        return None

def extract_quarterly_data(facts_data, metric_name, units='USD'):
    """Extrai dados trimestrais de uma métrica específica"""
    try:
        us_gaap = facts_data['facts']['us-gaap']
        
        if metric_name not in us_gaap:
            return [], False
        
        metric_data = us_gaap[metric_name]['units'].get(units, [])
        
        # Filtra apenas dados trimestrais (10-Q) e anuais (10-K)
        data = [
            item for item in metric_data 
            if item.get('form') in ['10-Q', '10-K']
        ]
        
        # Ordena por data (mais recente primeiro)
        data.sort(key=lambda x: (x.get('end', ''), x.get('filed', '')), reverse=True)
        
        return data, True
    except (KeyError, TypeError):
        return [], False

def format_value(value):
    """Retorna valor numérico limpo"""
    if value is None:
        return None
    try:
        return float(value)
    except:
        return None

def create_database(db_name):
    """Cria a base de dados e as tabelas"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Tabela principal com todos os dados
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_statements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            cik TEXT,
            fiscal_year INTEGER,
            fiscal_period TEXT,
            end_date TEXT,
            filed_date TEXT,
            statement_type TEXT,
            metric_name TEXT,
            metric_tag TEXT,
            value_type TEXT,
            value REAL,
            units TEXT,
            extraction_date TEXT
        )
    ''')
    
    # Tabela Income Statement
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS income_statement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            cik TEXT,
            fiscal_year INTEGER,
            fiscal_period TEXT,
            end_date TEXT,
            metric_name TEXT,
            value REAL
        )
    ''')
    
    # Tabela Balance Sheet
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS balance_sheet (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            cik TEXT,
            fiscal_year INTEGER,
            fiscal_period TEXT,
            end_date TEXT,
            metric_name TEXT,
            value REAL
        )
    ''')
    
    # Tabela Cash Flow Statement
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cash_flow_statement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            cik TEXT,
            fiscal_year INTEGER,
            fiscal_period TEXT,
            end_date TEXT,
            metric_name TEXT,
            value REAL
        )
    ''')
    
    conn.commit()
    return conn

def insert_data(conn, company_name, cik, fy, fp, end_date, filed, statement_type, 
                metric_name, metric_tag, value_type, value, units):
    """Insere dados na base de dados"""
    cursor = conn.cursor()
    extraction_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Insere na tabela principal
    cursor.execute('''
        INSERT INTO financial_statements 
        (company_name, cik, fiscal_year, fiscal_period, end_date, filed_date, 
         statement_type, metric_name, metric_tag, value_type, value, units, extraction_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (company_name, cik, fy, fp, end_date, filed, statement_type, 
          metric_name, metric_tag, value_type, value, units, extraction_date))
    
    # Insere na tabela específica se for valor trimestral
    if value_type == 'Quarterly':
        table_map = {
            'Income Statement': 'income_statement',
            'Balance Sheet': 'balance_sheet',
            'Cash Flow Statement': 'cash_flow_statement'
        }
        
        table_name = table_map.get(statement_type)
        if table_name:
            cursor.execute(f'''
                INSERT INTO {table_name}
                (company_name, cik, fiscal_year, fiscal_period, end_date, metric_name, value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (company_name, cik, fy, fp, end_date, metric_name, value))
    
    conn.commit()

def process_company(cik, conn):
    """Processa dados de uma empresa"""
    print(f"\nProcessando CIK {cik}...")
    
    company_data = get_company_facts(cik)
    if not company_data:
        print(f"  ✗ Falha ao obter dados para CIK {cik}")
        return False
    
    company_name = company_data.get('entityName', 'Unknown')
    print(f"  Empresa: {company_name}")
    
    # Métricas organizadas por demonstração financeira
    metrics = {
        'Income Statement': [
            ('RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenue', 'USD'),
            ('CostOfGoodsAndServicesSold', 'Cost of Goods and Services Sold', 'USD'),
            ('GrossProfit', 'Gross Profit', 'USD'),
            ('ResearchAndDevelopmentExpense', 'Research and Development', 'USD'),
            ('SellingGeneralAndAdministrativeExpense', 'SG&A Expense', 'USD'),
            ('OperatingExpenses', 'Operating Expenses', 'USD'),
            ('OperatingIncomeLoss', 'Operating Income', 'USD'),
            ('NonoperatingIncomeExpense', 'Nonoperating Income', 'USD'),
            ('IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'Pretax Income', 'USD'),
            ('IncomeTaxExpenseBenefit', 'Income Tax Expense', 'USD'),
            ('NetIncomeLoss', 'Net Income', 'USD'),
            ('EarningsPerShareBasic', 'EPS Basic', 'USD/shares'),
        ],
        'Balance Sheet': [
            ('CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'USD'),
            ('MarketableSecuritiesCurrent', 'Marketable Securities - Current', 'USD'),
            ('AccountsReceivableNetCurrent', 'Accounts Receivable - Current', 'USD'),
            ('NontradeReceivablesCurrent', 'Nontrade Receivables - Current', 'USD'),
            ('InventoryNet', 'Inventory', 'USD'),
            ('OtherAssetsCurrent', 'Other Current Assets', 'USD'),
            ('AssetsCurrent', 'Total Current Assets', 'USD'),
            ('MarketableSecuritiesNoncurrent', 'Marketable Securities - Noncurrent', 'USD'),
            ('PropertyPlantAndEquipmentNet', 'Property Plant and Equipment', 'USD'),
            ('OtherAssetsNoncurrent', 'Other Noncurrent Assets', 'USD'),
            ('AssetsNoncurrent', 'Total Noncurrent Assets', 'USD'),
            ('Assets', 'Total Assets', 'USD'),
            ('AccountsPayableCurrent', 'Accounts Payable', 'USD'),
            ('OtherLiabilitiesCurrent', 'Other Current Liabilities', 'USD'),
            ('ContractWithCustomerLiabilityCurrent', 'Deferred Revenue', 'USD'),
            ('CommercialPaper', 'Commercial Paper', 'USD'),
            ('LongTermDebtCurrent', 'Long-Term Debt - Current', 'USD'),
            ('LiabilitiesCurrent', 'Total Current Liabilities', 'USD'),
            ('LongTermDebtNoncurrent', 'Long-Term Debt - Noncurrent', 'USD'),
            ('OtherLiabilitiesNoncurrent', 'Other Noncurrent Liabilities', 'USD'),
            ('LiabilitiesNoncurrent', 'Total Noncurrent Liabilities', 'USD'),
            ('Liabilities', 'Total Liabilities', 'USD'),
        ],
        'Cash Flow Statement': [
            ('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents', 'Cash Beginning', 'USD'),
            ('NetIncomeLoss', 'Net Income', 'USD'),
            ('DepreciationDepletionAndAmortization', 'Depreciation and Amortization', 'USD'),
            ('ShareBasedCompensation', 'Share-Based Compensation', 'USD'),
            ('OtherNoncashIncomeExpense', 'Other Noncash Items', 'USD'),
            ('IncreaseDecreaseInAccountsReceivable', 'Change in Accounts Receivable', 'USD'),
            ('IncreaseDecreaseInOtherReceivables', 'Change in Other Receivables', 'USD'),
            ('IncreaseDecreaseInInventories', 'Change in Inventories', 'USD'),
            ('IncreaseDecreaseInOtherOperatingAssets', 'Change in Other Operating Assets', 'USD'),
            ('IncreaseDecreaseInAccountsPayable', 'Change in Accounts Payable', 'USD'),
            ('IncreaseDecreaseInOtherOperatingLiabilities', 'Change in Other Operating Liabilities', 'USD'),
            ('NetCashProvidedByUsedInOperatingActivities', 'Operating Cash Flow', 'USD'),
            ('PaymentsToAcquireAvailableForSaleSecuritiesDebt', 'Purchase of Securities', 'USD'),
            ('ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities', 'Proceeds from Maturities', 'USD'),
            ('ProceedsFromSaleOfAvailableForSaleSecuritiesDebt', 'Proceeds from Sales of Securities', 'USD'),
            ('PaymentsToAcquirePropertyPlantAndEquipment', 'Capital Expenditures', 'USD'),
            ('PaymentsForProceedsFromOtherInvestingActivities', 'Other Investing Activities', 'USD'),
            ('NetCashProvidedByUsedInInvestingActivities', 'Investing Cash Flow', 'USD'),
            ('PaymentsRelatedToTaxWithholdingForShareBasedCompensation', 'Tax Withholding', 'USD'),
            ('PaymentsOfDividends', 'Dividends Paid', 'USD'),
            ('PaymentsForRepurchaseOfCommonStock', 'Stock Repurchases', 'USD'),
            ('ProceedsFromIssuanceOfLongTermDebt', 'Proceeds from Debt', 'USD'),
            ('RepaymentsOfLongTermDebt', 'Debt Repayments', 'USD'),
            ('ProceedsFromRepaymentsOfCommercialPaper', 'Change in Commercial Paper', 'USD'),
            ('ProceedsFromPaymentsForOtherFinancingActivities', 'Other Financing Activities', 'USD'),
            ('NetCashProvidedByUsedInFinancingActivities', 'Financing Cash Flow', 'USD'),
            ('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'Net Change in Cash', 'USD'),
        ]
    }
    
    total_inserted = 0
    
    for statement_type, statement_metrics in metrics.items():
        for metric_tag, metric_label, units in statement_metrics:
            all_data, found = extract_quarterly_data(company_data, metric_tag, units)
            
            if found and all_data:
                fiscal_years = {}
                
                for item in all_data:
                    fy = item.get('fy')
                    fp = item.get('fp', 'FY')
                    end_date = item.get('end')
                    value = item.get('val')
                    filed = item.get('filed')
                    
                    if not fy or not end_date:
                        continue
                    
                    if fy not in fiscal_years:
                        fiscal_years[fy] = {}
                    
                    key = fp if fp != 'FY' else 'FY'
                    
                    if key not in fiscal_years[fy] or filed > fiscal_years[fy][key].get('filed', ''):
                        fiscal_years[fy][key] = {
                            'value': value,
                            'end_date': end_date,
                            'filed': filed,
                            'fp': fp
                        }
                
                sorted_years = sorted(fiscal_years.keys(), reverse=True)
                quarters_shown = 0
                ytd_shown = False
                
                for fy in sorted_years:
                    if quarters_shown >= NUM_QUARTERS:
                        break
                    
                    year_data = fiscal_years[fy]
                    
                    # YTD para o ano mais recente
                    if not ytd_shown:
                        latest_q = None
                        for q in ['Q4', 'Q3', 'Q2', 'Q1']:
                            if q in year_data:
                                latest_q = q
                                break
                        
                        if latest_q:
                            q_data = year_data[latest_q]
                            insert_data(conn, company_name, cik, fy, latest_q, 
                                      q_data['end_date'], q_data['filed'], statement_type,
                                      metric_label, metric_tag, 'YTD', 
                                      format_value(q_data['value']), units)
                            total_inserted += 1
                            ytd_shown = True
                    
                    # Quarters individuais
                    for q in ['Q4', 'Q3', 'Q2', 'Q1']:
                        if quarters_shown >= NUM_QUARTERS:
                            break
                        
                        if q in year_data:
                            q_data = year_data[q]
                            quarterly_value = q_data['value']
                            
                            prev_q = {'Q4': 'Q3', 'Q3': 'Q2', 'Q2': 'Q1', 'Q1': None}[q]
                            if prev_q and prev_q in year_data:
                                prev_value = year_data[prev_q]['value']
                                if prev_value and quarterly_value:
                                    quarterly_value = quarterly_value - prev_value
                            
                            insert_data(conn, company_name, cik, fy, q,
                                      q_data['end_date'], q_data['filed'], statement_type,
                                      metric_label, metric_tag, 'Quarterly',
                                      format_value(quarterly_value), units)
                            total_inserted += 1
                            quarters_shown += 1
                    
                    # FY completo
                    if 'FY' in year_data and quarters_shown < NUM_QUARTERS:
                        fy_data = year_data['FY']
                        insert_data(conn, company_name, cik, fy, 'FY',
                                  fy_data['end_date'], fy_data['filed'], statement_type,
                                  metric_label, metric_tag, 'Full Year',
                                  format_value(fy_data['value']), units)
                        total_inserted += 1
    
    print(f"  ✓ {total_inserted} registos inseridos")
    return True

def main():
    # Lê CIKs do ficheiro
    try:
        with open('CIK.txt', 'r') as f:
            ciks = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Erro: Ficheiro CIK.txt não encontrado!")
        print("Cria um ficheiro CIK.txt com um CIK por linha (ex: 0000320193)")
        return
    
    if not ciks:
        print("Erro: Ficheiro CIK.txt está vazio!")
        return
    
    print(f"Encontrados {len(ciks)} CIK(s) para processar")
    
    # Cria base de dados
    db_name = f"sec_financial_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    conn = create_database(db_name)
    print(f"\nBase de dados criada: {db_name}")
    
    # Processa cada empresa
    successful = 0
    for cik in ciks:
        if process_company(cik, conn):
            successful += 1
    
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"PROCESSAMENTO CONCLUÍDO")
    print(f"{'='*80}")
    print(f"Base de dados: {db_name}")
    print(f"Empresas processadas: {successful}/{len(ciks)}")
    print(f"\nTabelas criadas:")
    print(f"  1. financial_statements (tabela principal com todos os dados)")
    print(f"  2. income_statement (apenas Income Statement - valores trimestrais)")
    print(f"  3. balance_sheet (apenas Balance Sheet - valores trimestrais)")
    print(f"  4. cash_flow_statement (apenas Cash Flow - valores trimestrais)")

if __name__ == "__main__":
    main()