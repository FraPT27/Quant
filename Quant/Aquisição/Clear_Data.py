import re
from datetime import datetime
import pandas as pd
import csv
import json
import os
import glob

def process_all_companies():
    """
    Processa automaticamente todas as empresas na pasta dataCIK
    """
    base_dir = "dataCIK"
    
    if not os.path.exists(base_dir):
        print(f"❌ Pasta {base_dir} não encontrada!")
        return []
    
    # Encontrar todas as pastas de empresas
    company_folders = [f for f in os.listdir(base_dir) 
                      if os.path.isdir(os.path.join(base_dir, f))]
    
    print(f"📁 Encontradas {len(company_folders)} empresas em {base_dir}")
    
    all_results = []
    
    for folder in company_folders:
        folder_path = os.path.join(base_dir, folder)
        print(f"\n{'='*60}")
        print(f"🔄 Processando: {folder}")
        print(f"{'='*60}")
        
        # Extrair informações da pasta
        folder_parts = folder.split('_')
        if len(folder_parts) >= 3:
            cik = folder_parts[0]
            ticker = folder_parts[1]
            company_name = '_'.join(folder_parts[2:])
        else:
            print(f"❌ Formato de pasta inválido: {folder}")
            continue
        
        # Processar todos os arquivos .txt na pasta
        txt_files = glob.glob(os.path.join(folder_path, "*.txt"))
        
        for file_path in txt_files:
            filename = os.path.basename(file_path)
            print(f"📄 Processando arquivo: {filename}")
            
            # Determinar tipo de arquivo e data
            file_info = parse_filename(filename, ticker)
            
            if file_info:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                        content = file.read()
                    
                    # Processar com parser genérico robusto
                    if file_info['file_type'] in ['10-Q', '10-K']:
                        result = parse_financial_statement_generic(content, file_info)
                        if result:
                            result['company_info'] = {
                                'cik': cik,
                                'ticker': ticker,
                                'company_name': company_name,
                                'source_file': filename
                            }
                            all_results.append(result)
                            
                            # Salvar resultados individuais
                            save_individual_results(result, folder_path, filename)
                            
                except Exception as e:
                    print(f"❌ Erro ao processar {filename}: {e}")
    
    # Salvar resultados consolidados
    if all_results:
        save_consolidated_results(all_results)
    
    return all_results

def parse_filename(filename, ticker):
    """
    Extrai informações do nome do arquivo
    """
    patterns = [
        rf"({ticker})_((10-Q|10-K))_(\d{{4}}-\d{{2}}-\d{{2}})\.txt",
        rf"({ticker})_((10Q|10K))_(\d{{4}}-\d{{2}}-\d{{2}})\.txt",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return {
                'ticker': match.group(1),
                'file_type': match.group(2),
                'filing_date': match.group(4)
            }
    
    print(f"⚠️  Formato de arquivo não reconhecido: {filename}")
    return None

def parse_financial_statement_generic(content, file_info):
    """
    Parser genérico robusto para qualquer empresa
    """
    ticker = file_info['ticker']
    file_type = file_info['file_type']
    filing_date = file_info['filing_date']
    
    print(f"🔍 Analisando {file_type} de {ticker} ({filing_date})")
    
    try:
        # Extrair período baseado na data do arquivo
        filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
        period_info = calculate_fiscal_period_generic(filing_date_obj, ticker)
        
        # Extrair dados financeiros com parser genérico
        income_data = extract_income_statement_generic(content)
        balance_data = extract_balance_sheet_generic(content)
        cash_flow_data = extract_cash_flow_generic(content)
        
        # Preparar dados consolidados
        financial_data = prepare_data_for_csv_generic({
            'period': period_info,
            'complete_income_statement': income_data,
            'complete_balance_sheet': balance_data,
            'complete_cash_flow': cash_flow_data,
            'ticker': ticker
        })
        
        return {
            'period': period_info,
            'complete_income_statement': income_data,
            'complete_balance_sheet': balance_data,
            'complete_cash_flow': cash_flow_data,
            'financial_data': financial_data,
            'file_info': file_info
        }
        
    except Exception as e:
        print(f"❌ Erro no parser genérico para {ticker}: {e}")
        return None

def calculate_fiscal_period_generic(filing_date, ticker):
    """
    Calcula período fiscal genérico baseado na data
    """
    year = filing_date.year
    quarter = (filing_date.month - 1) // 3 + 1
    
    # Para a maioria das empresas, ano fiscal é o ano civil
    fiscal_year = year
    
    return {
        'fiscal_year': str(fiscal_year),
        'fiscal_quarter': f'Q{quarter}',
        'calendar_year': str(year),
        'calendar_quarter': f'Q{quarter}',
        'reference_date': filing_date.strftime('%B %d, %Y'),
        'period_represented': f'Three Months Ended {filing_date.strftime("%B %d, %Y")}'
    }

def extract_income_statement_generic(content):
    """
    Extrai dados da demonstração de resultados de forma genérica
    """
    income_data = {}
    
    # Padrões para buscar números financeiros com diferentes formatos
    patterns = {
        'revenue': [
            r'Total revenue\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Revenue\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Net sales\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Sales.*?[\$]?\s*([\d,]+\.?\d*)'
        ],
        'gross_profit': [
            r'Gross profit\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Gross margin\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'operating_income': [
            r'Operating income\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Income from operations\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'net_income': [
            r'Net income\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Net earnings\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Net loss\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'cost_of_revenue': [
            r'Cost of revenue\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cost of sales\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cost of goods sold\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'research_development': [
            r'Research and development\s*[\$]?\s*([\d,]+\.?\d*)',
            r'R&D\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'selling_general_admin': [
            r'Selling, general and administrative\s*[\$]?\s*([\d,]+\.?\d*)',
            r'SG&A\s*[\$]?\s*([\d,]+\.?\d*)'
        ]
    }
    
    for field, pattern_list in patterns.items():
        for pattern in pattern_list:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            if matches:
                # Pegar o último valor (mais recente) se houver múltiplos
                value_str = matches[-1] if matches else "0"
                try:
                    # Remover vírgulas e converter para float
                    value = float(value_str.replace(',', ''))
                    # Se for um valor muito pequeno, provavelmente está em bilhões, multiplicar
                    if value < 1000 and value > 0:
                        value = value * 1000000  # Converter para milhões
                    income_data[field] = value
                    print(f"✅ {field}: {value:,.0f}")
                    break
                except ValueError:
                    continue
    
    # Calcular valores derivados se necessário
    if 'revenue' in income_data and 'cost_of_revenue' in income_data:
        income_data['gross_profit_calculated'] = income_data['revenue'] - income_data['cost_of_revenue']
    
    print(f"📊 Income Statement extraído: {len(income_data)} itens")
    return income_data

def extract_balance_sheet_generic(content):
    """
    Extrai dados do balanço patrimonial de forma genérica
    """
    balance_data = {}
    
    # Padrões para balanço patrimonial
    patterns = {
        'total_assets': [
            r'Total assets\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Total Assets\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'total_liabilities': [
            r'Total liabilities\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Total Liabilities\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'cash': [
            r'Cash and cash equivalents\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cash\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cash and equivalents\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'accounts_receivable': [
            r'Accounts receivable\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Receivables\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'inventory': [
            r'Inventory\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Inventories\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'property_plant_equipment': [
            r'Property, plant and equipment\s*[\$]?\s*([\d,]+\.?\d*)',
            r'PP&E\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'total_debt': [
            r'Total debt\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Debt\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'accounts_payable': [
            r'Accounts payable\s*[\$]?\s*([\d,]+\.?\d*)'
        ]
    }
    
    for field, pattern_list in patterns.items():
        for pattern in pattern_list:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            if matches:
                value_str = matches[-1] if matches else "0"
                try:
                    value = float(value_str.replace(',', ''))
                    if value < 1000 and value > 0:
                        value = value * 1000000
                    balance_data[field] = value
                    print(f"✅ {field}: {value:,.0f}")
                    break
                except ValueError:
                    continue
    
    # Calcular equity se tivermos assets e liabilities
    if 'total_assets' in balance_data and 'total_liabilities' in balance_data:
        balance_data['total_equity'] = balance_data['total_assets'] - balance_data['total_liabilities']
        print(f"✅ total_equity: {balance_data['total_equity']:,.0f}")
    
    print(f"📊 Balance Sheet extraído: {len(balance_data)} itens")
    return balance_data

def extract_cash_flow_generic(content):
    """
    Extrai dados do fluxo de caixa de forma genérica
    """
    cash_flow_data = {}
    
    # Padrões para fluxo de caixa
    patterns = {
        'operating_cash_flow': [
            r'Net cash provided by operating activities\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cash from operating activities\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Operating cash flow\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'investing_cash_flow': [
            r'Net cash used in investing activities\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cash from investing activities\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'financing_cash_flow': [
            r'Net cash used in financing activities\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Cash from financing activities\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'capital_expenditures': [
            r'Capital expenditures\s*[\$]?\s*([\d,]+\.?\d*)',
            r'CAPEX\s*[\$]?\s*([\d,]+\.?\d*)',
            r'Purchase of property and equipment\s*[\$]?\s*([\d,]+\.?\d*)'
        ],
        'free_cash_flow': [
            r'Free cash flow\s*[\$]?\s*([\d,]+\.?\d*)'
        ]
    }
    
    for field, pattern_list in patterns.items():
        for pattern in pattern_list:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            if matches:
                value_str = matches[-1] if matches else "0"
                try:
                    value = float(value_str.replace(',', ''))
                    if value < 1000 and value > 0:
                        value = value * 1000000
                    cash_flow_data[field] = value
                    print(f"✅ {field}: {value:,.0f}")
                    break
                except ValueError:
                    continue
    
    # Calcular free cash flow se não encontrado diretamente
    if 'free_cash_flow' not in cash_flow_data and 'operating_cash_flow' in cash_flow_data and 'capital_expenditures' in cash_flow_data:
        cash_flow_data['free_cash_flow_calculated'] = cash_flow_data['operating_cash_flow'] + cash_flow_data['capital_expenditures']
        print(f"✅ free_cash_flow_calculated: {cash_flow_data['free_cash_flow_calculated']:,.0f}")
    
    print(f"📊 Cash Flow extraído: {len(cash_flow_data)} itens")
    return cash_flow_data

def prepare_data_for_csv_generic(data):
    """Prepara dados genéricos para CSV"""
    income_data = data['complete_income_statement']
    balance_data = data['complete_balance_sheet']
    cash_flow_data = data['complete_cash_flow']
    ticker = data['ticker']
    
    # Usar valores calculados se os diretos não estiverem disponíveis
    revenue = income_data.get('revenue', 0)
    gross_profit = income_data.get('gross_profit', income_data.get('gross_profit_calculated', 0))
    net_income = income_data.get('net_income', 0)
    operating_income = income_data.get('operating_income', 0)
    
    total_assets = balance_data.get('total_assets', 0)
    total_liabilities = balance_data.get('total_liabilities', 0)
    total_equity = balance_data.get('total_equity', 0)
    total_debt = balance_data.get('total_debt', 0)
    
    operating_cash_flow = cash_flow_data.get('operating_cash_flow', 0)
    capital_expenditures = abs(cash_flow_data.get('capital_expenditures', 0))
    free_cash_flow = cash_flow_data.get('free_cash_flow', cash_flow_data.get('free_cash_flow_calculated', 0))
    
    fiscal_year = data['period'].get('fiscal_year', '2024')
    fiscal_quarter = data['period'].get('fiscal_quarter', 'Q1')
    
    csv_data = {
        'ticker': ticker,
        'year': fiscal_year,
        'quarter': fiscal_quarter,
        'sector': 'Technology',  # Placeholder - pode ser melhorado
        'revenue': revenue,
        'gross_profit': gross_profit,
        'ebitda': operating_income,  # Usando operating income como proxy para EBITDA
        'net_income': net_income,
        'total_assets': total_assets,
        'total_liabilities': total_liabilities,
        'total_debt': total_debt,
        'capex': capital_expenditures,
        'free_cash_flow': free_cash_flow,
        'operating_cash_flow': operating_cash_flow,
        'cogs': income_data.get('cost_of_revenue', 0),
        'sga_expense': income_data.get('selling_general_admin', 0),
        'rd_expense': income_data.get('research_development', 0),
        'operating_income': operating_income
    }
    
    print(f"📈 Dados preparados para CSV: Revenue ${revenue:,.0f}, Net Income ${net_income:,.0f}")
    return csv_data

def save_individual_results(result, folder_path, filename):
    """
    Salva resultados individuais para cada arquivo processado
    """
    try:
        ticker = result['file_info']['ticker']
        file_type = result['file_info']['file_type']
        filing_date = result['file_info']['filing_date']
        
        # Criar pasta de resultados se não existir
        results_dir = os.path.join(folder_path, "processed_results")
        os.makedirs(results_dir, exist_ok=True)
        
        base_name = f"{ticker}_{file_type}_{filing_date}"
        
        # Salvar dados financeiros em CSV
        if 'financial_data' in result:
            df = pd.DataFrame([result['financial_data']])
            csv_path = os.path.join(results_dir, f"{base_name}_financials.csv")
            df.to_csv(csv_path, index=False)
            print(f"💾 CSV salvo: {csv_path}")
        
        # Salvar dados completos em JSON
        complete_data = {
            'file_info': result['file_info'],
            'period': result['period'],
            'income_statement': result['complete_income_statement'],
            'balance_sheet': result['complete_balance_sheet'],
            'cash_flow': result['complete_cash_flow'],
            'financial_data': result['financial_data'],
            'processing_date': datetime.now().isoformat()
        }
        
        json_path = os.path.join(results_dir, f"{base_name}_complete.json")
        with open(json_path, 'w') as f:
            json.dump(complete_data, f, indent=2, default=str)
        
        print(f"💾 JSON completo salvo: {json_path}")
        
    except Exception as e:
        print(f"❌ Erro ao salvar resultados individuais: {e}")

def save_consolidated_results(all_results):
    """
    Salva todos os resultados consolidados
    """
    try:
        # Criar pasta consolidated se não existir
        consolidated_dir = "dataCIK/consolidated_results"
        os.makedirs(consolidated_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Consolidar dados financeiros
        financial_data_list = []
        for result in all_results:
            if 'financial_data' in result:
                financial_data = result['financial_data'].copy()
                financial_data['cik'] = result['company_info']['cik']
                financial_data['company_name'] = result['company_info']['company_name']
                financial_data['source_file'] = result['company_info']['source_file']
                financial_data['filing_date'] = result['file_info']['filing_date']
                financial_data['file_type'] = result['file_info']['file_type']
                financial_data_list.append(financial_data)
        
        if financial_data_list:
            # Salvar CSV consolidado
            consolidated_df = pd.DataFrame(financial_data_list)
            csv_path = os.path.join(consolidated_dir, f"all_companies_financials_{timestamp}.csv")
            consolidated_df.to_csv(csv_path, index=False)
            print(f"📊 CSV consolidado salvo: {csv_path}")
            
            # Salvar Excel com múltiplas abas
            excel_path = os.path.join(consolidated_dir, f"all_companies_financials_{timestamp}.xlsx")
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                consolidated_df.to_excel(writer, sheet_name='All Data', index=False)
                
                # Adicionar resumo por empresa
                summary_df = consolidated_df.groupby(['ticker', 'company_name']).agg({
                    'revenue': 'mean',
                    'net_income': 'mean',
                    'total_assets': 'mean'
                }).round(2)
                summary_df.to_excel(writer, sheet_name='Company Summary')
            
            print(f"📊 Excel consolidado salvo: {excel_path}")
        
        # Salvar dados completos consolidados em JSON
        complete_data_list = []
        for result in all_results:
            complete_data = {
                'company_info': result['company_info'],
                'file_info': result['file_info'],
                'period': result['period'],
                'income_statement': result['complete_income_statement'],
                'balance_sheet': result['complete_balance_sheet'],
                'cash_flow': result['complete_cash_flow'],
                'financial_data': result['financial_data'],
                'processing_date': datetime.now().isoformat()
            }
            complete_data_list.append(complete_data)
        
        complete_json_path = os.path.join(consolidated_dir, f"all_companies_complete_{timestamp}.json")
        with open(complete_json_path, 'w') as f:
            json.dump(complete_data_list, f, indent=2, default=str)
        
        print(f"📋 JSON completo consolidado salvo: {complete_json_path}")
        
        # Gerar relatório de processamento
        generate_processing_report(all_results, consolidated_dir, timestamp)
        
    except Exception as e:
        print(f"❌ Erro ao salvar resultados consolidados: {e}")

def generate_processing_report(all_results, output_dir, timestamp):
    """
    Gera um relatório de processamento
    """
    report = []
    report.append("=" * 70)
    report.append("RELATÓRIO DE PROCESSAMENTO - TODAS AS EMPRESAS")
    report.append("=" * 70)
    report.append(f"Data de geração: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total de arquivos processados: {len(all_results)}")
    report.append("")
    
    # Estatísticas por empresa
    companies = {}
    for result in all_results:
        ticker = result['company_info']['ticker']
        if ticker not in companies:
            companies[ticker] = []
        companies[ticker].append(result)
    
    report.append("ESTATÍSTICAS POR EMPRESA:")
    report.append("-" * 50)
    for ticker, results in companies.items():
        company_name = results[0]['company_info']['company_name']
        report.append(f"{ticker} ({company_name}): {len(results)} arquivos")
        
        # Mostrar dados financeiros da primeira entrada
        if results and 'financial_data' in results[0]:
            financial_data = results[0]['financial_data']
            report.append(f"  Revenue: ${financial_data.get('revenue', 0):,.0f}")
            report.append(f"  Net Income: ${financial_data.get('net_income', 0):,.0f}")
            report.append(f"  Assets: ${financial_data.get('total_assets', 0):,.0f}")
        
        # Detalhes por tipo de arquivo
        file_types = {}
        for result in results:
            file_type = result['file_info']['file_type']
            file_types[file_type] = file_types.get(file_type, 0) + 1
        
        for file_type, count in file_types.items():
            report.append(f"  └─ {file_type}: {count} arquivos")
    
    report.append("")
    report.append("ARQUIVOS GERADOS:")
    report.append("-" * 30)
    report.append(f"• CSV consolidado: all_companies_financials_{timestamp}.csv")
    report.append(f"• Excel consolidado: all_companies_financials_{timestamp}.xlsx")
    report.append(f"• JSON completo: all_companies_complete_{timestamp}.json")
    report.append(f"• Este relatório: processing_report_{timestamp}.txt")
    
    # Salvar relatório
    report_path = os.path.join(output_dir, f"processing_report_{timestamp}.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print(f"📋 Relatório de processamento salvo: {report_path}")
    
    # Imprimir relatório no console
    print('\n'.join(report))

# Execução principal
if __name__ == "__main__":
    print("🚀 INICIANDO PROCESSAMENTO DE TODAS AS EMPRESAS")
    print("=" * 60)
    
    results = process_all_companies()
    
    print(f"\n{'='*60}")
    print("✅ PROCESSAMENTO CONCLUÍDO!")
    print(f"{'='*60}")
    print(f"📈 Total de arquivos processados: {len(results)}")
    print(f"💾 Resultados salvos em: dataCIK/consolidated_results/")
    print(f"📂 Resultados individuais salvos nas pastas de cada empresa")