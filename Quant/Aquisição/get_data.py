import yfinance as yf
import pandas as pd
import time
import os
import sqlite3
import logging
from datetime import datetime, timedelta

# --- Configuração ---
USE_FILE_TICKERS = True  # Mudar para True para ler do ficheiro
TICKERS = ['AAPL', 'MSFT', 'AMZN', 'TSLA', 'GOOGL', 'META', 'NVDA', 'JPM', 'JNJ', 'V', 'ASML']  # Fallback
DATABASE_NAME = "financial_data_new.db"
OUTPUT_CSV = "financial_data.csv"
BATCH_SIZE = 50
FORCE_REFRESH = False
FORCE_REFRESH_DAYS = 30
# --------------------

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database():
    """Cria a base de dados SQLite e as tabelas necessárias."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Tabela principal de dados financeiros (resumida) - MANTIDA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                year INTEGER NOT NULL,
                sector TEXT,
                revenue REAL,
                gross_profit REAL,
                ebitda REAL,
                net_income REAL,
                total_assets REAL,
                total_liabilities REAL,
                total_debt REAL,
                capex REAL,
                free_cash_flow REAL,
                operating_cash_flow REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, year)
            )
        ''')
        
        # Tabela: Income Statement detalhado - MANTIDA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS income_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                year INTEGER NOT NULL,
                revenue REAL,
                cogs REAL,
                gross_profit REAL,
                sga_expense REAL,
                rd_expense REAL,
                operating_expenses REAL,
                operating_income REAL,
                other_income REAL,
                pretax_income REAL,
                income_taxes REAL,
                net_income REAL,
                gross_profit_margin REAL,
                operating_profit_margin REAL,
                net_profit_margin REAL,
                rd_percent_revenue REAL,
                sga_percent_revenue REAL,
                revenue_growth REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, year)
            )
        ''')
        
        # NOVA TABELA: Balance Sheet dinâmica (como no exemplo)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS balance_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                year INTEGER NOT NULL,
                data_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, date)
            )
        ''')
        
        # NOVA TABELA: Cash Flow Statement dinâmica (como no exemplo)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cashflow_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                year INTEGER NOT NULL,
                data_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, date)
            )
        ''')
        
        # Tabela para outlook/metadados das empresas - MANTIDA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_outlook (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT UNIQUE NOT NULL,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                country TEXT,
                market_cap REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                dividend_yield REAL,
                beta REAL,
                fifty_two_week_high REAL,
                fifty_two_week_low REAL,
                analyst_target_price REAL,
                recommendation TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela para controlo de processamento - MANTIDA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                year INTEGER NOT NULL,
                current_ratio REAL, quick_ratio REAL, cash_ratio REAL,
                debt_to_equity REAL, debt_to_assets REAL, equity_multiplier REAL,
                roe REAL, roa REAL, return_on_tangible_equity REAL,
                gross_margin REAL, operating_margin REAL, net_margin REAL,
                asset_turnover REAL, inventory_turnover REAL, receivables_turnover REAL,
                operating_cash_flow_ratio REAL, free_cash_flow_margin REAL,
                working_capital REAL, net_debt REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, year)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Base de dados criada com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao criar base de dados: {e}")

def get_db_connection():
    """Retorna uma conexão com a base de dados."""
    return sqlite3.connect(DATABASE_NAME)

def get_successfully_processed_tickers():
    """Retorna um set com os tickers que foram processados com sucesso recentemente."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Buscar tickers com status SUCCESS nos últimos FORCE_REFRESH_DAYS
    cutoff_date = datetime.now() - timedelta(days=FORCE_REFRESH_DAYS)
    
    cursor.execute('''
        SELECT DISTINCT ticker 
        FROM processing_log 
        WHERE status = 'SUCCESS' 
        AND processed_at >= ?
    ''', (cutoff_date.strftime('%Y-%m-%d %H:%M:%S'),))
    
    successful_tickers = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    logger.info(f"Encontrados {len(successful_tickers)} tickers processados com sucesso recentemente")
    return successful_tickers

def get_tickers_to_process(requested_tickers):
    """
    Filtra a lista de tickers solicitados, removendo os que já foram processados com sucesso.
    """
    if FORCE_REFRESH:
        logger.info("Forçando refresh de todos os tickers")
        return requested_tickers
    
    successful_tickers = get_successfully_processed_tickers()
    tickers_to_process = [ticker for ticker in requested_tickers if ticker not in successful_tickers]
    
    logger.info(f"Tickers solicitados: {len(requested_tickers)}")
    logger.info(f"Tickers já processados: {len(successful_tickers)}")
    logger.info(f"Tickers a processar: {len(tickers_to_process)}")
    
    if len(tickers_to_process) < len(requested_tickers):
        skipped_tickers = set(requested_tickers) - set(tickers_to_process)
        logger.info(f"Tickers skipados (já processados): {skipped_tickers}")
    
    return tickers_to_process

def save_financial_data_directly(df, table_name='financial_statements'):
    """Salva dados financeiros diretamente com controle de duplicatas."""
    if df.empty:
        logger.warning("DataFrame vazio, nada para salvar")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    updated_count = 0
    
    for _, row in df.iterrows():
        try:
            # Verificar se já existe
            cursor.execute(
                f'SELECT 1 FROM {table_name} WHERE ticker = ? AND year = ?',
                (row['ticker'], row['year'])
            )
            
            if cursor.fetchone():
                # Update apenas se FORCE_REFRESH
                if FORCE_REFRESH:
                    set_clause = ', '.join([f"{col} = ?" for col in df.columns if col not in ['ticker', 'year']])
                    values = [row[col] for col in df.columns if col not in ['ticker', 'year']]
                    values.extend([row['ticker'], row['year']])
                    
                    cursor.execute(
                        f'UPDATE {table_name} SET {set_clause} WHERE ticker = ? AND year = ?',
                        values
                    )
                    updated_count += 1
            else:
                # Insert
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['?' for _ in df.columns])
                values = [row[col] for col in df.columns]
                
                cursor.execute(
                    f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})',
                    values
                )
                inserted_count += 1
                
        except Exception as e:
            logger.error(f"Erro ao salvar dados para {row['ticker']} {row['year']}: {e}")
            continue
    
    conn.commit()
    conn.close()
    logger.info(f"Financial data saved: {inserted_count} inserted, {updated_count} updated")

def save_income_statements_directly(income_statements_list):
    """Salva income statements detalhados."""
    if not income_statements_list:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    updated_count = 0
    
    for income_data in income_statements_list:
        try:
            # Verificar se já existe
            cursor.execute(
                'SELECT 1 FROM income_statements WHERE ticker = ? AND year = ?',
                (income_data['ticker'], income_data['year'])
            )
            
            if cursor.fetchone():
                # Update apenas se FORCE_REFRESH
                if FORCE_REFRESH:
                    set_clause = ', '.join([f"{key} = ?" for key in income_data.keys() if key not in ['ticker', 'year']])
                    values = [income_data[key] for key in income_data.keys() if key not in ['ticker', 'year']]
                    values.extend([income_data['ticker'], income_data['year']])
                    
                    cursor.execute(
                        f'UPDATE income_statements SET {set_clause} WHERE ticker = ? AND year = ?',
                        values
                    )
                    updated_count += 1
            else:
                # Insert
                columns = ', '.join(income_data.keys())
                placeholders = ', '.join(['?' for _ in income_data])
                values = list(income_data.values())
                
                cursor.execute(
                    f'INSERT INTO income_statements ({columns}) VALUES ({placeholders})',
                    values
                )
                inserted_count += 1
                
        except Exception as e:
            logger.error(f"Erro ao salvar income statement para {income_data.get('ticker', 'Unknown')} {income_data.get('year', 'Unknown')}: {e}")
            continue
    
    conn.commit()
    conn.close()
    logger.info(f"Income statements saved: {inserted_count} inserted, {updated_count} updated")

def save_balance_sheet_dynamic(ticker_symbol):
    """Salva balance sheet completo de forma dinâmica (como no exemplo)."""
    try:
        stock = yf.Ticker(ticker_symbol)
        bs = stock.balance_sheet
        
        if bs is None or bs.empty:
            logger.warning(f"Sem dados de balance sheet para {ticker_symbol}")
            return False
        
        # Transpor para ter anos como linhas
        bs_transposed = bs.transpose()
        
        conn = get_db_connection()
        
        # Guardar o DataFrame completo
        for date_index, row in bs_transposed.iterrows():
            try:
                date_str = date_index.strftime('%Y-%m-%d')
                year = date_index.year
                
                # Converter a linha para JSON
                data_dict = row.to_dict()
                # Remover valores NaN para JSON
                data_dict = {k: (v if pd.notna(v) else None) for k, v in data_dict.items()}
                import json
                data_json = json.dumps(data_dict)
                
                # Verificar se já existe
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT 1 FROM balance_sheets WHERE ticker = ? AND date = ?',
                    (ticker_symbol, date_str)
                )
                
                if cursor.fetchone():
                    # Update apenas se FORCE_REFRESH
                    if FORCE_REFRESH:
                        cursor.execute(
                            'UPDATE balance_sheets SET data_json = ? WHERE ticker = ? AND date = ?',
                            (data_json, ticker_symbol, date_str)
                        )
                else:
                    # Insert
                    cursor.execute(
                        'INSERT INTO balance_sheets (ticker, date, year, data_json) VALUES (?, ?, ?, ?)',
                        (ticker_symbol, date_str, year, data_json)
                    )
                
                conn.commit()
                
            except Exception as e:
                logger.error(f"Erro ao salvar balance sheet para {ticker_symbol} {date_str}: {e}")
                continue
        
        conn.close()
        logger.info(f"Balance sheet dinâmico guardado para {ticker_symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar balance sheet para {ticker_symbol}: {e}")
        return False

def save_cashflow_statement_dynamic(ticker_symbol):
    """Salva cash flow statement completo de forma dinâmica (como no exemplo)."""
    try:
        stock = yf.Ticker(ticker_symbol)
        cf = stock.cashflow
        
        if cf is None or cf.empty:
            logger.warning(f"Sem dados de cash flow para {ticker_symbol}")
            return False
        
        # Transpor para ter anos como linhas
        cf_transposed = cf.transpose()
        
        conn = get_db_connection()
        
        # Guardar o DataFrame completo
        for date_index, row in cf_transposed.iterrows():
            try:
                date_str = date_index.strftime('%Y-%m-%d')
                year = date_index.year
                
                # Converter a linha para JSON
                data_dict = row.to_dict()
                # Remover valores NaN para JSON
                data_dict = {k: (v if pd.notna(v) else None) for k, v in data_dict.items()}
                import json
                data_json = json.dumps(data_dict)
                
                # Verificar se já existe
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT 1 FROM cashflow_statements WHERE ticker = ? AND date = ?',
                    (ticker_symbol, date_str)
                )
                
                if cursor.fetchone():
                    # Update apenas se FORCE_REFRESH
                    if FORCE_REFRESH:
                        cursor.execute(
                            'UPDATE cashflow_statements SET data_json = ? WHERE ticker = ? AND date = ?',
                            (data_json, ticker_symbol, date_str)
                        )
                else:
                    # Insert
                    cursor.execute(
                        'INSERT INTO cashflow_statements (ticker, date, year, data_json) VALUES (?, ?, ?, ?)',
                        (ticker_symbol, date_str, year, data_json)
                    )
                
                conn.commit()
                
            except Exception as e:
                logger.error(f"Erro ao salvar cash flow para {ticker_symbol} {date_str}: {e}")
                continue
        
        conn.close()
        logger.info(f"Cash flow statement dinâmico guardado para {ticker_symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar cash flow para {ticker_symbol}: {e}")
        return False

def save_outlook_data_directly(outlook_data_list):
    """Salva dados de outlook diretamente com controle de duplicatas."""
    if not outlook_data_list:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for outlook_data in outlook_data_list:
        try:
            # Verificar se já existe
            cursor.execute(
                'SELECT 1 FROM company_outlook WHERE ticker = ?',
                (outlook_data['ticker'],)
            )
            
            if cursor.fetchone():
                # Update apenas se FORCE_REFRESH
                if FORCE_REFRESH:
                    set_clause = ', '.join([f"{key} = ?" for key in outlook_data.keys() if key != 'ticker'])
                    values = [outlook_data[key] for key in outlook_data.keys() if key != 'ticker']
                    values.append(outlook_data['ticker'])
                    
                    cursor.execute(
                        f'UPDATE company_outlook SET {set_clause} WHERE ticker = ?',
                        values
                    )
            else:
                # Insert
                columns = ', '.join(outlook_data.keys())
                placeholders = ', '.join(['?' for _ in outlook_data])
                values = list(outlook_data.values())
                
                cursor.execute(
                    f'INSERT INTO company_outlook ({columns}) VALUES ({placeholders})',
                    values
                )
                
        except Exception as e:
            logger.error(f"Erro ao salvar outlook para {outlook_data.get('ticker', 'Unknown')}: {e}")
            continue
    
    conn.commit()
    conn.close()
    logger.info(f"Outlook data saved for {len(outlook_data_list)} companies")

def log_processing_status(ticker, status, error_message=None):
    """Regista o status do processamento de cada ticker."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO processing_log (ticker, status, error_message)
        VALUES (?, ?, ?)
    ''', (ticker, status, error_message))
    conn.commit()
    conn.close()

def fetch_company_outlook(ticker):
    """Obtém informações de outlook e metadados da empresa."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        outlook_data = {
            'ticker': ticker,
            'company_name': info.get('longName', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'country': info.get('country', 'N/A'),
            'market_cap': info.get('marketCap'),
            'pe_ratio': info.get('trailingPE'),
            'pb_ratio': info.get('priceToBook'),
            'dividend_yield': info.get('dividendYield'),
            'beta': info.get('beta'),
            'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
            'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
            'analyst_target_price': info.get('targetMeanPrice'),
            'recommendation': info.get('recommendationKey', 'N/A')
        }
        
        return outlook_data
        
    except Exception as e:
        logger.error(f"Erro ao obter outlook para {ticker}: {e}")
        return None

def calculate_financial_ratios(income_data, previous_year_data=None):
    """Calcula ratios financeiros baseados nos dados do income statement."""
    try:
        revenue = income_data.get('revenue', 0)
        if revenue == 0:
            return income_data
            
        # Calcular margins
        gross_profit = income_data.get('gross_profit', 0)
        operating_income = income_data.get('operating_income', 0)
        net_income = income_data.get('net_income', 0)
        rd_expense = income_data.get('rd_expense', 0)
        sga_expense = income_data.get('sga_expense', 0)
        
        income_data['gross_profit_margin'] = (gross_profit / revenue * 100) if revenue > 0 else 0
        income_data['operating_profit_margin'] = (operating_income / revenue * 100) if revenue > 0 else 0
        income_data['net_profit_margin'] = (net_income / revenue * 100) if revenue > 0 else 0
        income_data['rd_percent_revenue'] = (rd_expense / revenue * 100) if revenue > 0 else 0
        income_data['sga_percent_revenue'] = (sga_expense / revenue * 100) if revenue > 0 else 0
        
        # Calcular crescimento de revenue
        if previous_year_data and previous_year_data.get('revenue', 0) > 0:
            prev_revenue = previous_year_data['revenue']
            income_data['revenue_growth'] = ((revenue - prev_revenue) / prev_revenue * 100)
        else:
            income_data['revenue_growth'] = 0
            
    except Exception as e:
        logger.error(f"Erro ao calcular ratios: {e}")
        income_data.update({
            'gross_profit_margin': 0,
            'operating_profit_margin': 0,
            'net_profit_margin': 0,
            'rd_percent_revenue': 0,
            'sga_percent_revenue': 0,
            'revenue_growth': 0
        })
    
    return income_data

def extract_detailed_income_data(stock, ticker_symbol):
    """Extrai dados detalhados do income statement."""
    try:
        income_stmt = stock.income_stmt
        if income_stmt is None or income_stmt.empty:
            return []
        
        income_data_list = []
        previous_year_data = None
        
        # Iterar por cada ano disponível
        for year in income_stmt.columns:
            try:
                year_data = {}
                year_int = year.year
                
                # Extrair dados básicos do income statement
                year_data['ticker'] = ticker_symbol
                year_data['year'] = year_int
                
                # Mapear colunas do yfinance para os nossos nomes
                column_mapping = {
                    'Total Revenue': 'revenue',
                    'Cost Of Revenue': 'cogs',
                    'Gross Profit': 'gross_profit',
                    'Selling General And Administration': 'sga_expense',
                    'Research And Development': 'rd_expense',
                    'Operating Income': 'operating_income',
                    'Other Income Expense': 'other_income',
                    'Pretax Income': 'pretax_income',
                    'Tax Provision': 'income_taxes',
                    'Net Income': 'net_income'
                }
                
                # Procurar por diferentes nomes possíveis das colunas
                for yf_col, our_col in column_mapping.items():
                    if yf_col in income_stmt.index:
                        year_data[our_col] = income_stmt.loc[yf_col, year]
                    else:
                        possible_cols = [col for col in income_stmt.index if yf_col.lower() in col.lower()]
                        if possible_cols:
                            year_data[our_col] = income_stmt.loc[possible_cols[0], year]
                        else:
                            year_data[our_col] = 0
                
                # Calcular operating expenses se não encontrado
                if 'operating_expenses' not in year_data:
                    year_data['operating_expenses'] = year_data.get('sga_expense', 0) + year_data.get('rd_expense', 0)
                
                # Calcular ratios
                year_data = calculate_financial_ratios(year_data, previous_year_data)
                
                # Preencher valores missing
                for key in year_data:
                    if year_data[key] is None:
                        year_data[key] = 0
                
                income_data_list.append(year_data)
                previous_year_data = year_data.copy()
                
            except Exception as e:
                logger.error(f"Erro ao processar ano {year} para {ticker_symbol}: {e}")
                continue
                
        return income_data_list
        
    except Exception as e:
        logger.error(f"Erro ao extrair income data para {ticker_symbol}: {e}")
        return []

def fetch_financial_data_batch(tickers):
    """
    Vai buscar dados financeiros para uma lista de tickers em lotes.
    """
    all_financial_data = []
    all_income_statements = []
    all_outlook_data = []
    
    logger.info(f"Iniciando download de dados para {len(tickers)} tickers...")
    
    for i, ticker_symbol in enumerate(tickers):
        try:
            logger.info(f"Processando [{i+1}/{len(tickers)}]: {ticker_symbol}")
            
            stock = yf.Ticker(ticker_symbol)
            
            # Obter dados financeiros básicos
            income = stock.income_stmt
            balance = stock.balance_sheet
            cashflow = stock.cashflow
            
            if income is None or income.empty:
                logger.warning(f"Sem dados de income statement para {ticker_symbol}")
                log_processing_status(ticker_symbol, "ERROR", "Sem dados de income statement")
                continue
            
            # Combinar os relatórios para dados básicos
            df = pd.concat([
                income.transpose(), 
                balance.transpose(), 
                cashflow.transpose()
            ], axis=1)
            
            # Adicionar colunas de identificação
            df['ticker'] = ticker_symbol
            df['year'] = df.index.year
            df['sector'] = stock.info.get('sector', 'N/A')
            
            # Resetar índice
            df = df.reset_index(drop=True)
            all_financial_data.append(df)
            
            # Extrair income statements detalhados
            income_statements = extract_detailed_income_data(stock, ticker_symbol)
            all_income_statements.extend(income_statements)
            
            # Salvar balance sheet dinâmico
            save_balance_sheet_dynamic(ticker_symbol)
            
            # Salvar cash flow statement dinâmico
            save_cashflow_statement_dynamic(ticker_symbol)
            
            # Obter dados de outlook
            outlook_data = fetch_company_outlook(ticker_symbol)
            if outlook_data:
                all_outlook_data.append(outlook_data)
            
            log_processing_status(ticker_symbol, "SUCCESS")
            
            # Pausa para evitar rate limiting
            if (i + 1) % BATCH_SIZE == 0:
                logger.info(f"Pausa após processar {i + 1} empresas...")
                time.sleep(2)
            else:
                time.sleep(0.5)
                
        except Exception as e:
            error_msg = f"Erro ao processar {ticker_symbol}: {str(e)}"
            logger.error(error_msg)
            log_processing_status(ticker_symbol, "ERROR", error_msg)
            continue
    
    return all_financial_data, all_income_statements, all_outlook_data

def clean_financial_data(df_list):
    """
    Limpa e normaliza os dados financeiros básicos.
    """
    if not df_list:
        return pd.DataFrame()
    
    # Combinar todos os DataFrames
    final_df = pd.concat(df_list, ignore_index=True)
    
    # Mapeamento de colunas
    column_map = {
        'Total Revenue': 'revenue',
        'Gross Profit': 'gross_profit',
        'EBITDA': 'ebitda',
        'Net Income': 'net_income',
        'Total Assets': 'total_assets',
        'Total Liabilities Net Minority Interest': 'total_liabilities',
        'Total Debt': 'total_debt',
        'Capital Expenditures': 'capex',
        'Free Cash Flow': 'free_cash_flow',
        'Operating Cash Flow': 'operating_cash_flow'
    }
    
    # Renomear colunas existentes
    for old_name, new_name in column_map.items():
        if old_name in final_df.columns:
            final_df[new_name] = final_df[old_name]
    
    # Selecionar e ordenar colunas
    base_columns = ['ticker', 'year', 'sector']
    data_columns = [col for col in column_map.values() if col in final_df.columns]
    
    final_columns = base_columns + data_columns
    final_df = final_df[final_columns]
    
    # Preencher valores missing
    final_df = final_df.fillna(0)
    
    return final_df

def load_tickers_from_file(file_path):
    """Carrega lista de tickers de um ficheiro."""
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Ficheiro {file_path} não encontrado. A usar lista padrão.")
            return []
            
        with open(file_path, 'r') as file:
            tickers = []
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):  # Ignorar linhas vazias e comentários
                    # Extrair o ticker (ignorar tudo após espaço ou tab)
                    ticker = line.split()[0] if line.split() else line
                    tickers.append(ticker.upper())
        
        logger.info(f"Carregados {len(tickers)} tickers do ficheiro {file_path}")
        return tickers
        
    except Exception as e:
        logger.error(f"Erro ao carregar tickers do ficheiro {file_path}: {e}")
        return []

def get_processing_stats():
    """Retorna estatísticas do processamento."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT status, COUNT(*) as count 
        FROM processing_log 
        GROUP BY status
    ''')
    
    stats = cursor.fetchall()
    conn.close()
    
    return dict(stats)

# --- Funções para consultar a base de dados ---

def query_financial_data(tickers=None, years=None, sectors=None):
    """Consulta dados financeiros da base de dados."""
    conn = get_db_connection()
    
    query = "SELECT * FROM financial_statements WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    if years:
        placeholders = ','.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    if sectors:
        placeholders = ','.join(['?' for _ in sectors])
        query += f" AND sector IN ({placeholders})"
        params.extend(sectors)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def query_income_statements(tickers=None, years=None):
    """Consulta income statements detalhados."""
    conn = get_db_connection()
    
    query = "SELECT * FROM income_statements WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    if years:
        placeholders = ','.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def query_balance_sheets(tickers=None, years=None):
    """Consulta balance sheets dinâmicos."""
    conn = get_db_connection()
    
    query = "SELECT * FROM balance_sheets WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    if years:
        placeholders = ','.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def query_cashflow_statements(tickers=None, years=None):
    """Consulta cash flow statements dinâmicos."""
    conn = get_db_connection()
    
    query = "SELECT * FROM cashflow_statements WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    if years:
        placeholders = ','.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def query_company_outlook(tickers=None):
    """Consulta dados de outlook das empresas."""
    conn = get_db_connection()
    
    query = "SELECT * FROM company_outlook WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def get_balance_sheet_fields(ticker, year=None):
    """Retorna todos os campos disponíveis no balance sheet para um ticker."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if year:
        cursor.execute('SELECT data_json FROM balance_sheets WHERE ticker = ? AND year = ?', (ticker, year))
    else:
        cursor.execute('SELECT data_json FROM balance_sheets WHERE ticker = ? LIMIT 1', (ticker,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        import json
        data = json.loads(result[0])
        return list(data.keys())
    
    return []

def get_cashflow_fields(ticker, year=None):
    """Retorna todos os campos disponíveis no cash flow para um ticker."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if year:
        cursor.execute('SELECT data_json FROM cashflow_statements WHERE ticker = ? AND year = ?', (ticker, year))
    else:
        cursor.execute('SELECT data_json FROM cashflow_statements WHERE ticker = ? LIMIT 1', (ticker,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        import json
        data = json.loads(result[0])
        return list(data.keys())
    
    return []

# --- Ponto de Entrada Principal ---

def calculate_and_save_ratios(ticker, year):
    """Calcula e guarda os ratios para um ticker e ano específicos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Buscar dados do balance sheet
        cursor.execute('''
            SELECT data_json FROM balance_sheets 
            WHERE ticker = ? AND year = ?
            ORDER BY date DESC LIMIT 1
        ''', (ticker, year))
        
        balance_result = cursor.fetchone()
        if not balance_result:
            logger.warning(f"Sem dados de balance sheet para {ticker} {year}")
            return False
        
        # Buscar dados financeiros
        cursor.execute('''
            SELECT revenue, gross_profit, ebitda, net_income, 
                   operating_cash_flow, free_cash_flow
            FROM financial_statements 
            WHERE ticker = ? AND year = ?
        ''', (ticker, year))
        
        financial_result = cursor.fetchone()
        if not financial_result:
            logger.warning(f"Sem dados financeiros para {ticker} {year}")
            return False
        
        revenue, gross_profit, ebitda, net_income, operating_cash_flow, free_cash_flow = financial_result
        
        # Parse do JSON do balance sheet
        import json
        balance_data = json.loads(balance_result[0])
        
        # Extrair valores do balance sheet com fallbacks
        current_assets = balance_data.get('Current Assets', 0)
        current_liabilities = balance_data.get('Current Liabilities', 0)
        cash = balance_data.get('Cash And Cash Equivalents', 0)
        inventory = balance_data.get('Inventory', 0)
        receivables = balance_data.get('Receivables', 0)
        total_assets = balance_data.get('Total Assets', 0)
        total_equity = balance_data.get('Total Equity Gross Minority Interest', 0)
        tangible_book_value = balance_data.get('Tangible Book Value', total_equity)
        total_debt = balance_data.get('Total Debt', 0)
        working_capital = balance_data.get('Working Capital', 0)
        net_debt = balance_data.get('Net Debt', 0)
        
        # Calcular ratios
        ratios = {
            'ticker': ticker,
            'year': year,
            # Liquidez
            'current_ratio': current_assets / current_liabilities if current_liabilities else 0,
            'quick_ratio': (current_assets - inventory) / current_liabilities if current_liabilities else 0,
            'cash_ratio': cash / current_liabilities if current_liabilities else 0,
            
            # Solvabilidade
            'debt_to_equity': total_debt / total_equity if total_equity else 0,
            'debt_to_assets': total_debt / total_assets if total_assets else 0,
            'equity_multiplier': total_assets / total_equity if total_equity else 0,
            
            # Rentabilidade
            'roe': net_income / total_equity if total_equity else 0,
            'roa': net_income / total_assets if total_assets else 0,
            'return_on_tangible_equity': net_income / tangible_book_value if tangible_book_value else 0,
            'gross_margin': (gross_profit / revenue * 100) if revenue else 0,
            'operating_margin': (ebitda / revenue * 100) if revenue else 0,
            'net_margin': (net_income / revenue * 100) if revenue else 0,
            
            # Eficiência
            'asset_turnover': revenue / total_assets if total_assets else 0,
            'inventory_turnover': revenue / inventory if inventory else 0,
            'receivables_turnover': revenue / receivables if receivables else 0,
            
            # Cash Flow
            'operating_cash_flow_ratio': operating_cash_flow / current_liabilities if current_liabilities else 0,
            'free_cash_flow_margin': (free_cash_flow / revenue * 100) if revenue else 0,
            
            # Outros
            'working_capital': working_capital,
            'net_debt': net_debt
        }
        
        # Inserir ou atualizar na tabela ratios
        columns = ', '.join(ratios.keys())
        placeholders = ', '.join(['?' for _ in ratios])
        values = list(ratios.values())
        
        cursor.execute(f'''
            INSERT OR REPLACE INTO ratios ({columns}) 
            VALUES ({placeholders})
        ''', values)
        
        conn.commit()
        conn.close()
        
        logger.info(f"Ratios calculados e guardados para {ticker} {year}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao calcular ratios para {ticker} {year}: {e}")
        return False

def calculate_ratios_for_all_data():
    """Calcula ratios para todos os dados existentes na base de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Buscar todos os tickers e anos com dados financeiros
        cursor.execute('SELECT DISTINCT ticker, year FROM financial_statements')
        ticker_years = cursor.fetchall()
        
        success_count = 0
        for ticker, year in ticker_years:
            if calculate_and_save_ratios(ticker, year):
                success_count += 1
        
        logger.info(f"Ratios calculados para {success_count} de {len(ticker_years)} combinações ticker/ano")
        return success_count
        
    except Exception as e:
        logger.error(f"Erro ao calcular ratios para dados existentes: {e}")
        return 0
    finally:
        conn.close()

def calculate_ratios_for_existing_data():
    """Calcula ratios para todos os dados existentes na base de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Buscar todos os tickers e anos com dados financeiros
        cursor.execute('SELECT DISTINCT ticker, year FROM financial_statements')
        ticker_years = cursor.fetchall()
        
        for ticker, year in ticker_years:
            # Inserir um registo na tabela ratios (o trigger vai calcular automaticamente)
            cursor.execute('''
                INSERT OR IGNORE INTO ratios (ticker, year) 
                VALUES (?, ?)
            ''', (ticker, year))
        
        conn.commit()
        logger.info(f"Ratios calculados para {len(ticker_years)} combinações ticker/ano existentes")
        
    except Exception as e:
        logger.error(f"Erro ao calcular ratios para dados existentes: {e}")
    finally:
        conn.close()

def query_ratios(tickers=None, years=None):
    """Consulta ratios da base de dados."""
    conn = get_db_connection()
    
    query = "SELECT * FROM ratios WHERE 1=1"
    params = []
    
    if tickers:
        placeholders = ','.join(['?' for _ in tickers])
        query += f" AND ticker IN ({placeholders})"
        params.extend(tickers)
    
    if years:
        placeholders = ','.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    query += " ORDER BY ticker, year"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

if __name__ == "__main__":
    # Criar base de dados (já inclui a tabela ratios)
    create_database()
    
    # Opções de configuração
    USE_FILE_TICKERS = True
    
    if USE_FILE_TICKERS:
        tickers_requested = load_tickers_from_file("Tickers.txt")
        if not tickers_requested:
            logger.warning("Não foi possível carregar tickers do ficheiro. A usar lista padrão.")
            tickers_requested = TICKERS
    else:
        tickers_requested = TICKERS
    
    logger.info(f"Tickers a processar: {tickers_requested}")
    
    # Filtrar tickers para processar apenas os necessários
    tickers_to_process = get_tickers_to_process(tickers_requested)
    
    if not tickers_to_process:
        logger.info("Todos os tickers já foram processados com sucesso. Nada a fazer.")
        
        # Calcular ratios para dados existentes
        logger.info("A calcular ratios para dados existentes...")
        calculate_ratios_for_all_data()
        
        stats = get_processing_stats()
        logger.info(f"Estatísticas atuais: {stats}")
        
        # Mostrar exemplo de consulta
        print("\n--- Exemplo de consulta (dados existentes) ---")
        ratios_data = query_ratios(tickers=['AAPL', 'ASML'], years=[2023])
        print("Ratios disponíveis:")
        print(ratios_data.head())
        exit(0)
    
    logger.info(f"Processando {len(tickers_to_process)} tickers")
    
    # Obter dados
    financial_data_list, income_statements_list, outlook_data_list = fetch_financial_data_batch(tickers_to_process)
    
    # Processar e guardar dados financeiros básicos
    if financial_data_list:
        cleaned_financial_data = clean_financial_data(financial_data_list)
        
        # Guardar em CSV
        cleaned_financial_data.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Dados básicos guardados em CSV: {OUTPUT_CSV}")
        
        # Guardar na base de dados (esta função já calcula os ratios automaticamente)
        save_financial_data_directly(cleaned_financial_data)
    
    # Guardar income statements detalhados
    if income_statements_list:
        save_income_statements_directly(income_statements_list)
        logger.info(f"Income statements detalhados guardados para {len(income_statements_list)} registos")
    
    # Guardar dados de outlook
    if outlook_data_list:
        save_outlook_data_directly(outlook_data_list)
    
    # Calcular ratios para quaisquer dados que possam ter falhado anteriormente
    calculate_ratios_for_all_data()
    
    # Mostrar estatísticas finais
    stats = get_processing_stats()
    logger.info(f"Estatísticas do processamento: {stats}")
    
    # Exemplo de como consultar os dados
    print("\n--- Exemplo de consulta ---")
    
    # Consultar ratios
    ratios_data = query_ratios(tickers=['AAPL', 'ASML'], years=[2023])
    print("Ratios financeiros:")
    print(ratios_data[['ticker', 'year', 'current_ratio', 'debt_to_equity', 'roe', 'roa']].head())
    
    # ... (o resto do teu código de consulta mantém-se igual) ...
    # Consultar dados básicos
    financial_data = query_financial_data(tickers=['AAPL', 'ASML'], years=[2023])
    print("Dados financeiros básicos:")
    print(financial_data.head())
    
    # Consultar income statements detalhados
    income_data = query_income_statements(tickers=['AAPL', 'ASML'], years=[2023])
    print("\nIncome statements detalhados:")
    print(income_data[['ticker', 'year', 'revenue', 'net_income', 'gross_profit_margin']].head())
    
    # Consultar balance sheets dinâmicos
    balance_data = query_balance_sheets(tickers=['ASML'], years=[2023])
    print("\nBalance sheets dinâmicos:")
    print(f"Encontrados {len(balance_data)} registos para ASML")
    
    # Mostrar campos disponíveis no balance sheet do ASML
    asml_balance_fields = get_balance_sheet_fields('ASML', 2023)
    print(f"\nCampos disponíveis no balance sheet do ASML 2023 ({len(asml_balance_fields)} campos):")
    for field in sorted(asml_balance_fields)[:10]:  # Mostrar primeiros 10
        print(f"  - {field}")
    
    # Consultar cash flow statements dinâmicos
    cashflow_data = query_cashflow_statements(tickers=['ASML'], years=[2023])
    print(f"\nCash flow statements dinâmicos:")
    print(f"Encontrados {len(cashflow_data)} registos para ASML")
    
    # Mostrar campos disponíveis no cash flow do ASML
    asml_cashflow_fields = get_cashflow_fields('ASML', 2023)
    print(f"\nCampos disponíveis no cash flow do ASML 2023 ({len(asml_cashflow_fields)} campos):")
    for field in sorted(asml_cashflow_fields)[:10]:  # Mostrar primeiros 10
        print(f"  - {field}")
    
    # Exemplo de como extrair dados específicos do JSON
    if not balance_data.empty:
        import json
        sample_row = balance_data.iloc[0]
        data_json = sample_row['data_json']
        data_dict = json.loads(data_json)
        
        print(f"\nDados específicos do balance sheet para {sample_row['ticker']} {sample_row['year']}:")
        specific_fields = ['Total Assets', 'Total Liabilities Net Minority Interest', 
                          'Total Equity Gross Minority Interest', 'Cash And Cash Equivalents',
                          'Accounts Receivable', 'Property Plant Equipment']
        for field in specific_fields:
            if field in data_dict:
                value = data_dict[field]
                print(f"  {field}: {value}")
    
    # Consultar outlook
    outlook_data = query_company_outlook(tickers=['AAPL', 'ASML'])
    print("\nDados de outlook:")
    print(outlook_data[['ticker', 'company_name', 'sector', 'market_cap']].head())