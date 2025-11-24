import sqlite3
import pandas as pd
import json
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialDataImporter:
    def __init__(self, db_name="Financial_data_Final.db"):
        self.db_name = db_name
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            logger.info(f"Connected to database: {self.db_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def update_database_schema(self):
        """Update existing database schema to include quarter column"""
        try:
            cursor = self.conn.cursor()
            
            # List of tables that need quarter column
            tables_to_update = [
                'financial_statements',
                'income_statements', 
                'balance_sheets',
                'cashflow_statements',
                'ratios'
            ]
            
            for table in tables_to_update:
                # Check if quarter column exists
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'quarter' not in columns:
                    logger.info(f"Adding quarter column to {table} table")
                    
                    if table in ['financial_statements', 'income_statements', 'ratios']:
                        # For tables with year+quarter unique constraint
                        # First create temp table with new schema
                        if table == 'financial_statements':
                            cursor.execute(f'''
                                CREATE TABLE IF NOT EXISTS {table}_temp (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    ticker TEXT NOT NULL,
                                    year INTEGER NOT NULL,
                                    quarter TEXT,
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
                                    UNIQUE(ticker, year, quarter)
                                )
                            ''')
                        elif table == 'income_statements':
                            cursor.execute(f'''
                                CREATE TABLE IF NOT EXISTS {table}_temp (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    ticker TEXT NOT NULL,
                                    year INTEGER NOT NULL,
                                    quarter TEXT,
                                    revenue REAL,
                                    cogs REAL,
                                    gross_profit REAL,
                                    sga_expense REAL,
                                    rd_expense REAL,
                                    operating_income REAL,
                                    net_income REAL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE(ticker, year, quarter)
                                )
                            ''')
                        elif table == 'ratios':
                            cursor.execute(f'''
                                CREATE TABLE IF NOT EXISTS {table}_temp (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    ticker TEXT NOT NULL,
                                    year INTEGER NOT NULL,
                                    quarter TEXT,
                                    current_ratio REAL, 
                                    quick_ratio REAL, 
                                    cash_ratio REAL,
                                    debt_to_equity REAL, 
                                    debt_to_assets REAL, 
                                    equity_multiplier REAL,
                                    roe REAL, 
                                    roa REAL, 
                                    return_on_tangible_equity REAL,
                                    gross_margin REAL, 
                                    operating_margin REAL, 
                                    net_margin REAL,
                                    asset_turnover REAL, 
                                    inventory_turnover REAL, 
                                    receivables_turnover REAL,
                                    operating_cash_flow_ratio REAL, 
                                    free_cash_flow_margin REAL,
                                    working_capital REAL, 
                                    net_debt REAL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE(ticker, year, quarter)
                                )
                            ''')
                        
                        # Copy data from old table to temp table
                        cursor.execute(f"INSERT INTO {table}_temp SELECT *, 'Q1' FROM {table}")
                        # Drop old table
                        cursor.execute(f"DROP TABLE {table}")
                        # Rename temp table to original name
                        cursor.execute(f"ALTER TABLE {table}_temp RENAME TO {table}")
                        
                    else:
                        # For balance_sheets and cashflow_statements, just add the column
                        cursor.execute(f"ALTER TABLE {table} ADD COLUMN quarter TEXT")
                        # Set default value for existing records
                        cursor.execute(f"UPDATE {table} SET quarter = 'Q1' WHERE quarter IS NULL")
            
            self.conn.commit()
            logger.info("Database schema updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error updating database schema: {e}")
            return False
    
    def create_tables(self):
        """Create all necessary tables in the database"""
        try:
            cursor = self.conn.cursor()
            
            # Drop existing tables and recreate with new schema
            tables = [
                'financial_statements',
                'income_statements', 
                'balance_sheets',
                'cashflow_statements',
                'company_outlook',
                'ratios'
            ]
            
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            
            # Main financial statements table - WITH QUARTER
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS financial_statements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter TEXT,
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
                    UNIQUE(ticker, year, quarter)
                )
            ''')
            
            # Detailed income statements table - WITH QUARTER
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS income_statements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter TEXT,
                    revenue REAL,
                    cogs REAL,
                    gross_profit REAL,
                    sga_expense REAL,
                    rd_expense REAL,
                    operating_income REAL,
                    net_income REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, year, quarter)
                )
            ''')
            
            # Balance sheets table with JSON storage - WITH QUARTER
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS balance_sheets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    date TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter TEXT,
                    data_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            ''')
            
            # Cash flow statements table with JSON storage - WITH QUARTER
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cashflow_statements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    date TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter TEXT,
                    data_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            ''')
            
            # Company outlook table
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
            
            # Financial ratios table - WITH QUARTER
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ratios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter TEXT,
                    current_ratio REAL, 
                    quick_ratio REAL, 
                    cash_ratio REAL,
                    debt_to_equity REAL, 
                    debt_to_assets REAL, 
                    equity_multiplier REAL,
                    roe REAL, 
                    roa REAL, 
                    return_on_tangible_equity REAL,
                    gross_margin REAL, 
                    operating_margin REAL, 
                    net_margin REAL,
                    asset_turnover REAL, 
                    inventory_turnover REAL, 
                    receivables_turnover REAL,
                    operating_cash_flow_ratio REAL, 
                    free_cash_flow_margin REAL,
                    working_capital REAL, 
                    net_debt REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, year, quarter)
                )
            ''')
            
            self.conn.commit()
            logger.info("All tables created successfully with quarter support")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def import_financial_statements(self, csv_file="financial_statements_msft.csv"):
        """Import main financial statements from CSV"""
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"File not found: {csv_file}")
                return False
            
            df = pd.read_csv(csv_file)
            
            # Check if quarter column exists, if not add default
            if 'quarter' not in df.columns:
                df['quarter'] = 'Q1'  # Default to Q1 if not specified
            
            cursor = self.conn.cursor()
            
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_statements 
                    (ticker, year, quarter, sector, revenue, gross_profit, ebitda, net_income, 
                     total_assets, total_liabilities, total_debt, capex, free_cash_flow, operating_cash_flow)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['ticker'], row['year'], row.get('quarter', 'Q1'), row['sector'], 
                    row['revenue'], row['gross_profit'], row['ebitda'], row['net_income'],
                    row['total_assets'], row['total_liabilities'], row['total_debt'],
                    row['capex'], row['free_cash_flow'], row['operating_cash_flow']
                ))
            
            self.conn.commit()
            logger.info(f"Imported {len(df)} records into financial_statements table")
            return True
            
        except Exception as e:
            logger.error(f"Error importing financial statements: {e}")
            return False
    
    def import_income_statements(self, csv_file="income_statements_msft.csv"):
        """Import detailed income statements from CSV"""
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"File not found: {csv_file}")
                return False
            
            df = pd.read_csv(csv_file)
            
            # Check if quarter column exists, if not add default
            if 'quarter' not in df.columns:
                df['quarter'] = 'Q1'  # Default to Q1 if not specified
            
            cursor = self.conn.cursor()
            
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT OR REPLACE INTO income_statements 
                    (ticker, year, quarter, revenue, cogs, gross_profit, sga_expense, rd_expense, operating_income, net_income)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['ticker'], row['year'], row.get('quarter', 'Q1'), 
                    row['revenue'], row['cogs'], row['gross_profit'], 
                    row['sga_expense'], row['rd_expense'], row['operating_income'], 
                    row['net_income']
                ))
            
            self.conn.commit()
            logger.info(f"Imported {len(df)} records into income_statements table")
            return True
            
        except Exception as e:
            logger.error(f"Error importing income statements: {e}")
            return False
    
    def import_balance_sheet_json(self, json_file="MSFT_Balance_Sheet_JSON.json"):
        """Import complete balance sheet from JSON"""
        try:
            if not os.path.exists(json_file):
                logger.warning(f"File not found: {json_file}")
                return False
            
            with open(json_file, 'r') as f:
                balance_data = json.load(f)
            
            cursor = self.conn.cursor()
            
            # Get year and quarter from financial statements or use defaults
            cursor.execute("SELECT DISTINCT year, quarter FROM financial_statements WHERE ticker = 'MSFT'")
            result = cursor.fetchone()
            if result:
                year, quarter = result
            else:
                year = datetime.now().year
                quarter = 'Q1'
            
            # Use appropriate date based on quarter
            quarter_dates = {
                'Q1': f"{year}-09-30",  # September 30 for Q1 (Microsoft fiscal)
                'Q2': f"{year}-12-31",  # December 31 for Q2
                'Q3': f"{year}-03-31",  # March 31 for Q3
                'Q4': f"{year}-06-30"   # June 30 for Q4
            }
            date_str = quarter_dates.get(quarter, f"{year}-12-31")
            
            cursor.execute('''
                INSERT OR REPLACE INTO balance_sheets 
                (ticker, date, year, quarter, data_json)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                'MSFT', date_str, year, quarter, json.dumps(balance_data)
            ))
            
            self.conn.commit()
            logger.info(f"Imported balance sheet data for MSFT {year} {quarter}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing balance sheet JSON: {e}")
            return False
    
    def import_cashflow_json(self, json_file="MSFT_Cash_Flow_JSON.json"):
        """Import complete cash flow statement from JSON"""
        try:
            if not os.path.exists(json_file):
                logger.warning(f"File not found: {json_file}")
                return False
            
            with open(json_file, 'r') as f:
                cashflow_data = json.load(f)
            
            cursor = self.conn.cursor()
            
            # Get year and quarter from financial statements or use defaults
            cursor.execute("SELECT DISTINCT year, quarter FROM financial_statements WHERE ticker = 'MSFT'")
            result = cursor.fetchone()
            if result:
                year, quarter = result
            else:
                year = datetime.now().year
                quarter = 'Q1'
            
            # Use appropriate date based on quarter
            quarter_dates = {
                'Q1': f"{year}-09-30",  # September 30 for Q1 (Microsoft fiscal)
                'Q2': f"{year}-12-31",  # December 31 for Q2
                'Q3': f"{year}-03-31",  # March 31 for Q3
                'Q4': f"{year}-06-30"   # June 30 for Q4
            }
            date_str = quarter_dates.get(quarter, f"{year}-12-31")
            
            cursor.execute('''
                INSERT OR REPLACE INTO cashflow_statements 
                (ticker, date, year, quarter, data_json)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                'MSFT', date_str, year, quarter, json.dumps(cashflow_data)
            ))
            
            self.conn.commit()
            logger.info(f"Imported cash flow data for MSFT {year} {quarter}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing cash flow JSON: {e}")
            return False
    
    def import_complete_csv_data(self):
        """Import data from the complete CSV files (Balance Sheet, Income Statement, Cash Flow)"""
        try:
            # Import complete balance sheet from CSV
            if os.path.exists("MSFT_Complete_Balance_Sheet.csv"):
                df_balance = pd.read_csv("MSFT_Complete_Balance_Sheet.csv")
                logger.info(f"Loaded complete balance sheet with {len(df_balance)} accounts")
                
                # Convert to JSON format for storage
                balance_dict = dict(zip(df_balance['Account'], df_balance['Amount']))
                
                cursor = self.conn.cursor()
                cursor.execute("SELECT DISTINCT year, quarter FROM financial_statements WHERE ticker = 'MSFT'")
                result = cursor.fetchone()
                if result:
                    year, quarter = result
                else:
                    year = datetime.now().year
                    quarter = 'Q1'
                
                # Use appropriate date based on quarter
                quarter_dates = {
                    'Q1': f"{year}-09-30",
                    'Q2': f"{year}-12-31", 
                    'Q3': f"{year}-03-31",
                    'Q4': f"{year}-06-30"
                }
                date_str = quarter_dates.get(quarter, f"{year}-12-31")
                
                cursor.execute('''
                    INSERT OR REPLACE INTO balance_sheets 
                    (ticker, date, year, quarter, data_json)
                    VALUES (?, ?, ?, ?, ?)
                ''', ('MSFT', date_str, year, quarter, json.dumps(balance_dict)))
                
                self.conn.commit()
                logger.info("Complete balance sheet CSV data imported")
            
            # Import complete income statement from CSV
            if os.path.exists("MSFT_Complete_Income_Statement.csv"):
                df_income = pd.read_csv("MSFT_Complete_Income_Statement.csv")
                logger.info(f"Loaded complete income statement with {len(df_income)} accounts")
                
                # You could store this as JSON too, or update the income_statements table
                income_dict = dict(zip(df_income['Account'], df_income['Amount']))
                logger.info("Complete income statement CSV data processed")
            
            # Import complete cash flow from CSV
            if os.path.exists("MSFT_Complete_Cash_Flow.csv"):
                df_cashflow = pd.read_csv("MSFT_Complete_Cash_Flow.csv")
                logger.info(f"Loaded complete cash flow with {len(df_cashflow)} accounts")
                
                cashflow_dict = dict(zip(df_cashflow['Account'], df_cashflow['Amount']))
                
                cursor = self.conn.cursor()
                cursor.execute("SELECT DISTINCT year, quarter FROM financial_statements WHERE ticker = 'MSFT'")
                result = cursor.fetchone()
                if result:
                    year, quarter = result
                else:
                    year = datetime.now().year
                    quarter = 'Q1'
                
                # Use appropriate date based on quarter
                quarter_dates = {
                    'Q1': f"{year}-09-30",
                    'Q2': f"{year}-12-31",
                    'Q3': f"{year}-03-31", 
                    'Q4': f"{year}-06-30"
                }
                date_str = quarter_dates.get(quarter, f"{year}-12-31")
                
                cursor.execute('''
                    INSERT OR REPLACE INTO cashflow_statements 
                    (ticker, date, year, quarter, data_json)
                    VALUES (?, ?, ?, ?, ?)
                ''', ('MSFT', date_str, year, quarter, json.dumps(cashflow_dict)))
                
                self.conn.commit()
                logger.info("Complete cash flow CSV data imported")
                
            return True
            
        except Exception as e:
            logger.error(f"Error importing complete CSV data: {e}")
            return False
    
    def calculate_financial_ratios(self):
        """Calculate and store financial ratios based on imported data"""
        try:
            cursor = self.conn.cursor()
            
            # Get financial data for ratio calculation - INCLUDING QUARTER
            cursor.execute('''
                SELECT ticker, year, quarter, total_assets, total_liabilities, total_debt, 
                       revenue, gross_profit, net_income, operating_cash_flow, free_cash_flow
                FROM financial_statements 
                WHERE ticker = 'MSFT'
            ''')
            
            financial_data = cursor.fetchall()
            
            for row in financial_data:
                ticker, year, quarter, total_assets, total_liabilities, total_debt, \
                revenue, gross_profit, net_income, operating_cash_flow, free_cash_flow = row
                
                # Get balance sheet details from JSON
                cursor.execute('''
                    SELECT data_json FROM balance_sheets 
                    WHERE ticker = ? AND year = ? AND quarter = ?
                ''', (ticker, year, quarter))
                
                balance_result = cursor.fetchone()
                if not balance_result:
                    continue
                
                balance_data = json.loads(balance_result[0])
                
                # Extract values from balance sheet with defaults
                current_assets = balance_data.get('CurrentAssets', balance_data.get('TotalCurrentAssets', 0))
                current_liabilities = balance_data.get('CurrentLiabilities', balance_data.get('TotalCurrentLiabilities', 0))
                cash = balance_data.get('CashAndCashEquivalents', 0)
                inventory = balance_data.get('Inventory', 0)
                receivables = balance_data.get('AccountsReceivable', balance_data.get('AccountsReceivableNet', 0))
                total_equity = balance_data.get('TotalEquity', balance_data.get('StockholdersEquity', 0))
                
                # Calculate ratios
                ratios = {
                    'ticker': ticker,
                    'year': year,
                    'quarter': quarter,
                    # Liquidity ratios
                    'current_ratio': current_assets / current_liabilities if current_liabilities else 0,
                    'quick_ratio': (current_assets - inventory) / current_liabilities if current_liabilities else 0,
                    'cash_ratio': cash / current_liabilities if current_liabilities else 0,
                    
                    # Solvency ratios
                    'debt_to_equity': total_debt / total_equity if total_equity else 0,
                    'debt_to_assets': total_debt / total_assets if total_assets else 0,
                    'equity_multiplier': total_assets / total_equity if total_equity else 0,
                    
                    # Profitability ratios
                    'roe': net_income / total_equity if total_equity else 0,
                    'roa': net_income / total_assets if total_assets else 0,
                    'return_on_tangible_equity': net_income / total_equity if total_equity else 0,  # Simplified
                    'gross_margin': (gross_profit / revenue * 100) if revenue else 0,
                    'operating_margin': (net_income / revenue * 100) if revenue else 0,  # Simplified
                    'net_margin': (net_income / revenue * 100) if revenue else 0,
                    
                    # Efficiency ratios
                    'asset_turnover': revenue / total_assets if total_assets else 0,
                    'inventory_turnover': revenue / inventory if inventory else 0,
                    'receivables_turnover': revenue / receivables if receivables else 0,
                    
                    # Cash flow ratios
                    'operating_cash_flow_ratio': operating_cash_flow / current_liabilities if current_liabilities else 0,
                    'free_cash_flow_margin': (free_cash_flow / revenue * 100) if revenue else 0,
                    
                    # Other
                    'working_capital': current_assets - current_liabilities,
                    'net_debt': total_debt - cash
                }
                
                # Insert ratios
                columns = ', '.join(ratios.keys())
                placeholders = ', '.join(['?' for _ in ratios])
                values = list(ratios.values())
                
                cursor.execute(f'''
                    INSERT OR REPLACE INTO ratios ({columns}) 
                    VALUES ({placeholders})
                ''', values)
            
            self.conn.commit()
            logger.info("Financial ratios calculated and stored")
            return True
            
        except Exception as e:
            logger.error(f"Error calculating financial ratios: {e}")
            return False
    
    def add_company_outlook(self):
        """Add Microsoft company outlook data"""
        try:
            cursor = self.conn.cursor()
            
            outlook_data = {
                'ticker': 'MSFT',
                'company_name': 'Microsoft Corporation',
                'sector': 'Technology',
                'industry': 'Software—Infrastructure',
                'country': 'USA',
                'market_cap': 3000000000000,  # Example value - would need real data
                'pe_ratio': 35.0,  # Example value
                'pb_ratio': 12.0,  # Example value
                'dividend_yield': 0.007,  # 0.7%
                'beta': 0.9,
                'fifty_two_week_high': 400.0,  # Example
                'fifty_two_week_low': 250.0,   # Example
                'analyst_target_price': 380.0,  # Example
                'recommendation': 'Buy'
            }
            
            cursor.execute('''
                INSERT OR REPLACE INTO company_outlook 
                (ticker, company_name, sector, industry, country, market_cap, 
                 pe_ratio, pb_ratio, dividend_yield, beta, fifty_two_week_high, 
                 fifty_two_week_low, analyst_target_price, recommendation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(outlook_data.values()))
            
            self.conn.commit()
            logger.info("Company outlook data added for Microsoft")
            return True
            
        except Exception as e:
            logger.error(f"Error adding company outlook: {e}")
            return False
    
    def verify_import(self):
        """Verify that data was imported correctly"""
        try:
            cursor = self.conn.cursor()
            
            tables = [
                'financial_statements',
                'income_statements', 
                'balance_sheets',
                'cashflow_statements',
                'company_outlook',
                'ratios'
            ]
            
            print("\n" + "="*50)
            print("DATABASE IMPORT VERIFICATION")
            print("="*50)
            
            for table in tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                count = cursor.fetchone()[0]
                print(f"{table:25} : {count} records")
            
            # Show sample data with quarter
            print("\nSAMPLE DATA:")
            cursor.execute('''
                SELECT ticker, year, quarter, revenue, net_income 
                FROM financial_statements 
                LIMIT 5
            ''')
            sample_data = cursor.fetchall()
            for row in sample_data:
                print(f"  {row[0]} {row[1]} {row[2]}: Revenue ${row[3]:,.0f}, Net Income ${row[4]:,.0f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error verifying import: {e}")
            return False
    
    def run_complete_import(self):
        """Run the complete import process"""
        logger.info("Starting complete database import process...")
        
        if not self.connect():
            return False
        
        try:
            # Update database schema first
            logger.info("Updating database schema...")
            self.update_database_schema()
            
            # Create tables (will recreate them with new schema)
            if not self.create_tables():
                return False
            
            # Import data from various sources
            self.import_financial_statements()
            self.import_income_statements()
            self.import_balance_sheet_json()
            self.import_cashflow_json()
            self.import_complete_csv_data()
            
            # Add company outlook
            self.add_company_outlook()
            
            # Calculate ratios
            self.calculate_financial_ratios()
            
            # Verify import
            self.verify_import()
            
            logger.info("Database import completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error during import process: {e}")
            return False
        finally:
            self.close()

def main():
    """Main function to run the database import"""
    importer = FinancialDataImporter("Financial_data_Final.db")
    importer.run_complete_import()

if __name__ == "__main__":
    main()