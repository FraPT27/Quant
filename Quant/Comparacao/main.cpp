#include <sqlite3.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <memory>
#include <cmath>
#include <optional>
#include <numeric>
#include <random>

class Database {
private:
    sqlite3* db;
    
public:
    Database(const std::string& db_path) : db(nullptr) {
        if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
            throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(db)));
        }
    }
    
    ~Database() {
        if (db) {
            sqlite3_close(db);
        }
    }
    
    std::vector<std::map<std::string, std::string>> executeQuery(const std::string& query) {
        sqlite3_stmt* stmt;
        std::vector<std::map<std::string, std::string>> results;
        
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db)));
        }
        
        int columnCount = sqlite3_column_count(stmt);
        
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::map<std::string, std::string> row;
            for (int i = 0; i < columnCount; i++) {
                std::string columnName = sqlite3_column_name(stmt, i);
                const unsigned char* value = sqlite3_column_text(stmt, i);
                row[columnName] = value ? std::string(reinterpret_cast<const char*>(value)) : "N/A";
            }
            results.push_back(row);
        }
        
        sqlite3_finalize(stmt);
        return results;
    }

    std::vector<std::string> getTableNames() {
        std::string query = "SELECT name FROM sqlite_master WHERE type='table';";
        std::vector<std::string> tables;
        try {
            auto results = executeQuery(query);
            for (const auto& row : results) {
                tables.push_back(row.at("name"));
            }
        } catch (const std::exception& e) {
            std::cout << "Error getting table names: " << e.what() << "\n";
        }
        return tables;
    }

    std::vector<std::string> getColumnNames(const std::string& tableName) {
        std::string query = "PRAGMA table_info(" + tableName + ");";
        std::vector<std::string> columns;
        try {
            auto results = executeQuery(query);
            for (const auto& row : results) {
                columns.push_back(row.at("name"));
            }
        } catch (const std::exception& e) {
            std::cout << "Error getting column names for " << tableName << ": " << e.what() << "\n";
        }
        return columns;
    }

    void inspectDatabase() {
        std::cout << "\n=== DATABASE INSPECTION ===\n";
        auto tables = getTableNames();
        
        if (tables.empty()) {
            std::cout << "No tables found in the database!\n";
            return;
        }
        
        std::cout << "Found " << tables.size() << " tables:\n";
        
        for (const auto& table : tables) {
            std::cout << "\nTable: " << table << "\n";
            auto columns = getColumnNames(table);
            std::cout << "Columns (" << columns.size() << "): ";
            for (size_t i = 0; i < columns.size(); ++i) {
                std::cout << columns[i];
                if (i < columns.size() - 1) std::cout << ", ";
            }
            std::cout << "\n";
            
            // Show sample data
            try {
                std::string sampleQuery = "SELECT * FROM " + table + " LIMIT 2;";
                auto sampleResults = executeQuery(sampleQuery);
                if (!sampleResults.empty()) {
                    std::cout << "Sample data:\n";
                    for (const auto& row : sampleResults) {
                        for (const auto& [key, value] : row) {
                            std::cout << "  " << key << ": " << (value.length() > 50 ? value.substr(0, 50) + "..." : value) << "\n";
                        }
                        std::cout << "  ---\n";
                    }
                } else {
                    std::cout << "  Table is empty\n";
                }
            } catch (const std::exception& e) {
                std::cout << "  Could not sample data: " << e.what() << "\n";
            }
        }
    }
    
    bool tableExists(const std::string& tableName) {
        auto tables = getTableNames();
        return std::find(tables.begin(), tables.end(), tableName) != tables.end();
    }
};

class MonteCarloSimulator {
private:
    std::mt19937_64 rng;
    
public:
    MonteCarloSimulator() : rng(std::random_device{}()) {}
    
    // Gera caminhos aleatórios usando Geometric Brownian Motion (GBM)
    std::vector<std::vector<double>> simulateGBM(double initialValue, double meanReturn, 
                                                double volatility, int years, int numSimulations) {
        std::vector<std::vector<double>> paths(numSimulations);
        std::normal_distribution<double> normal(0.0, 1.0);
        
        double dt = 1.0; // 1 year time step
        double drift = (meanReturn - 0.5 * volatility * volatility) * dt;
        double diffusion = volatility * std::sqrt(dt);
        
        for (int i = 0; i < numSimulations; ++i) {
            std::vector<double> path(years + 1);
            path[0] = initialValue;
            
            for (int t = 1; t <= years; ++t) {
                double randomShock = normal(rng);
                path[t] = path[t-1] * std::exp(drift + diffusion * randomShock);
            }
            
            paths[i] = path;
        }
        
        return paths;
    }
    
    // Calcula estatísticas dos caminhos simulados
    std::map<std::string, double> calculateStatistics(const std::vector<std::vector<double>>& paths) {
        std::map<std::string, double> stats;
        int numSimulations = paths.size();
        int projectionYears = paths[0].size() - 1;
        
        // Coletar valores finais
        std::vector<double> finalValues(numSimulations);
        for (int i = 0; i < numSimulations; ++i) {
            finalValues[i] = paths[i].back();
        }
        
        // Ordenar para percentis
        std::sort(finalValues.begin(), finalValues.end());
        
        // Calcular estatísticas
        double sum = std::accumulate(finalValues.begin(), finalValues.end(), 0.0);
        stats["mean"] = sum / numSimulations;
        stats["median"] = finalValues[numSimulations / 2];
        stats["p5"] = finalValues[static_cast<int>(numSimulations * 0.05)];
        stats["p25"] = finalValues[static_cast<int>(numSimulations * 0.25)];
        stats["p75"] = finalValues[static_cast<int>(numSimulations * 0.75)];
        stats["p95"] = finalValues[static_cast<int>(numSimulations * 0.95)];
        stats["min"] = finalValues.front();
        stats["max"] = finalValues.back();
        
        // Probabilidade de crescimento
        double initialValue = paths[0][0];
        int growthCount = std::count_if(finalValues.begin(), finalValues.end(),
                                       [initialValue](double x) { return x > initialValue; });
        stats["growth_probability"] = (static_cast<double>(growthCount) / numSimulations) * 100.0;
        
        return stats;
    }
};

class FinancialAnalyzer {
private:
    Database db;
    std::string mainTable;
    MonteCarloSimulator mcSimulator;
    
    std::string formatMillions(double value) {
        if (value == 0) return "0";
        double absValue = std::abs(value);
        double millions = value / 1000000.0;
        std::stringstream ss;
        
        if (absValue >= 1000000000) { // Billions
            double billions = value / 1000000000.0;
            ss << std::fixed << std::setprecision(2) << billions << "B";
        } else if (absValue >= 1000000) { // Millions
            ss << std::fixed << std::setprecision(1) << millions << "M";
        } else if (absValue >= 1000) { // Thousands
            double thousands = value / 1000.0;
            ss << std::fixed << std::setprecision(1) << thousands << "K";
        } else {
            ss << std::fixed << std::setprecision(0) << value;
        }
        
        return ss.str();
    }
    
    void runMonteCarloTests() {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "MONTE CARLO SIMULATION - TEST CASES\n";
        std::cout << std::string(70, '=') << "\n";
        
        // Test Case 1: Ação com crescimento estável
        std::cout << "\nTEST CASE 1: Stable Growth Company\n";
        std::cout << "Initial Revenue: 1000M, Expected Return: 8%, Volatility: 15%\n";
        testMonteCarloScenario("TEST1", "Revenue", 1000.0, 0.08, 0.15, 5, 5000);
        
        // Test Case 2: Ação de alta volatilidade
        std::cout << "\nTEST CASE 2: High Volatility Tech Stock\n";
        std::cout << "Initial Revenue: 500M, Expected Return: 12%, Volatility: 40%\n";
        testMonteCarloScenario("TEST2", "Revenue", 500.0, 0.12, 0.40, 5, 5000);
        
        // Test Case 3: Ação defensiva
        std::cout << "\nTEST CASE 3: Defensive Stock\n";
        std::cout << "Initial Revenue: 2000M, Expected Return: 4%, Volatility: 10%\n";
        testMonteCarloScenario("TEST3", "Revenue", 2000.0, 0.04, 0.10, 5, 5000);
    }
    
    void testMonteCarloScenario(const std::string& ticker, const std::string& metric,
                               double initialValue, double meanReturn, double volatility,
                               int years, int simulations) {
        auto paths = mcSimulator.simulateGBM(initialValue, meanReturn, volatility, years, simulations);
        auto stats = mcSimulator.calculateStatistics(paths);
        
        std::cout << "\n" << std::string(60, '-') << "\n";
        std::cout << "SIMULATION RESULTS (" << ticker << " - " << metric << ")\n";
        std::cout << std::string(60, '-') << "\n";
        std::cout << "Initial Value: " << formatMillions(initialValue) << "\n";
        std::cout << "Expected Annual Return: " << std::fixed << std::setprecision(1) << (meanReturn * 100) << "%\n";
        std::cout << "Annual Volatility: " << std::fixed << std::setprecision(1) << (volatility * 100) << "%\n";
        std::cout << "Projection Years: " << years << "\n";
        std::cout << "Simulations: " << simulations << "\n";
        std::cout << "\nPROJECTION STATISTICS (in millions):\n";
        std::cout << "Average: " << formatMillions(stats["mean"]) << "\n";
        std::cout << "Median: " << formatMillions(stats["median"]) << "\n";
        std::cout << "5th Percentile: " << formatMillions(stats["p5"]) << "\n";
        std::cout << "25th Percentile: " << formatMillions(stats["p25"]) << "\n";
        std::cout << "75th Percentile: " << formatMillions(stats["p75"]) << "\n";
        std::cout << "95th Percentile: " << formatMillions(stats["p95"]) << "\n";
        std::cout << "Probability of Growth: " << std::fixed << std::setprecision(1) << stats["growth_probability"] << "%\n";
        
        // Verificação de sanidade - os resultados devem fazer sentido
        std::cout << "\nSANITY CHECK:\n";
        double expected_value = initialValue * std::exp(meanReturn * years);
        std::cout << "Theoretical Expected Value: " << formatMillions(expected_value) << "\n";
        std::cout << "Simulation Average: " << formatMillions(stats["mean"]) << "\n";
        std::cout << "Difference: " << std::fixed << std::setprecision(1) 
                  << ((stats["mean"] - expected_value) / expected_value * 100) << "%\n";
        
        if (std::abs(stats["mean"] - expected_value) / expected_value < 0.1) {
            std::cout << "✅ Simulation results are consistent with theoretical expectations\n";
        } else {
            std::cout << "⚠️  Significant deviation from theoretical expectations\n";
        }
    }
    
    void displayMonteCarloResults(const std::string& ticker, const std::string& metric,
                                 double currentValue, int currentYear,
                                 double meanReturn, double volatility,
                                 int projectionYears, int simulations,
                                 const std::map<std::string, double>& stats) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "MONTE CARLO SIMULATION RESULTS: " << ticker << " - " << metric << "\n";
        std::cout << std::string(70, '=') << "\n";
        std::cout << "Current Value (" << currentYear << "): " << formatMillions(currentValue) << "\n";
        std::cout << "Historical Mean Return: " << std::fixed << std::setprecision(1) << (meanReturn * 100) << "%\n";
        std::cout << "Historical Volatility: " << std::fixed << std::setprecision(1) << (volatility * 100) << "%\n";
        std::cout << "Projection Years: " << projectionYears << "\n";
        std::cout << "Simulations: " << simulations << "\n";
        
        std::cout << "\nPROJECTION STATISTICS FOR " << (currentYear + projectionYears) << " (in millions):\n";
        std::cout << "Average: " << formatMillions(stats.at("mean")) << "\n";
        std::cout << "Median: " << formatMillions(stats.at("median")) << "\n";
        std::cout << "5th Percentile (Conservative): " << formatMillions(stats.at("p5")) << "\n";
        std::cout << "25th Percentile: " << formatMillions(stats.at("p25")) << "\n";
        std::cout << "75th Percentile: " << formatMillions(stats.at("p75")) << "\n";
        std::cout << "95th Percentile (Optimistic): " << formatMillions(stats.at("p95")) << "\n";
        std::cout << "Probability of Growth: " << std::fixed << std::setprecision(1) << stats.at("growth_probability") << "%\n";
        
        // Análise de risco
        std::cout << "\nRISK ANALYSIS:\n";
        double downside_risk = (stats.at("p5") - currentValue) / currentValue * 100;
        double upside_potential = (stats.at("p95") - currentValue) / currentValue * 100;
        
        std::cout << "Downside Risk (5th %ile): " << std::fixed << std::setprecision(1) << downside_risk << "%\n";
        std::cout << "Upside Potential (95th %ile): " << std::fixed << std::setprecision(1) << upside_potential << "%\n";
        
        if (stats.at("growth_probability") > 70) {
            std::cout << "📈 HIGH confidence in growth\n";
        } else if (stats.at("growth_probability") > 50) {
            std::cout << "↗️  MODERATE confidence in growth\n";
        } else {
            std::cout << "📊 UNCERTAIN growth outlook\n";
        }
    }
    
public:
    FinancialAnalyzer(const std::string& db_path) : db(db_path) {
        std::cout << "Database connected successfully!\n";
        
        // Inspect the database to understand its structure
        db.inspectDatabase();
        
        // Try to find the main financial data table
        auto tables = db.getTableNames();
        
        if (tables.empty()) {
            std::cout << "No tables found in the database!\n";
            return;
        }
        
        // Common financial table names to look for
        std::vector<std::string> financialTableNames = {
            "financial_data", "financial", "stocks", "stock_data", 
            "company_data", "companies", "financial_statements"
        };
        
        for (const auto& table : tables) {
            std::string lowerTable = table;
            std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(), ::tolower);
            
            for (const auto& financialName : financialTableNames) {
                if (lowerTable.find(financialName) != std::string::npos) {
                    mainTable = table;
                    std::cout << "Using table: " << mainTable << " for financial data\n";
                    return;
                }
            }
        }
        
        // If no obvious financial table found, let user choose
        if (mainTable.empty()) {
            std::cout << "\nNo obvious financial table found. Please select a table:\n";
            for (size_t i = 0; i < tables.size(); ++i) {
                std::cout << i + 1 << ". " << tables[i] << "\n";
            }
            
            int choice;
            std::cout << "Enter choice (1-" << tables.size() << "): ";
            std::cin >> choice;
            
            if (choice >= 1 && choice <= static_cast<int>(tables.size())) {
                mainTable = tables[choice - 1];
                std::cout << "Using table: " << mainTable << "\n";
            } else {
                mainTable = tables[0]; // Use first table as fallback
                std::cout << "Invalid choice. Using: " << mainTable << "\n";
            }
        }
    }
    
    void setMainTable(const std::string& tableName) {
        if (db.tableExists(tableName)) {
            mainTable = tableName;
            std::cout << "Main table set to: " << mainTable << "\n";
        } else {
            std::cout << "Table '" << tableName << "' does not exist!\n";
        }
    }
    
    // Feature 1: Stock Comparison
    void stockComparison() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        std::string ticker1, ticker2;
        int year;
        
        std::cout << "\n=== STOCK COMPARISON (HEAD-TO-HEAD) ===\n";
        std::cout << "Enter first ticker: ";
        std::cin >> ticker1;
        std::cout << "Enter second ticker: ";
        std::cin >> ticker2;
        std::cout << "Enter year: ";
        std::cin >> year;
        
        std::transform(ticker1.begin(), ticker1.end(), ticker1.begin(), ::toupper);
        std::transform(ticker2.begin(), ticker2.end(), ticker2.begin(), ::toupper);
        
        auto columns = db.getColumnNames(mainTable);
        
        bool hasTicker = std::find(columns.begin(), columns.end(), "ticker") != columns.end();
        bool hasYear = std::find(columns.begin(), columns.end(), "year") != columns.end();
        
        if (!hasTicker || !hasYear) {
            std::cout << "Error: Table doesn't have required 'ticker' or 'year' columns!\n";
            return;
        }
        
        std::stringstream query;
        query << "SELECT ";
        
        std::vector<std::string> numericColumns;
        for (const auto& col : columns) {
            if (col == "ticker" || col == "year" || col == "sector" || col == "company" || 
                col == "id" || col == "date" || col == "period") continue;
            numericColumns.push_back(col);
        }
        
        if (numericColumns.empty()) {
            std::cout << "No numeric columns found for comparison!\n";
            return;
        }
        
        query << "ticker, year";
        for (const auto& col : numericColumns) {
            query << ", " << col;
        }
        
        query << " FROM " << mainTable 
              << " WHERE (ticker = '" << ticker1 << "' OR ticker = '" << ticker2 << "') "
              << "AND year = " << year;
        
        try {
            auto results = db.executeQuery(query.str());
            
            if (results.empty()) {
                std::cout << "No data found for the specified tickers and year.\n";
                return;
            }
            
            std::cout << "\n" << std::string(80, '=') << "\n";
            std::cout << std::left << std::setw(25) << "METRIC";
            
            std::vector<std::string> foundTickers;
            for (const auto& row : results) {
                std::string ticker = row.at("ticker");
                if (std::find(foundTickers.begin(), foundTickers.end(), ticker) == foundTickers.end()) {
                    foundTickers.push_back(ticker);
                }
            }
            
            for (const auto& ticker : foundTickers) {
                std::cout << std::setw(25) << ticker;
            }
            std::cout << "\n" << std::string(80, '=') << "\n";
            
            for (const auto& col : numericColumns) {
                std::cout << std::left << std::setw(25) << col;
                for (const auto& ticker : foundTickers) {
                    bool found = false;
                    for (const auto& row : results) {
                        if (row.at("ticker") == ticker) {
                            std::string value = row.at(col);
                            if (value != "N/A") {
                                try {
                                    double numValue = std::stod(value);
                                    // Always format in millions for financial metrics
                                    std::cout << std::setw(25) << formatMillions(numValue);
                                } catch (...) {
                                    std::cout << std::setw(25) << value;
                                }
                            } else {
                                std::cout << std::setw(25) << "N/A";
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        std::cout << std::setw(25) << "N/A";
                    }
                }
                std::cout << "\n";
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error fetching data: " << e.what() << "\n";
        }
    }
    
    // Feature 2: Sector Analysis
    void sectorAnalysis() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        auto columns = db.getColumnNames(mainTable);
        
        bool hasSector = std::find(columns.begin(), columns.end(), "sector") != columns.end();
        
        if (!hasSector) {
            std::cout << "No sector information found in the database.\n";
            return;
        }
        
        std::cout << "\n=== SECTOR ANALYSIS ===\n";
        
        try {
            auto sectorResults = db.executeQuery("SELECT DISTINCT sector FROM " + mainTable + " WHERE sector IS NOT NULL AND sector != 'N/A';");
            if (sectorResults.empty()) {
                std::cout << "No sectors found in the database.\n";
                return;
            }
            
            std::cout << "Available sectors:\n";
            for (const auto& row : sectorResults) {
                std::cout << " - " << row.at("sector") << "\n";
            }
        } catch (const std::exception& e) {
            std::cout << "Error getting sectors: " << e.what() << "\n";
            return;
        }
        
        std::string sector;
        int year;
        
        std::cout << "Enter sector: ";
        std::cin.ignore();
        std::getline(std::cin, sector);
        std::cout << "Enter year: ";
        std::cin >> year;
        
        std::string metricColumn;
        for (const auto& col : columns) {
            if (col != "ticker" && col != "year" && col != "sector" && col != "id" && 
                col != "company" && col != "date" && col != "period") {
                metricColumn = col;
                break;
            }
        }
        
        if (metricColumn.empty()) {
            std::cout << "No numeric metrics found for analysis.\n";
            return;
        }
        
        std::string query = "SELECT ticker, " + metricColumn + " FROM " + mainTable + 
                           " WHERE sector = '" + sector + "' AND year = " + std::to_string(year) + 
                           " AND " + metricColumn + " IS NOT NULL AND " + metricColumn + " != 'N/A';";
        
        try {
            auto results = db.executeQuery(query);
            
            if (results.empty()) {
                std::cout << "No data found for sector '" << sector << "' in year " << year << "\n";
                return;
            }
            
            std::vector<double> values;
            for (const auto& row : results) {
                try {
                    values.push_back(std::stod(row.at(metricColumn)));
                } catch (...) {
                    // Skip non-numeric values
                }
            }
            
            if (values.empty()) {
                std::cout << "No numeric data found for analysis.\n";
                return;
            }
            
            // Calculate advanced statistics
            std::sort(values.begin(), values.end());
            double sum = 0.0;
            for (double val : values) sum += val;
            double average = sum / values.size();
            double median = values.size() % 2 == 0 ? 
                (values[values.size()/2 - 1] + values[values.size()/2]) / 2.0 : 
                values[values.size()/2];
            
            // Standard deviation
            double variance = 0.0;
            for (double val : values) {
                variance += (val - average) * (val - average);
            }
            variance /= values.size();
            double std_dev = std::sqrt(variance);
            
            std::cout << "\n" << std::string(60, '=') << "\n";
            std::cout << "SECTOR ANALYSIS: " << sector << " (" << year << ")\n";
            std::cout << "Metric: " << metricColumn << "\n";
            std::cout << std::string(60, '=') << "\n";
            std::cout << "Companies analyzed: " << values.size() << "\n";
            std::cout << "Average: " << formatMillions(average) << "\n";
            std::cout << "Median: " << formatMillions(median) << "\n";
            std::cout << "Standard Deviation: " << formatMillions(std_dev) << "\n";
            std::cout << "Min: " << formatMillions(values.front()) << "\n";
            std::cout << "Max: " << formatMillions(values.back()) << "\n";
            std::cout << "25th Percentile: " << formatMillions(values[values.size()/4]) << "\n";
            std::cout << "75th Percentile: " << formatMillions(values[3*values.size()/4]) << "\n";
            
        } catch (const std::exception& e) {
            std::cout << "Error in sector analysis: " << e.what() << "\n";
        }
    }
    
    // Feature 3: Portfolio Screener
    void portfolioScreener() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        auto columns = db.getColumnNames(mainTable);
        
        std::cout << "\n=== PORTFOLIO SCREENER ===\n";
        std::cout << "Available numeric columns for screening:\n";
        for (const auto& col : columns) {
            if (col != "ticker" && col != "year" && col != "sector" && col != "id" && 
                col != "company" && col != "date" && col != "period") {
                std::cout << " - " << col << "\n";
            }
        }
        
        std::string condition;
        std::cout << "\nEnter screening condition (e.g., \"revenue > 1000 AND net_income > 500\"):\n";
        std::cout << "NOTE: Values should be in millions (e.g., 1000 for 1 billion)\n";
        std::cin.ignore();
        std::getline(std::cin, condition);
        
        if (condition.empty()) {
            std::cout << "No condition provided.\n";
            return;
        }
        
        std::string query = "SELECT ticker, year FROM " + mainTable + " WHERE " + condition + " ORDER BY ticker, year;";
        
        try {
            auto results = db.executeQuery(query);
            
            std::cout << "\n" << std::string(60, '=') << "\n";
            std::cout << "SCREENER RESULTS: " << results.size() << " companies found\n";
            std::cout << std::string(60, '=') << "\n";
            
            if (!results.empty()) {
                std::cout << std::left << std::setw(10) << "Ticker" << std::setw(10) << "Year" << "\n";
                std::cout << std::string(20, '-') << "\n";
                
                for (const auto& row : results) {
                    std::cout << std::left << std::setw(10) << row.at("ticker")
                              << std::setw(10) << row.at("year") << "\n";
                }
            } else {
                std::cout << "No companies matched your criteria.\n";
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error running screener: " << e.what() << "\n";
            std::cout << "Please check your condition syntax.\n";
        }
    }
    
    // Feature 4: Financial Ratios Analysis
    void financialRatiosAnalysis() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        std::string ticker;
        int year;
        
        std::cout << "\n=== FINANCIAL RATIOS ANALYSIS ===\n";
        std::cout << "Enter ticker: ";
        std::cin >> ticker;
        std::cout << "Enter year: ";
        std::cin >> year;
        
        std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);
        
        auto columns = db.getColumnNames(mainTable);
        
        // Try to get common financial metrics
        std::map<std::string, double> metrics;
        std::vector<std::string> metricNames = {
            "revenue", "net_income", "gross_profit", "operating_income",
            "total_assets", "total_liabilities", "shareholders_equity",
            "current_assets", "current_liabilities", "cash", "long_term_debt",
            "ebitda", "eps", "shares_outstanding"
        };
        
        std::stringstream query;
        query << "SELECT * FROM " << mainTable 
              << " WHERE ticker = '" << ticker << "' AND year = " << year;
        
        try {
            auto results = db.executeQuery(query.str());
            
            if (results.empty()) {
                std::cout << "No data found for " << ticker << " in year " << year << "\n";
                return;
            }
            
            // Extract available metrics
            for (const auto& row : results) {
                for (const auto& metric : metricNames) {
                    if (row.find(metric) != row.end() && row.at(metric) != "N/A") {
                        try {
                            metrics[metric] = std::stod(row.at(metric));
                        } catch (...) {
                            metrics[metric] = 0.0;
                        }
                    }
                }
            }
            
            std::cout << "\n" << std::string(70, '=') << "\n";
            std::cout << "FINANCIAL RATIOS: " << ticker << " (" << year << ")\n";
            std::cout << std::string(70, '=') << "\n";
            
            // Display key financial metrics in millions
            std::cout << "KEY FINANCIAL METRICS (in millions):\n";
            if (metrics.count("revenue")) std::cout << "Revenue: " << formatMillions(metrics["revenue"]) << "\n";
            if (metrics.count("net_income")) std::cout << "Net Income: " << formatMillions(metrics["net_income"]) << "\n";
            if (metrics.count("ebitda")) std::cout << "EBITDA: " << formatMillions(metrics["ebitda"]) << "\n";
            if (metrics.count("total_assets")) std::cout << "Total Assets: " << formatMillions(metrics["total_assets"]) << "\n";
            if (metrics.count("total_liabilities")) std::cout << "Total Liabilities: " << formatMillions(metrics["total_liabilities"]) << "\n";
            
            std::cout << "\nPROFITABILITY RATIOS:\n";
            // Calculate and display ratios
            if (metrics.count("net_income") && metrics.count("revenue") && metrics["revenue"] != 0) {
                double net_margin = (metrics["net_income"] / metrics["revenue"]) * 100;
                std::cout << "Net Profit Margin: " << std::fixed << std::setprecision(2) << net_margin << "%\n";
            }
            
            if (metrics.count("gross_profit") && metrics.count("revenue") && metrics["revenue"] != 0) {
                double gross_margin = (metrics["gross_profit"] / metrics["revenue"]) * 100;
                std::cout << "Gross Margin: " << std::fixed << std::setprecision(2) << gross_margin << "%\n";
            }
            
            if (metrics.count("ebitda") && metrics.count("revenue") && metrics["revenue"] != 0) {
                double ebitda_margin = (metrics["ebitda"] / metrics["revenue"]) * 100;
                std::cout << "EBITDA Margin: " << std::fixed << std::setprecision(2) << ebitda_margin << "%\n";
            }
            
            std::cout << "\nLIQUIDITY & SOLVENCY RATIOS:\n";
            if (metrics.count("current_assets") && metrics.count("current_liabilities") && metrics["current_liabilities"] != 0) {
                double current_ratio = metrics["current_assets"] / metrics["current_liabilities"];
                std::cout << "Current Ratio: " << std::fixed << std::setprecision(2) << current_ratio << "x\n";
            }
            
            if (metrics.count("total_liabilities") && metrics.count("total_assets") && metrics["total_assets"] != 0) {
                double debt_ratio = (metrics["total_liabilities"] / metrics["total_assets"]) * 100;
                std::cout << "Debt Ratio: " << std::fixed << std::setprecision(2) << debt_ratio << "%\n";
            }
            
            if (metrics.count("long_term_debt") && metrics.count("ebitda") && metrics["ebitda"] != 0) {
                double debt_ebitda = metrics["long_term_debt"] / metrics["ebitda"];
                std::cout << "Debt/EBITDA: " << std::fixed << std::setprecision(2) << debt_ebitda << "x\n";
            }
            
            std::cout << "\nRETURN RATIOS:\n";
            if (metrics.count("net_income") && metrics.count("shareholders_equity") && metrics["shareholders_equity"] != 0) {
                double roe = (metrics["net_income"] / metrics["shareholders_equity"]) * 100;
                std::cout << "Return on Equity (ROE): " << std::fixed << std::setprecision(2) << roe << "%\n";
            }
            
            if (metrics.count("operating_income") && metrics.count("total_assets") && metrics["total_assets"] != 0) {
                double roa = (metrics["operating_income"] / metrics["total_assets"]) * 100;
                std::cout << "Return on Assets (ROA): " << std::fixed << std::setprecision(2) << roa << "%\n";
            }
            
            std::cout << std::string(70, '=') << "\n";
            
        } catch (const std::exception& e) {
            std::cout << "Error in ratio analysis: " << e.what() << "\n";
        }
    }
    
    // Feature 5: Time Series Analysis
    void timeSeriesAnalysis() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        std::string ticker;
        std::string metric;
        int years;
        
        std::cout << "\n=== TIME SERIES ANALYSIS ===\n";
        std::cout << "Enter ticker: ";
        std::cin >> ticker;
        
        auto columns = db.getColumnNames(mainTable);
        std::cout << "Available metrics:\n";
        for (const auto& col : columns) {
            if (col != "ticker" && col != "year" && col != "sector" && col != "id" && 
                col != "company" && col != "date" && col != "period") {
                std::cout << " - " << col << "\n";
            }
        }
        
        std::cout << "Enter metric to analyze: ";
        std::cin >> metric;
        std::cout << "Enter number of years to analyze: ";
        std::cin >> years;
        
        std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);
        
        std::stringstream query;
        query << "SELECT year, " << metric << " FROM " << mainTable 
              << " WHERE ticker = '" << ticker << "' AND year IS NOT NULL"
              << " ORDER BY year DESC LIMIT " << years;
        
        try {
            auto results = db.executeQuery(query.str());
            
            if (results.empty()) {
                std::cout << "No time series data found for " << ticker << "\n";
                return;
            }
            
            std::vector<double> values;
            std::vector<int> years_data;
            
            for (const auto& row : results) {
                if (row.at(metric) != "N/A") {
                    try {
                        values.push_back(std::stod(row.at(metric)));
                        years_data.push_back(std::stoi(row.at("year")));
                    } catch (...) {
                        // Skip invalid data
                    }
                }
            }
            
            if (values.size() < 2) {
                std::cout << "Insufficient data for time series analysis.\n";
                return;
            }
            
            std::cout << "\n" << std::string(60, '=') << "\n";
            std::cout << "TIME SERIES ANALYSIS: " << ticker << " - " << metric << "\n";
            std::cout << std::string(60, '=') << "\n";
            
            // Display historical data in millions
            std::cout << "HISTORICAL DATA (in millions):\n";
            for (size_t i = 0; i < values.size(); ++i) {
                std::cout << years_data[i] << ": " << formatMillions(values[i]) << "\n";
            }
            
            // Calculate growth rates
            std::cout << "\nGROWTH ANALYSIS:\n";
            for (size_t i = 1; i < values.size(); ++i) {
                double growth = ((values[i-1] - values[i]) / values[i]) * 100;
                std::cout << years_data[i] << " to " << years_data[i-1] << ": " 
                          << std::fixed << std::setprecision(2) << growth << "%\n";
            }
            
            // Calculate CAGR if we have enough data
            if (values.size() >= 2) {
                double cagr = (std::pow(values[0] / values.back(), 1.0 / (years_data[0] - years_data.back())) - 1) * 100;
                std::cout << "\nCAGR (" << years_data.back() << "-" << years_data[0] << "): " 
                          << std::fixed << std::setprecision(2) << cagr << "%\n";
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error in time series analysis: " << e.what() << "\n";
        }
    }
    
    // Feature 6: Monte Carlo Simulation - CORRIGIDA
    void monteCarloSimulation() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        std::cout << "\n=== MONTE CARLO SIMULATION ===\n";
        std::cout << "This simulation uses Geometric Brownian Motion to project financial metrics.\n\n";
        
        // Opção para usar dados reais ou de teste
        char useTestData;
        std::cout << "Use test data for demonstration? (y/n): ";
        std::cin >> useTestData;
        
        if (useTestData == 'y' || useTestData == 'Y') {
            runMonteCarloTests();
            return;
        }
        
        // Caso contrário, usar dados reais da base de dados
        std::string ticker;
        std::string metric;
        int simulations;
        int years_projection;
        
        std::cout << "Enter ticker: ";
        std::cin >> ticker;
        
        auto columns = db.getColumnNames(mainTable);
        std::cout << "Available metrics for simulation:\n";
        for (const auto& col : columns) {
            if (col != "ticker" && col != "year" && col != "sector" && col != "id" && 
                col != "company" && col != "date" && col != "period") {
                std::cout << " - " << col << "\n";
            }
        }
        
        std::cout << "Enter metric: ";
        std::cin >> metric;
        std::cout << "Enter number of simulations (recommended: 1000-10000): ";
        std::cin >> simulations;
        std::cout << "Enter years for projection: ";
        std::cin >> years_projection;
        
        std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);
        
        // Get historical data for parameter estimation
        std::stringstream query;
        query << "SELECT year, " << metric << " FROM " << mainTable 
              << " WHERE ticker = '" << ticker << "' AND " << metric << " IS NOT NULL"
              << " AND " << metric << " != 'N/A'"
              << " ORDER BY year ASC";  // Ordem ascendente para cálculos corretos
        
        try {
            auto results = db.executeQuery(query.str());
            
            if (results.size() < 3) {
                std::cout << "Insufficient historical data for simulation (need at least 3 data points).\n";
                std::cout << "Available data points: " << results.size() << "\n";
                return;
            }
            
            // Extrair valores históricos
            std::vector<double> historical_values;
            std::vector<int> years;
            
            for (const auto& row : results) {
                try {
                    historical_values.push_back(std::stod(row.at(metric)));
                    years.push_back(std::stoi(row.at("year")));
                } catch (...) {
                    // Skip invalid data
                }
            }
            
            if (historical_values.size() < 3) {
                std::cout << "Insufficient valid data points for simulation.\n";
                return;
            }
            
            // Calcular retornos logarítmicos e volatilidade
            std::vector<double> log_returns;
            for (size_t i = 1; i < historical_values.size(); ++i) {
                if (historical_values[i-1] > 0) {
                    double log_return = std::log(historical_values[i] / historical_values[i-1]);
                    log_returns.push_back(log_return);
                }
            }
            
            if (log_returns.empty()) {
                std::cout << "Cannot calculate returns from the data.\n";
                return;
            }
            
            // Calcular média e desvio padrão dos retornos
            double mean_return = std::accumulate(log_returns.begin(), log_returns.end(), 0.0) / log_returns.size();
            double variance = 0.0;
            for (double ret : log_returns) {
                variance += (ret - mean_return) * (ret - mean_return);
            }
            double volatility = std::sqrt(variance / log_returns.size());
            
            double current_value = historical_values.back();
            int latest_year = years.back();
            
            // Executar simulação Monte Carlo
            auto paths = mcSimulator.simulateGBM(current_value, mean_return, volatility, years_projection, simulations);
            auto stats = mcSimulator.calculateStatistics(paths);
            
            // Mostrar resultados
            displayMonteCarloResults(ticker, metric, current_value, latest_year, 
                                   mean_return, volatility, years_projection, simulations, stats);
            
        } catch (const std::exception& e) {
            std::cout << "Error in Monte Carlo simulation: " << e.what() << "\n";
        }
    }
    
    // Feature 7: Risk Analysis
    void riskAnalysis() {
        if (mainTable.empty()) {
            std::cout << "No suitable table found for financial data!\n";
            return;
        }
        
        std::string ticker;
        std::cout << "\n=== RISK ANALYSIS ===\n";
        std::cout << "Enter ticker: ";
        std::cin >> ticker;
        
        std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);
        
        // Get volatility data
        std::string volatilityQuery = 
            "SELECT year, revenue, net_income, total_assets, total_liabilities "
            "FROM " + mainTable + " WHERE ticker = '" + ticker + "' "
            "AND year IS NOT NULL ORDER BY year DESC LIMIT 5";
        
        try {
            auto results = db.executeQuery(volatilityQuery);
            
            if (results.size() < 3) {
                std::cout << "Insufficient data for risk analysis.\n";
                return;
            }
            
            std::map<std::string, std::vector<double>> metrics;
            std::vector<int> years;
            
            for (const auto& row : results) {
                years.push_back(std::stoi(row.at("year")));
                
                for (const auto& [key, value] : row) {
                    if (key != "year" && key != "ticker" && value != "N/A") {
                        try {
                            metrics[key].push_back(std::stod(value));
                        } catch (...) {
                            // Skip conversion errors
                        }
                    }
                }
            }
            
            std::cout << "\n" << std::string(60, '=') << "\n";
            std::cout << "RISK ANALYSIS: " << ticker << "\n";
            std::cout << std::string(60, '=') << "\n";
            
            // Calculate volatility for each metric
            std::cout << "VOLATILITY ANALYSIS (Coefficient of Variation):\n";
            for (const auto& [metric, values] : metrics) {
                if (values.size() >= 3) {
                    double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
                    double variance = 0.0;
                    for (double val : values) {
                        variance += (val - mean) * (val - mean);
                    }
                    double std_dev = std::sqrt(variance / values.size());
                    double coefficient_of_variation = (std_dev / std::abs(mean)) * 100;
                    
                    std::cout << metric << ": " 
                              << std::fixed << std::setprecision(2) << coefficient_of_variation << "%\n";
                }
            }
            
            // Calculate financial stability ratios
            std::cout << "\nFINANCIAL STABILITY:\n";
            if (metrics.count("total_assets") && metrics.count("total_liabilities")) {
                auto& assets = metrics["total_assets"];
                auto& liabilities = metrics["total_liabilities"];
                
                if (!assets.empty() && !liabilities.empty()) {
                    double current_debt_ratio = liabilities.back() / assets.back();
                    std::cout << "Current Debt Ratio: " << std::fixed << std::setprecision(3) << current_debt_ratio << "\n";
                    
                    if (current_debt_ratio > 0.6) {
                        std::cout << "⚠️  High debt level detected\n";
                    } else if (current_debt_ratio > 0.4) {
                        std::cout << "ℹ️  Moderate debt level\n";
                    } else {
                        std::cout << "✅ Conservative debt level\n";
                    }
                }
            }
            
            // Revenue stability analysis
            if (metrics.count("revenue") && metrics["revenue"].size() >= 3) {
                auto& revenue = metrics["revenue"];
                double total_growth = 0.0;
                int growth_periods = 0;
                
                for (size_t i = 1; i < revenue.size(); ++i) {
                    if (revenue[i-1] > 0) {
                        double growth = (revenue[i] - revenue[i-1]) / revenue[i-1];
                        total_growth += growth;
                        growth_periods++;
                    }
                }
                
                if (growth_periods > 0) {
                    double avg_growth = (total_growth / growth_periods) * 100;
                    std::cout << "\nREVENUE STABILITY:\n";
                    std::cout << "Average Revenue Growth: " << std::fixed << std::setprecision(2) << avg_growth << "%\n";
                    
                    if (avg_growth > 15) {
                        std::cout << "🚀 High growth company\n";
                    } else if (avg_growth > 5) {
                        std::cout << "📈 Moderate growth company\n";
                    } else if (avg_growth > 0) {
                        std::cout << "📊 Stable company\n";
                    } else {
                        std::cout << "📉 Declining company\n";
                    }
                }
            }
            
            // Show current financial position
            std::cout << "\nCURRENT FINANCIAL POSITION (in millions):\n";
            for (const auto& [metric, values] : metrics) {
                if (!values.empty()) {
                    std::cout << metric << ": " << formatMillions(values.back()) << "\n";
                }
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error in risk analysis: " << e.what() << "\n";
        }
    }
    
    // Feature 8: Change Main Table
    void changeMainTable() {
        auto tables = db.getTableNames();
        
        if (tables.empty()) {
            std::cout << "No tables found in the database!\n";
            return;
        }
        
        std::cout << "\n=== CHANGE MAIN TABLE ===\n";
        std::cout << "Available tables:\n";
        for (size_t i = 0; i < tables.size(); ++i) {
            std::cout << i + 1 << ". " << tables[i] << "\n";
        }
        
        int choice;
        std::cout << "Select table (1-" << tables.size() << "): ";
        std::cin >> choice;
        
        if (choice >= 1 && choice <= static_cast<int>(tables.size())) {
            mainTable = tables[choice - 1];
            std::cout << "Main table changed to: " << mainTable << "\n";
        } else {
            std::cout << "Invalid choice!\n";
        }
    }
};

void showMenu() {
    std::cout << "\n" << std::string(50, '=') << "\n";
    std::cout << "WALL STREET QUANT ANALYSIS TOOL\n";
    std::cout << std::string(50, '=') << "\n";
    std::cout << "1. Stock Comparison (Head-to-Head)\n";
    std::cout << "2. Sector Analysis\n";
    std::cout << "3. Portfolio Screener\n";
    std::cout << "4. Financial Ratios Analysis\n";
    std::cout << "5. Time Series Analysis\n";
    std::cout << "6. Monte Carlo Simulation\n";
    std::cout << "7. Risk Analysis\n";
    std::cout << "8. Change Main Table\n";
    std::cout << "9. Exit\n";
    std::cout << std::string(50, '=') << "\n";
    std::cout << "Select option (1-9): ";
}

int main() {
    try {
        std::string dbPath;
        std::cout << "Enter database path (default: financial_data_new.db): ";
        std::getline(std::cin, dbPath);
        
        if (dbPath.empty()) {
            dbPath = "../../financial_data.db";
        }
        
        std::cout << "Using database: " << dbPath << "\n";
        FinancialAnalyzer analyzer(dbPath);
        
        int choice = 0;
        while (choice != 9) {
            showMenu();
            std::cin >> choice;
            
            switch (choice) {
                case 1:
                    analyzer.stockComparison();
                    break;
                case 2:
                    analyzer.sectorAnalysis();
                    break;
                case 3:
                    analyzer.portfolioScreener();
                    break;
                case 4:
                    analyzer.financialRatiosAnalysis();
                    break;
                case 5:
                    analyzer.timeSeriesAnalysis();
                    break;
                case 6:
                    analyzer.monteCarloSimulation();
                    break;
                case 7:
                    analyzer.riskAnalysis();
                    break;
                case 8:
                    analyzer.changeMainTable();
                    break;
                case 9:
                    std::cout << "Goodbye!\n";
                    break;
                default:
                    std::cout << "Invalid option. Please try again.\n";
                    break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        std::cerr << "Make sure the database file exists and is a valid SQLite database.\n";
        return 1;
    }
    
    return 0;
}