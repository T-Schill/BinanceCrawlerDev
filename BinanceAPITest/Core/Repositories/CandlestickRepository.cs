using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Binance;
using BinanceAPITest.Core.Models;
using Dapper;
using Npgsql;
using Serilog;

namespace BinanceAPITest.Core.Repositories
{
    public class CandlestickRepository : ICandlestickRepository
    {
        private readonly string _connectionString;

        internal IDbConnection Connection
        {
            get
            {
                return new NpgsqlConnection(_connectionString);
            }
        }

        public CandlestickRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void Delete(int klinesId)
        {
            throw new NotImplementedException();
        }

        public Candlestick Get(Symbol symbol, DateTime openTime)
        {
            openTime = new DateTime(openTime.Year, openTime.Month, openTime.Day, openTime.Hour, openTime.Minute, 0);
            var sql = @"SELECT * FROM public.klines where symbol = @symbol AND ""openTime"" = @openTime";           
            using (IDbConnection connection = Connection)
            {
                var data = connection.Query<dynamic>(sql, new { symbol = symbol.ToString(), openTime }).FirstOrDefault();
                if(data != null)
                {
                    return new Candlestick(data.symbol, CandlestickInterval.Minute, data.openTime, data.open, data.high, data.low, data.close, data.volume, data.closeTime, data.quoteAssetVolume, data.numberOfTrades, data.baseVolume, data.quoteVolume);
                }

            }

            return null;
        }

        public void Insert(Candlestick candlestick, string exchange = "Binance")
        {
            var openTime = new DateTime(candlestick.OpenTime.Year, candlestick.OpenTime.Month, candlestick.OpenTime.Day, candlestick.OpenTime.Hour, candlestick.OpenTime.Minute, 0);
            var closeTime = new DateTime(candlestick.CloseTime.Year, candlestick.CloseTime.Month, candlestick.CloseTime.Day, candlestick.CloseTime.Hour, candlestick.CloseTime.Minute, 0);
            var sql = @"INSERT INTO public.klines(""openTime"", open, high, low, close, ""closeTime"", ""quoteAssetVolume"", ""numberOfTrades"", ""baseVolume"", ""quoteVolume"", symbol, exchange)
                VALUES(@openTime, @open, @high, @low, @close, @closeTime, @quoteAssetVolume, @numberOfTrades, @baseVolume, @quoteVolume, @symbol, @exchange); ";
            var parameters = new
            {
                openTime,
                open = candlestick.Open,
                high = candlestick.High,
                low = candlestick.Low,
                close = candlestick.Close,
                volume = candlestick.Volume,
                closeTime,
                quoteAssetVolume = candlestick.QuoteAssetVolume,
                numberOfTrades = candlestick.NumberOfTrades,
                baseVolume = candlestick.TakerBuyBaseAssetVolume,
                quoteVolume = candlestick.TakerBuyQuoteAssetVolume,
                symbol = candlestick.Symbol,
                exchange
            };
            using (IDbConnection connection = Connection)
            {
                connection.Execute(sql, parameters);
            }
        }

        public void InsertBulk(IEnumerable<Candlestick> candlesticks, string exchange = "Binance")
        {
            var sql =  new StringBuilder(@"INSERT INTO public.klines(symbol, ""openTime"", open, high, low, close, ""closeTime"", ""quoteAssetVolume"", ""numberOfTrades"", ""baseVolume"", ""quoteVolume"", exchange) ");
            foreach(var candlestick in candlesticks)
            {
                sql.AppendLine($"SELECT '{candlestick.Symbol}', '{candlestick.OpenTime.ToString("yyyy-MM-dd HH:mm:ss")}'::timestamp, {candlestick.Open}, {candlestick.High}, {candlestick.Low}, {candlestick.Close}, '{candlestick.CloseTime.ToString("yyyy-MM-dd HH:mm:ss")}'::timestamp, {candlestick.Volume}, {candlestick.NumberOfTrades}, {candlestick.TakerBuyBaseAssetVolume}, {candlestick.TakerBuyQuoteAssetVolume}, '{exchange}'");
                sql.AppendLine("UNION ALL");
            }
            sql.Remove(sql.Length - 9, 9); // trim the last UNION ALL
          
            using (IDbConnection connection = Connection)
            {
                connection.Execute(sql.ToString());
            }         
        }

        public void Update(Candlestick candlestick, int klinesId)
        {
            throw new NotImplementedException();
        }

        public void LogStartDate(DateTime startDate, Symbol symbol)
        {
            var sql = "INSERT INTO public.start_dates(symbol, start_date) VALUES(@symbol, @startDate)";
            using (IDbConnection connection = Connection)
            {
                connection.Execute(sql, new { symbol = symbol.ToString(), startDate});
            }
        }

        public DateTime? GetStartDate(Symbol symbol)
        {
            var sql = "SELECT start_date from start_dates WHERE symbol = @symbol";
            var startDate = default(DateTime?);
            using (IDbConnection connection = Connection)
            {
                startDate = connection.Query<DateTime?>(sql, new { symbol = symbol.ToString()}).FirstOrDefault();
            }

            return startDate;
        }

        public IEnumerable<ExchangeStartInfo> GetStartDates()
        {
            var sql = "SELECT start_date as \"StartDate\", symbol as \"SymbolLabel\" from start_dates";            
            using (IDbConnection connection = Connection)
            {
                return connection.Query<ExchangeStartInfo>(sql);
            }           
        }

        public DateTime? GetNextDate(Symbol symbol, string exchange = "Binance")
        {
            var sql = "SELECT \"closeTime\" from klines WHERE symbol = @symbol and exchange = @exchange order by \"closeTime\" desc limit 1";
            DateTime? startDate = null;
            using (IDbConnection connection = Connection)
            {
                startDate = connection.Query<DateTime?>(sql, new { symbol = symbol.ToString(), exchange }).FirstOrDefault();
            }

            return startDate;
        }

        public TableMetrics GetTableMetrics(string table)
        {
            var sql = @"SELECT
               relname as ""Table"",
               pg_total_relation_size(relid) As ""Size"",
               pg_total_relation_size(relid) - pg_relation_size(relid) as ""ExternalSize""
               FROM pg_catalog.pg_statio_user_tables
            WHERE relname = @table
            ORDER BY pg_total_relation_size(relid) DESC; ";

            using (IDbConnection connection = Connection)
            {
                return connection.Query<TableMetrics>(sql, new { table }).FirstOrDefault();
            }
        }
    }
}
