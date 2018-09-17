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
    public class DatabaseRepository : IDatabaseRespository
    {
        private readonly string _connectionString;

        internal IDbConnection Connection
        {
            get
            {
                return new NpgsqlConnection(_connectionString);
            }
        }

        public DatabaseRepository(string connectionString)
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

        public void Insert(Candlestick candlestick)
        {
            var openTime = new DateTime(candlestick.OpenTime.Year, candlestick.OpenTime.Month, candlestick.OpenTime.Day, candlestick.OpenTime.Hour, candlestick.OpenTime.Minute, 0);
            var closeTime = new DateTime(candlestick.CloseTime.Year, candlestick.CloseTime.Month, candlestick.CloseTime.Day, candlestick.CloseTime.Hour, candlestick.CloseTime.Minute, 0);
            var sql = @"INSERT INTO public.klines(""openTime"", open, high, low, close, ""closeTime"", ""quoteAssetVolume"", ""numberOfTrades"", ""baseVolume"", ""quoteVolume"", symbol)
                VALUES(@openTime, @open, @high, @low, @close, @closeTime, @quoteAssetVolume, @numberOfTrades, @baseVolume, @quoteVolume, @symbol); ";
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
                symbol = candlestick.Symbol
            };
            using (IDbConnection connection = Connection)
            {
                connection.Execute(sql, parameters);
            }
        }

        public void InsertBulk(IEnumerable<Candlestick> candlesticks)
        {

            var sql =  new StringBuilder(@"INSERT INTO public.klines(symbol, ""openTime"", open, high, low, close, ""closeTime"", ""quoteAssetVolume"", ""numberOfTrades"", ""baseVolume"", ""quoteVolume"") ");
            foreach(var candlestick in candlesticks)
            {
                sql.AppendLine($"SELECT '{candlestick.Symbol}', '{candlestick.OpenTime.ToString("yyyy-MM-dd HH:mm:ss")}'::timestamp, {candlestick.Open}, {candlestick.High}, {candlestick.Low}, {candlestick.Close}, '{candlestick.CloseTime.ToString("yyyy-MM-dd HH:mm:ss")}'::timestamp, {candlestick.Volume}, {candlestick.NumberOfTrades}, {candlestick.TakerBuyBaseAssetVolume}, {candlestick.TakerBuyQuoteAssetVolume}");
                sql.AppendLine("UNION ALL");
            }
            sql.Remove(sql.Length - 9, 9); // trim the last UNION ALL

            Log.Logger.ForContext("Action", "InsertBulk")
                .Debug(sql.ToString());

            using (IDbConnection connection = Connection)
            {
                connection.Execute(sql.ToString());
            }


            //using (SqlBulkCopy copy = new SqlBulkCopy(_connectionString))
            //{
            //    copy.DestinationTableName = "Quotes";
            //    DataTable table = new DataTable("Quotes");
            //    table.Columns.Add("symbol", typeof(string));
            //    table.Columns.Add("openTime", typeof(DateTime));
            //    table.Columns.Add("open", typeof(decimal));
            //    table.Columns.Add("high", typeof(decimal));
            //    table.Columns.Add("low", typeof(decimal));
            //    table.Columns.Add("close", typeof(decimal));
            //    table.Columns.Add("volume", typeof(decimal));
            //    table.Columns.Add("closeTime", typeof(DateTime));
            //    table.Columns.Add("quoteAssetVolume", typeof(decimal));
            //    table.Columns.Add("numberOfTrades", typeof(int));
            //    table.Columns.Add("baseVolume", typeof(decimal));
            //    table.Columns.Add("quoteVolume", typeof(decimal));

            //    foreach(var candlestick in candlesticks)
            //    {
            //        table.Rows.Add(candlestick.Symbol, candlestick.OpenTime, candlestick.Open, candlestick.High, candlestick.Low, candlestick.Close, candlestick.Volume, candlestick.CloseTime, candlestick.QuoteAssetVolume, candlestick.NumberOfTrades, candlestick.TakerBuyBaseAssetVolume, candlestick.TakerBuyQuoteAssetVolume);
            //    }

            //    // TODO: Make async?
            //    copy.WriteToServer(table);
            //}
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
            DateTime? startDate = null;
            using (IDbConnection connection = Connection)
            {
                startDate = connection.Query<DateTime?>(sql, new { symbol = symbol.ToString()}).FirstOrDefault();
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
