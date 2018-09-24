using System;
using System.Collections.Generic;
using System.Text;
using Binance;
using BinanceAPITest.Core.Models;

namespace BinanceAPITest.Core.Repositories
{
    public interface ICandlestickRepository
    {
        Candlestick Get(Symbol symbol, DateTime openTime);
        void Insert(Candlestick candlestick, string exchange);
        void InsertBulk(IEnumerable<Candlestick> candlesticks, string exchange);
        void Update(Candlestick candlestick, int klines1mId);
        void Delete(int klines1mId);

        void LogStartDate(DateTime startDate, Symbol symbol);
        DateTime? GetStartDate(Symbol symbol);
        IEnumerable<ExchangeStartInfo> GetStartDates();
        DateTime? GetNextDate(Symbol symbol, string exchange);

        TableMetrics GetTableMetrics(string table);
    }
}
