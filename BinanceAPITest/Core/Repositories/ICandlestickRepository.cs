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
        void Insert(Candlestick candlestick);
        void InsertBulk(IEnumerable<Candlestick> candlesticks);
        void Update(Candlestick candlestick, int klines1mId);
        void Delete(int klines1mId);

        void LogStartDate(DateTime startDate, Symbol symbol);
        DateTime? GetStartDate(Symbol symbol);
        IEnumerable<ExchangeStartInfo> GetStartDates();
        DateTime? GetNextDate(Symbol symbol);

        TableMetrics GetTableMetrics(string table);
    }
}
