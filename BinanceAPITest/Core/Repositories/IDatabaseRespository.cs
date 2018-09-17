using System;
using System.Collections.Generic;
using System.Text;
using Binance;
using BinanceAPITest.Core.Models;

namespace BinanceAPITest.Core.Repositories
{
    public interface IDatabaseRespository
    {
        Candlestick Get(Symbol symbol, DateTime openTime);
        void Insert(Candlestick candlestick);
        void Update(Candlestick candlestick, int klines1mId);
        void Delete(int klines1mId);

        void LogStartDate(DateTime startDate, Symbol symbol);
        DateTime? GetStartDate(Symbol symbol);

        TableMetrics GetTableMetrics(string table);
    }
}
