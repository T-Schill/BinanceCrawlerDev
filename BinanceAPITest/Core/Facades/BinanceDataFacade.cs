using Binance;
using BinanceAPITest.Core.Repositories;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinanceAPITest.Core.Facades
{
    public class BinanceDataFacade
    {
        private readonly IBinanceApi _api;
        private readonly IDatabaseRespository _database;       

        public BinanceDataFacade(IBinanceApi api, IDatabaseRespository database)
        {
            _api = api;
            _database = database;
        }

        public async Task LogStartDates()
        {
            var symbols = await _api.GetSymbolsAsync();
            var count = 1;
            DateTime? startDate = new DateTime(2018, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            foreach (var symbol in symbols)
            {
                if (_database.GetStartDate(symbol) != null)
                {
                    Log.Logger.Information("Already logged start date for symbol " + symbol);                    
                    count++;
                    continue;
                }
                startDate = await GetStartDate(symbol, startDate.Value);
                if (startDate.HasValue)
                {
                    Log.Logger.Information($"Found start date {startDate.Value.ToShortDateString()} {startDate.Value.ToShortTimeString()} for symbol {symbol}. {count++} of {symbols.Count()}");                   
                    _database.LogStartDate(startDate.Value, symbol);
                }
            }
        }

        private async Task<DateTime?> GetStartDate(Symbol symbol, DateTime initialDate)
        {
            try
            {
                var startDate = initialDate;
                var endDate = startDate.AddMinutes(999);
                var candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, 1000);
                while (!candlesticks.Any())
                {
                    startDate = endDate.AddDays(7);
                    endDate = startDate.AddMinutes(999);
                    candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, 1000);
                }
                while (candlesticks.Count() == 1000)
                {
                    startDate = startDate.AddMinutes(-1000);
                    endDate = startDate.AddMinutes(999);
                    candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, 1000);
                }
                return candlesticks.Any() ? candlesticks.First().OpenTime : startDate;
            }
            catch (Exception e)
            {
                Log.Logger.Error(e, "Error getting start date for symbol " + symbol);               
            }

            return null;
        }
    }
}
