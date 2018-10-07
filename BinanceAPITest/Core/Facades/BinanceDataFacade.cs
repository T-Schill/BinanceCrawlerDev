using Binance;
using BinanceAPITest.Core.Models;
using BinanceAPITest.Core.Repositories;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinanceAPITest.Core.Facades
{
    public class BinanceDataFacade
    {
        private readonly IBinanceApi _api;
        private readonly ICandlestickRepository _candlestickRepository;
        private readonly IEnumerable<string> _onlySymbols;
        private readonly IEnumerable<string> _excludeSymbols;
        private readonly string _startSymbol;

        public BinanceDataFacade(IBinanceApi api, ICandlestickRepository candlestickRepository, IEnumerable<string> onlySymbols, IEnumerable<string> excludeSymbols, string startSymbol)
        {            
            _api = api;
            _candlestickRepository = candlestickRepository;
            _onlySymbols = onlySymbols;
            _excludeSymbols = excludeSymbols;
            _startSymbol = startSymbol;
        }

        public async Task<IEnumerable<ExchangeStartInfo>> InitializeSymbols()
        {
            var symbols = await _api.GetSymbolsAsync();
            var loggedExchangeStartInfo = _candlestickRepository.GetStartDates();
            var exchangeStartInfo = new List<ExchangeStartInfo>();
            var count = 1;           
            var latestStartDate = default(DateTime?);
            var hasStartSymbolConfigured = !string.IsNullOrEmpty(_startSymbol);
            var startSymbolTriggered = !hasStartSymbolConfigured;
            foreach (var symbol in symbols)
            {

                if(hasStartSymbolConfigured && symbol == _startSymbol)
                {
                    startSymbolTriggered = true;
                }

                if(!startSymbolTriggered)
                {
                    continue;
                }

                if((_onlySymbols.Any() && !_onlySymbols.Contains(symbol)) || _excludeSymbols.Any(s => s == symbol))
                {
                    continue;
                }              
                var info = loggedExchangeStartInfo.FirstOrDefault(i => i.SymbolLabel == symbol);
                if (info != null)
                {
                    info.Symbol = symbol;
                    Log.Logger.ForContext("Action", "InitializeSymbols")
                        .Debug("Already logged start date for symbol {Symbol}", symbol);                    
                    count++;
                    exchangeStartInfo.Add(info);
                    latestStartDate = info.StartDate;
                    continue;
                }
                var startDate = await GetStartDate(symbol, null);
                if (startDate.HasValue)
                {
                    Log.Logger.ForContext("Action", "InitializeSymbols")
                        .Information($"Found start date {{StartDate}} for symbol {{Symbol}}. {count++} of {symbols.Count()}", startDate, symbol);                   
                    _candlestickRepository.LogStartDate(startDate.Value, symbol);
                    exchangeStartInfo.Add(new ExchangeStartInfo { Symbol = symbol, StartDate = startDate.Value });
                }
                else
                {
                    Log.Logger.ForContext("Action", "InitializeSymbols")
                       .Error($"Failed to find start date for symbol {symbol}. {count++} of {symbols.Count()}");
                }
            }

            return exchangeStartInfo;
        }

        public async Task CrawlSymbol(ExchangeStartInfo info)
        {
            var startDateData = _candlestickRepository.GetNextDate(info.Symbol, "Binance");
            DateTime startDate;
            if(!startDateData.HasValue)
            {
                Log.Logger.ForContext("Action", "CrawlSymbol")
                    .Debug("No candlesticks logged for symbol {Symbol}", info.Symbol);

                startDate = new DateTime(info.StartDate.Ticks, DateTimeKind.Utc);                
            }
            else
            {
                startDate = new DateTime(startDateData.Value.Ticks, DateTimeKind.Utc);
                Log.Logger.ForContext("Action", "CrawlSymbol")
                   .Debug("Found candlesticks logged for {Symbol}.", info.Symbol);
            }

            Log.Logger.ForContext("Action", "CrawlSymbol")
                .Information("Crawling 1m Candlesticks for Symbol {Symbol} starting from {StartCrawlDate}", info.Symbol, startDate);           
            
            var endDate = startDate.AddMinutes(999);        
            var pageSize = 1000;   // TODO: Get this from configuration    
            var recordsParsed = 0;
            var stopwatch = new Stopwatch();
            stopwatch.Start();            

            var candleSticksBatch = await GetNextCandlesticks(info.Symbol, startDate, endDate, pageSize);

            while(candleSticksBatch.CandleSticks.Any())
            {
                _candlestickRepository.InsertBulk(candleSticksBatch.CandleSticks, "Binance");

                Log.Logger.ForContext("Action", "CrawlSymbol")
                      .Verbose("Logged {Count} Candlesticks for Symbol {Symbol} for range {StartTime} to {EndTime}", candleSticksBatch.CandleSticks.Count(), info.Symbol, startDate, endDate);

                recordsParsed += candleSticksBatch.CandleSticks.Count();

                Log.Logger.ForContext("Action", "CrawlSymbol")
                    .Debug("Elapsed time {ElapsedMilliseconds}. {TotalRecords} candlesticks recorded for {Symbol}", stopwatch.ElapsedMilliseconds, recordsParsed, info.Symbol);

                startDate = endDate;
                endDate = startDate.AddMinutes(999);
                candleSticksBatch = await GetNextCandlesticks(info.Symbol, startDate, endDate, pageSize);
            }

            if (Log.Logger.IsEnabled(Serilog.Events.LogEventLevel.Debug))
            {
                var metrics = _candlestickRepository.GetTableMetrics("klines");
                Log.Logger.ForContext("Action", "KlinesMetrics")
                    .Debug($"Klines table size {{Size}} ({metrics.Size.PrettyBytes()}). External size {{ExternalSize}} ({metrics.ExternalSize.PrettyBytes()})", metrics.Size, metrics.ExternalSize);
            }
        }

      
        /// <summary>
        /// Given the symbol, will fetch the startDate of the first candlestick recorded to the minute.
        /// Will call the API 2-4 times
        /// </summary>
        /// <param name="symbol">The symbol to use for the search</param>
        /// <returns></returns>
        public async Task<DateTime?> GetStartDate(Symbol symbol, DateTime? initialDate)
        {
            try
            {
                var startDate = initialDate.HasValue ? new DateTime(initialDate.Value.Ticks, DateTimeKind.Utc) : new DateTime(2015, 1, 1, 12, 0, 0, DateTimeKind.Utc); // This should go far enough back before all Exchange APIs ;
              
                var endDate = startDate.AddDays(999);
                var candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Day, startDate, endDate, 1000);
                while (!candlesticks.Any() && startDate <= DateTime.UtcNow)
                {
                    startDate = endDate;
                    endDate = startDate.AddDays(999);
                    candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Day, startDate, endDate, 1000);
                }
                if(!candlesticks.Any() && startDate > DateTime.UtcNow)
                {
                    Log.Logger.ForContext("Action", "GetStartDate")
                        .Warning("No start time found for symbol {Symbol}", symbol);

                    return null;
                }

                startDate = candlesticks.First().OpenTime;
                endDate = startDate.AddMinutes(999);
                candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, 1000);

                while (!candlesticks.Any())
                {
                    startDate = startDate.AddMinutes(1000);
                    endDate = startDate.AddMinutes(999);
                    candlesticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, 1000);
                }
                return candlesticks.Any() ? candlesticks.First().OpenTime : startDate;
            }
            catch (Exception e)
            {
                Log.Logger.ForContext("Action", "GetStartDate")
                    .Error(e, "Error getting start date for symbol {Symbol}", symbol);
            }

            return null;
        }

        /// <summary>
        /// Gets the next set of candlesticks. Will take maintenance windows and long coin swaps into account for gaps in data.
        /// </summary>
        /// <param name="symbol">Symbol to fetch</param>
        /// <param name="startDate">Start of the time frame</param>
        /// <param name="endDate">End of the time frame</param>
        /// <param name="pageSize">Page size to fetch</param>
        /// <returns>CandleStickBatch with the result candlesticks and startDate\endDate for the actual next range found.</returns>
        private async Task<CandleSticksBatch> GetNextCandlesticks(Symbol symbol, DateTime startDate, DateTime endDate, int pageSize)
        {
            var candleSticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, pageSize);
            var candleStickBatch = new CandleSticksBatch()
            {
                CandleSticks = candleSticks,
                StartDate = startDate,
                EndDate = endDate
            };
            if(!candleSticks.Any() && startDate > DateTime.UtcNow)
            {
                Log.Logger.ForContext("Action", "GetCandleSticks")
                    .Debug("Caught up on candlesticks for symbol {Symbol}", symbol);
                return candleStickBatch;
            }
            else if(!candleSticks.Any())
            {
                var nextDate = await GetStartDate(symbol, endDate);
                if(!nextDate.HasValue)
                {
                    Log.Logger.ForContext("Action", "GetCandleSticks")
                        .Information("No more candlesticks found for {Symbol}. End of data at {EndDate}", symbol, endDate);
                    return candleStickBatch;
                }
                candleStickBatch.StartDate = nextDate.Value;
                candleStickBatch.EndDate = startDate.AddMinutes(pageSize);
                candleStickBatch.CandleSticks = await _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, candleStickBatch.StartDate, candleStickBatch.EndDate, pageSize);
            }

            //while(!candleSticks.Any() && maintenanceWindowHoursCount < _maintenanceWindowHours)
            //{
            //    maintenanceWindowHoursCount += pageSize/60;
            //    startDate = endDate;
            //    endDate = startDate.AddMinutes(pageSize);
            //    candleSticks = _api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, pageSize).Result;
            //}

            //if(maintenanceWindowHoursCount >= _maintenanceWindowHours)
            //{
                
            //}

            return candleStickBatch;
        }
    }
}
