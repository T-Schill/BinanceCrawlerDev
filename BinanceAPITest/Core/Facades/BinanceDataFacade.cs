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

        public BinanceDataFacade(IBinanceApi api, ICandlestickRepository candlestickRepository)
        {
            _api = api;
            _candlestickRepository = candlestickRepository;
        }

        public async Task<IEnumerable<ExchangeStartInfo>> InitializeSymbols()
        {
            var symbols = await _api.GetSymbolsAsync();
            var loggedExchangeStartInfo = _candlestickRepository.GetStartDates();
            var exchangeStartInfo = new List<ExchangeStartInfo>();
            var count = 1;
            var initialDate = new DateTime(2018, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var latestStartDate = default(DateTime?); 
            foreach (var symbol in symbols)
            {
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
                var startDate = await GetStartDate(symbol, latestStartDate.HasValue ? latestStartDate.Value : initialDate);
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
            var startDateData = _candlestickRepository.GetNextDate(info.Symbol);
            DateTime startDate;
            if(!startDateData.HasValue)
            {
                Log.Logger.ForContext("Action", "CrawlSymbol")
                    .Debug("No candlesticks logged for symbol '{Symbol}'", info.Symbol);

                startDate = new DateTime(info.StartDate.Ticks, DateTimeKind.Utc);                
            }
            else
            {
                startDate = new DateTime(startDateData.Value.Ticks, DateTimeKind.Utc);
                Log.Logger.ForContext("Action", "CrawlSymbol")
                   .Debug("Found candlesticks logged for '{Symbol}'.", info.Symbol);
            }

            Log.Logger.ForContext("Action", "CrawlSymbol")
                .Information("Crawling 1m Candlesticks for Symbol '{Symbol}' starting from {StartCrawlDate}", info.Symbol, startDate);           
            
            var endDate = startDate.AddMinutes(999);        
            var pageSize = 1000;   // TODO: Get this from configuration    
            var recordsParsed = 0;
            var stopwatch = new Stopwatch();
            stopwatch.Start();            

            var candlesticks = await _api.GetCandlesticksAsync(info.Symbol, CandlestickInterval.Minute, startDate, endDate, pageSize);

            while(candlesticks.Any())
            {
                _candlestickRepository.InsertBulk(candlesticks);

                Log.Logger.ForContext("Action", "CrawlSymbol")
                      .Verbose("Logged {Count} Candlesticks for Symbol {Symbol} for range {StartTime} to {EndTime}", candlesticks.Count(), info.Symbol, startDate, endDate);

                recordsParsed += candlesticks.Count();

                if (Log.Logger.IsEnabled(Serilog.Events.LogEventLevel.Debug))
                {
                    var metrics = _candlestickRepository.GetTableMetrics("klines");
                    Log.Logger.ForContext("Action", "KlinesMetrics")
                        .Debug($"Klines table size {{Size}} ({metrics.Size.PrettyBytes()}). External size {{ExternalSize}} ({metrics.ExternalSize.PrettyBytes()})", metrics.Size, metrics.ExternalSize);
                }


                Log.Logger.ForContext("Action", "CrawlSymbol")
                    .Debug("Elapsed time {ElapsedMilliseconds}. Total records parsed {TotalRecords}", stopwatch.ElapsedMilliseconds, recordsParsed);

                startDate = endDate;
                endDate = startDate.AddMinutes(999);
                candlesticks = await _api.GetCandlesticksAsync(info.Symbol, CandlestickInterval.Minute, startDate, endDate, pageSize);
            }

        }

        public async Task<DateTime?> GetStartDate(Symbol symbol, DateTime initialDate)
        {
            try
            {
                var startDate = initialDate;
                if(startDate.Kind != DateTimeKind.Utc)
                {
                    startDate = new DateTime(startDate.Ticks, DateTimeKind.Utc);
                }
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
                Log.Logger.Error(e, "Error getting start date for symbol {Symbol}", symbol);               
            }

            return null;
        }
    }
}
