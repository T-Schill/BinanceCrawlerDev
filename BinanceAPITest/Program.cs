using System;
using Binance;
using Binance.WebSocket;
using Binance.Cache;
using System.Linq;
using Serilog;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using BinanceAPITest.Core.Repositories;
using System.Threading.Tasks;
using BinanceAPITest.Core.Facades;
using BinanceAPITest.Core.Logging;
using BinanceAPITest.Core;
using System.Diagnostics;
using System.Collections.Generic;

namespace BinanceAPITest
{
    class Program
    {
        public static IConfigurationRoot Configuration;
        public static IServiceProvider ServiceProvider;
        public static readonly object ConsoleSync = new object();        
       

        static void Main(string[] args)
        {
            Configuration = new ConfigurationBuilder()
                                    .SetBasePath(Directory.GetCurrentDirectory())
                                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                                    .Build();

            var connectionString = Configuration.GetConnectionString("DefaultConnection");
            var logLevel = Configuration.GetValue<string>("Logging:LogLevel") ?? "Information";
            var onlySymbolsString = Configuration.GetValue<string>("CrawlOptions:OnlySymbols");
            var onlySymbols = new List<string>();
            if(!string.IsNullOrWhiteSpace(onlySymbolsString))
            {
                onlySymbols = onlySymbolsString.Split(',').ToList();
            }
            var excludeSymbolsString = Configuration.GetValue<string>("CrawlOptions:ExcludeSymbols");
            var excludeSymbols = new List<string>();
            if (!string.IsNullOrWhiteSpace(excludeSymbolsString))
            {
                excludeSymbols = excludeSymbolsString.Split(',').ToList();
            }

            var startSymbol = Configuration.GetValue<string>("CrawlOptions:StartSymbol");
            Log.Logger = new LoggerConfiguration()
                   .MinimumLevel.ControlledBy(SerilogConfiguration.LoggingLevel)
                   .Enrich.WithMachineName()
                   .Enrich.WithEnvironmentUserName()
                   .Enrich.WithProperty("Source", "FinaScope BinanceCrawler 0.1")  
                   .WriteTo.Console()
                   .WriteTo.PostgreSQL(connectionString, "logs", ColumnConfiguration.GetColumnConfiguration(), needAutoCreateTable: true)
                   .CreateLogger();

            SerilogConfiguration.SetLoggingLevel(logLevel);
            Log.Logger.Debug("Running with the following crawl configuration. StartSymbol: {StartSymbol}. OnlySymbols: '{OnlySymbols}'. ExcludeSymbols: '{ExcludeSymbols}'", startSymbol, onlySymbolsString, excludeSymbolsString);

            try
            {
                ServiceProvider = new ServiceCollection()
                    .AddBinance()
                    .AddOptions()
                    .Configure<BinanceApiOptions>(Configuration.GetSection("ApiOptions"))
                    .AddLogging(builder => builder.AddSerilog(Log.Logger))
                    .BuildServiceProvider();                

                var api = ServiceProvider.GetService<IBinanceApi>();
                var candlestickRepository = new CandlestickRepository(connectionString);
                var binanceDataFacade = new BinanceDataFacade(api, candlestickRepository, onlySymbols, excludeSymbols, startSymbol);

                var symbols = binanceDataFacade.InitializeSymbols().Result;
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var symbolCount = 1;
                var totalSymbols = symbols.Count();
                foreach (var symbol in symbols)
                {
                    try
                    {
                        Log.Logger.ForContext("Action", "CrawlSymbol")
                            .Information("Begin crawling symbol {Symbol}", symbol.SymbolLabel);
                        binanceDataFacade.CrawlSymbol(symbol).Wait();                       
                    }
                    catch (Exception e)
                    {
                        Log.Logger.ForContext("Action", "CrawlSymbol")
                            .Error(e, "Error crawling symbol {Symbol}", symbol.SymbolLabel);
                    }
                    Log.Logger.ForContext("Action", "CrawlSymbol")
                        .Information("Finished crawling symbol {Symbol}. Total elapsed time: {ElapsedTimeTotal}. Symbol {SymbolCount} of {TotalSymbols}", symbol.SymbolLabel, stopwatch.Elapsed, symbolCount++, totalSymbols);
                }

                Log.Logger.ForContext("Action", "CrawlSymbol")
                    .Information("Finished crawling all symbols. Total elapsed time: {ElapsedTimeTotal}", stopwatch.Elapsed);

            }
            catch(Exception e)
            {
               Log.Logger.Fatal(e, "Uncaught exceptions. Program aborting.");               
            }          

            if(Environment.UserInteractive)
            {
                Console.Write("Press any key to quit: ");
                Console.ReadKey();
            }
            
        }

        
    }
}
