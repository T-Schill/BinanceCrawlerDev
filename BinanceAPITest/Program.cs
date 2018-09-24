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
            Log.Logger = new LoggerConfiguration()
                   .MinimumLevel.Verbose()
                   .Enrich.WithMachineName()
                   .Enrich.WithEnvironmentUserName()
                   .Enrich.WithProperty("Source", "FinaScope BinanceCrawler 0.1")  
                   .WriteTo.Console()
                   .WriteTo.PostgreSQL(connectionString, "logs", ColumnConfiguration.GetColumnConfiguration(), needAutoCreateTable: true)
                   .CreateLogger();

            try
            {
                ServiceProvider = new ServiceCollection()
                    .AddBinance()
                    .AddOptions()
                    .Configure<BinanceApiOptions>(Configuration.GetSection("ApiOptions"))
                    .AddLogging(builder => builder.AddSerilog(Log.Logger))
                    .BuildServiceProvider();

                var maintenceWindowHours = Convert.ToInt32(Configuration.GetSection("CrawlOptions:MaintenanceWindowHours").Value);

                var api = ServiceProvider.GetService<IBinanceApi>();
                var candlestickRepository = new CandlestickRepository(connectionString);
                var binanceDataFacade = new BinanceDataFacade(maintenceWindowHours, api, candlestickRepository);

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
