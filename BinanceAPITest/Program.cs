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

                var api = ServiceProvider.GetService<IBinanceApi>();
                var candlestickRepository = new CandlestickRepository(connectionString);
                var binanceDataFacade = new BinanceDataFacade(api, candlestickRepository);

                var symbols = binanceDataFacade.InitializeSymbols().Result;
                foreach(var symbol in symbols)
                {
                    try
                    {
                        binanceDataFacade.CrawlSymbol(symbol).Wait();
                    }
                    catch(Exception e)
                    {
                        Log.Logger.Error(e, "Error crawling symbol '{Symbol}'", symbol);
                    }                    
                }

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
