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
                var database = new DatabaseRepository(connectionString);  

                var startDateData = database.GetStartDate(Symbol.ETH_BTC).Value;
                var startDate = new DateTime(startDateData.Ticks, DateTimeKind.Utc);
                var endDate = startDate.AddMinutes(999);
                var iterations = 20;
                var pageSize = 1000;
                var symbol = Symbol.ETH_USDT;
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (var i = 0; i < iterations; i++)
                {
                    var candlesticks = api.GetCandlesticksAsync(symbol, CandlestickInterval.Minute, startDate, endDate, pageSize).Result;

                    //foreach (var candlestick in candlesticks)
                    //{
                    //    database.Insert(candlestick);
                    //}
                    database.InsertBulk(candlesticks);
                    Log.Logger.ForContext("Action", "LogCandlestickBulk")
                          .Verbose("Logged Candlesticks for Symbol {Symbol} for range {StartTime} to {EndTime}", symbol, startDate, endDate);

                    var metrics = database.GetTableMetrics("klines");

                    Log.Logger.ForContext("Action", "KlinesMetrics")
                        .Debug($"Klines table size {{Size}} ({metrics.Size.PrettyBytes()}). External size {{ExternalSize}} ({metrics.ExternalSize.PrettyBytes()})", metrics.Size, metrics.ExternalSize);

                    Log.Logger.ForContext("Action", "LogCandlestickBulk")
                        .Debug("Elapsed time {ElapsedMilliseconds}. Total records parsed {TotalRecords}", stopwatch.ElapsedMilliseconds, i * pageSize);

                    startDate = endDate;
                    endDate = startDate.AddMinutes(999);

                }

               

               

                //var interval = startDate;
                
                //while(interval < endDate)
                //{
                //    var loggedCandlestick = database.Get(Symbol.ETH_BTC, interval);
                //    if(loggedCandlestick != null)
                //    {
                //        Log.Logger.ForContext("Action", "LogCandlestick")
                //            .Verbose("Logged Candlestick for Symbol {Symbol} for open time {OpenTime}", loggedCandlestick.Symbol, loggedCandlestick.OpenTime);
                //        interval = interval.AddMinutes(1);
                //    }
                  
                //}

            }
            catch(Exception e)
            {
               Log.Logger.Fatal(e, "Uncaught exceptions. Program aborting.");               
            }
            //var api = new BinanceApi();

            //var data = api.GetCandlesticksAsync(Symbol.ETC_BTC, CandlestickInterval.Minute, (1510617600000, 1510646880000)).Result;

            //var lastKline = data.FirstOrDefault();
            //var epochTime = new DateTimeOffset(lastKline.OpenTime, TimeSpan.Zero);
            //Console.WriteLine(epochTime.ToUnixTimeMilliseconds());

            //Console.WriteLine(JsonConvert.SerializeObject(data, Formatting.Indented));

            if(Environment.UserInteractive)
            {
                Console.Write("Press any key to quit: ");
                Console.ReadKey();
            }
            
        }

        
    }
}
