using Serilog;
using Serilog.Core;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace BinanceAPITest.Core.Logging
{
    public static class SerilogConfiguration
    {
        public static LoggingLevelSwitch LoggingLevel { get; set; }

        static SerilogConfiguration()
        {
            LoggingLevel = new LoggingLevelSwitch();
            LoggingLevel.MinimumLevel = LogEventLevel.Information;
        }

        public static void SetLoggingLevel(string level)
        {
            LogEventLevel result;
            if(Enum.TryParse(level, out result))
            {
                LoggingLevel.MinimumLevel = result;
            }
            Log.Logger.Information("Logging level is set to {LoggingLevel}", LoggingLevel.MinimumLevel);
        }
    }
}
