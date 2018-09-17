using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Serilog.Sinks.PostgreSQL;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace BinanceAPITest.Core.Logging
{
    public static class ColumnConfiguration
    {               
        public static IDictionary<string, ColumnWriterBase> GetColumnConfiguration()
        {
            return new Dictionary<string, ColumnWriterBase>
            {
                {"message", new RenderedMessageColumnWriter(NpgsqlDbType.Text) },               
                {"level", new LevelColumnWriter(true, NpgsqlDbType.Varchar) },
                {"raise_date", new TimestampColumnWriter(NpgsqlDbType.Timestamp) },
                {"exception", new ExceptionColumnWriter(NpgsqlDbType.Text) },              
                {"props_test", new PropertiesColumnWriter(NpgsqlDbType.Jsonb) },
                {"properties", new PropertiesColumnWriter(NpgsqlDbType.Jsonb) },
                {"machine_name", new SinglePropertyColumnWriter("MachineName", PropertyWriteMethod.ToString, NpgsqlDbType.Text, "l") },
                {"source", new SinglePropertyColumnWriter("Source", PropertyWriteMethod.ToString, NpgsqlDbType.Text, "l") },
                {"environment_user_name", new SinglePropertyColumnWriter("EnvironmentUserName", PropertyWriteMethod.ToString, NpgsqlDbType.Text, "l") }
            };
        }
      
    }

   
}
