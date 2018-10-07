using Binance;
using System;
using System.Collections.Generic;
using System.Text;

namespace BinanceAPITest.Core.Models
{
    public class CandleSticksBatch
    {
        public IEnumerable<Candlestick> CandleSticks { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }
}
