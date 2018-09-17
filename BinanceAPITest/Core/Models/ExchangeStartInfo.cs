using Binance;
using System;
using System.Collections.Generic;
using System.Text;

namespace BinanceAPITest.Core.Models
{
    public class ExchangeStartInfo
    {
        public Symbol Symbol { get; set; }
        public string SymbolLabel { get; set; }
        public DateTime StartDate { get; set; }
    }
}
