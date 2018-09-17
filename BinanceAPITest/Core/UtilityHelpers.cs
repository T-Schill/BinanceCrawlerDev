using System;
using System.Collections.Generic;
using System.Text;

namespace BinanceAPITest.Core
{
    public static class UtilityHelpers
    {
        public static string PrettyBytes(this long size)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };            
            int order = 0;
            while (size >= 1024 && order < sizes.Length - 1)
            {
                order++;
                size = size / 1024;
            }
            
            return String.Format("{0:0.##} {1}", size, sizes[order]);
        }
    }
}
