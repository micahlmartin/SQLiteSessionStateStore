using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SQLite;

namespace Littlefish.SQLiteSessionStateProvider
{
    static class SQLiteHelper
    {
        public static IDbDataParameter CreateParameter(string name, DbType type, object value)
        {
            return new SQLiteParameter
            {
                ParameterName = name,
                DbType = type,
                Value = value
            };        
        }
        public static IDbDataParameter CreateParameter(string name, DbType type, int size, object value)
        {
            return new SQLiteParameter
            {
                ParameterName = name,
                DbType = type,
                Size = size,
                Value = value
            };
        }
    }
}
