using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.IO;
using System.Data;
using System.Reflection;
using System.Resources;
using Littlefish.SQLiteSessionStateProvider.Properties;
using System.Web;

namespace Littlefish.SQLiteSessionStateProvider
{
    class SchemaGenerator
    {
        private string _databaseFile;

        public SchemaGenerator(string databaseFile)
        {
            _databaseFile = databaseFile;
        }

        public void Create()
        {
            if (File.Exists(_databaseFile)) return;

            SQLiteConnection.CreateFile(_databaseFile);


            using (var connection = new SQLiteConnection("Data Source=" + _databaseFile))
            {
                connection.Open();

                var cmd = connection.CreateCommand();
                cmd.CommandText = Resources.InstallSessionSchema;
                cmd.CommandType = CommandType.Text;

                cmd.ExecuteNonQuery();
            }
        }
    }
}
