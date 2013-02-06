using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Configuration;
using System.Configuration;

namespace TwitterMonitorDAL
{
    public static class DataProvider
    {
        public static string GetConnectionString(string connectionStringId = "default")
        {
            var conSett = ConfigurationManager.ConnectionStrings[connectionStringId];
            if (conSett == null) { return null; }
            else { return conSett.ConnectionString; }
        }
        public static string GetManifestResourceString(string resName)
        {
            string fullResName = Assembly.GetExecutingAssembly().GetManifestResourceNames().FirstOrDefault(res => res.EndsWith(resName));
            if (fullResName == null) throw new Exception(string.Format("The specified resource name does not exist!"));
            Stream resStream = Assembly.GetExecutingAssembly().GetManifestResourceStream(fullResName);
            if (resStream == null) return null;
            return new StreamReader(resStream).ReadToEnd();
        }

        public static List<T> GetDataSql<T>(string sqlQuery, object[] sqlParams = null, string connectionStringId = "default")
        {
            using (var db = new DataContext(GetConnectionString(connectionStringId)))
            {
                return db.ExecuteQuery<T>(sqlQuery, sqlParams ?? new object[] { }).ToList();
            }
        }

        public static List<T> GetData<T>(string sqlQueryResFile, object[] sqlParams = null, string connectionStringId = "default")
        {
            string sql = GetManifestResourceString(sqlQueryResFile);
            using (var db = new DataContext(GetConnectionString(connectionStringId)))
            {
                return db.ExecuteQuery<T>(sql, sqlParams ?? new object[] { }).ToList();
            }
        }
        public static List<T> GetDataWithReplace<T>(string sqlQueryResFile, List<Tuple<string, string>> replacements = null, object[] sqlParams = null, string connectionStringId = "default")
        {
            string sql = GetManifestResourceString(sqlQueryResFile);
            if (replacements != null)
            {
                foreach (Tuple<string, string> replacement in replacements)
                {
                    sql = sql.Replace(replacement.Item1, replacement.Item2);
                }
            }

            using (var db = new DataContext(GetConnectionString(connectionStringId)))
            {
                return db.ExecuteQuery<T>(sql, sqlParams ?? new object[] { }).ToList();
            }
        }
    }
}