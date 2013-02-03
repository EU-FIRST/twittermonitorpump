using System;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Data;
using Latino;
using Latino.TextMining;
using Latino.Model;

namespace BagOfWordsTest
{
    public class Utils
    {
        private static Regex mUrlRegex
            = new Regex(@"http://\S*", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            
        public static void AssignParamsToCommand(SqlCommand command, params object[] args)
        {
            for (int i = 0; i < args.Length; i += 2)
            {
                object val = args[i + 1];
                SqlParameter param = new SqlParameter((string)args[i], val == null ? DBNull.Value : val);
                command.Parameters.Add(param);
            }
        }

        public static IncrementalBowSpace CreateBowSpace()
        {
            IncrementalBowSpace bowSpc = new IncrementalBowSpace();
            RegexTokenizer tok = new RegexTokenizer();
            tok.TokenRegex = @"[#@$]?([\d_]*\p{L}[\d_]*){2,}";
            tok.IgnoreUnknownTokens = true;
            bowSpc.Tokenizer = tok;
            Set<string> stopWords = new Set<string>(StopWords.EnglishStopWords);
            // additional stop words
            stopWords.AddRange("i,m,you,re,he,s,she,it,we,they,ve,d,ll,isn,t,aren,wasn,weren,hasn,haven,hadn,doesn,don,didn,wouldn,shan,shouldn,couldn,mustn,let,that,who,what,here,there,when,where,why,how".Split(','));
            stopWords.Add("rt");
            bowSpc.Stemmer = new TwitterLemmatizer();
            bowSpc.StopWords = stopWords;
            bowSpc.MaxNGramLen = 2;
            return bowSpc;
        }

        public static IncrementalKMeansClustering CreateClustering()
        {
            IncrementalKMeansClustering clustering = new IncrementalKMeansClustering();
            clustering.QualThresh = Convert.ToDouble(Latino.Utils.GetConfigValue("ClusterQualityThresh", "0.2"));
            return clustering;
        }

        public static DataTable CreateClustersTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("StartTime", typeof(DateTime));
            table.Columns.Add("EndTime", typeof(DateTime));
            table.Columns.Add("Topic", typeof(long));
            table.Columns.Add("NumDocs", typeof(int));
            return table;        
        }

        public static DataTable CreateTermsTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("ClusterId", typeof(Guid));
            table.Columns.Add("Stem", typeof(string));
            table.Columns.Add("MostFrequentForm", typeof(string));
            table.Columns.Add("TF", typeof(int));
            table.Columns.Add("D", typeof(int));
            table.Columns.Add("TFIDF", typeof(double));
            table.Columns.Add("[User]", typeof(bool));
            table.Columns.Add("Hashtag", typeof(bool));
            table.Columns.Add("Stock", typeof(bool));
            table.Columns.Add("NGram", typeof(bool));
            return table;
        }

        public static T Cast<T>(object obj)
        {
            if (obj is DBNull) { return default(T); }
            return (T)obj;
        }

        public static T GetVal<T>(SqlDataReader reader, string colName)
        {
            return Cast<T>(reader.GetValue(reader.GetOrdinal(colName)));
        }

        public static string RemoveUrls(string tweet)
        {
            return mUrlRegex.Replace(tweet, "");
        }
    }
}
