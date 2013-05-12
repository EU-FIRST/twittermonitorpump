using System;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Data;
using Latino;
using Latino.TextMining;
using Latino.Model;

using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    public class Utils
    {
        public static class Config
        {
            public static readonly TimeSpan SaveStateTimeDiff
                = TimeSpan.Parse(LUtils.GetConfigValue("SaveStateTimeDiff", "10:00:00"));
            public static readonly int CommandTimeout
                = Convert.ToInt32(LUtils.GetConfigValue("CommandTimeout", "0"));
            public static readonly string InputConnectionString
                = LUtils.GetConfigValue("InputConnectionString");
            public static readonly string OutputConnectionString
                = LUtils.GetConfigValue("OutputConnectionString");
        }

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
            tok.TokenRegex = @"[#@$]?([\d_]*[\p{IsBasicLatin}\p{IsLatin-1Supplement}\p{IsLatinExtended-A}\p{IsLatinExtended-B}\p{IsLatinExtendedAdditional}-[^\p{L}]][\d_]*){2,}";
            tok.IgnoreUnknownTokens = true;
            bowSpc.Tokenizer = tok;
            Set<string> stopWords = new Set<string>(StopWords.EnglishStopWords);
            // additional stop words
            stopWords.AddRange("can,will,must".Split(','));
            stopWords.AddRange("im,youre,hes,shes,its,were,theyre,ive,youve,weve,theyve,youd,hed,theyd,youll,theyll,isnt,arent,wasnt,werent,hasnt,havent,hadnt,doesnt,dont,didnt,wont,wouldnt,shant,shouldnt,cant,couldnt,mustnt,lets,thats,whos,whats,heres,theres,whens,wheres,whys,hows,i,m,you,re,he,s,she,it,we,they,ve,d,ll,isn,t,aren,wasn,weren,hasn,haven,hadn,doesn,don,didn,won,wouldn,shan,shouldn,can,couldn,mustn,let,that,who,what,here,there,when,where,why,how".Split(','));
            stopWords.Add("rt");
            bowSpc.Stemmer = new TwitterLemmatizer();
            bowSpc.StopWords = stopWords;
            bowSpc.MaxNGramLen = 2;
            return bowSpc;
        }

        public static IncrementalKMeansClustering CreateClustering(double qualThresh)
        {
            IncrementalKMeansClustering clustering = new IncrementalKMeansClustering();
            clustering.Random = new Random(1);
            clustering.QualThresh = qualThresh;
            return clustering;
        }

        public static DataTable CreateClustersTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("TableId", typeof(Guid));
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("StartTime", typeof(DateTime));
            table.Columns.Add("EndTime", typeof(DateTime));
            table.Columns.Add("Topic", typeof(long));
            table.Columns.Add("NumDocs", typeof(int));
            table.Columns.Add("RecordState", typeof(int));
            return table;        
        }

        public static DataTable CreateTermsTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("TableId", typeof(Guid));
            table.Columns.Add("ClusterId", typeof(Guid));
            table.Columns.Add("StemHash", typeof(Guid));
            table.Columns.Add("Stem", typeof(string));
            table.Columns.Add("MostFrequentForm", typeof(string));
            table.Columns.Add("TF", typeof(int));
            table.Columns.Add("D", typeof(int));
            table.Columns.Add("TFIDF", typeof(double));
            table.Columns.Add("[User]", typeof(bool));
            table.Columns.Add("Hashtag", typeof(bool));
            table.Columns.Add("Stock", typeof(bool));
            table.Columns.Add("NGram", typeof(bool));
            table.Columns.Add("Tagged", typeof(bool));
            table.Columns.Add("EndTime", typeof(DateTime));
            table.Columns.Add("RecordState", typeof(int));
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
