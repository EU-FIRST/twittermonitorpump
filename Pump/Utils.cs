/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    Utils.cs
 *  Desc:    Various utilities
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;
using System.Linq;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Data;
using System.Reflection;
using System.Globalization;
using System.Collections.Generic;
using Latino;
using Latino.TextMining;
using Latino.Model;

using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class Utils
       |
       '-----------------------------------------------------------------------
    */
    public static class Utils
    {            
        // Text mining utils

        public static IncrementalBowSpace CreateBowSpace(IEnumerable<string> taskStopWords)
        {
            IncrementalBowSpace bowSpc = new IncrementalBowSpace();
            RegexTokenizer tok = new RegexTokenizer();
            tok.TokenRegex = @"[#@$]?([\d_]*[\p{IsBasicLatin}\p{IsLatin-1Supplement}\p{IsLatinExtended-A}\p{IsLatinExtended-B}\p{IsLatinExtendedAdditional}-[^\p{L}]][\d_]*){2,}";
            tok.IgnoreUnknownTokens = true;
            bowSpc.Tokenizer = tok;
            Set<string> stopWords = new Set<string>(StopWords.EnglishStopWords);
            // additional stop words
            stopWords.AddRange("rt,can,will,must,just,got".Split(','));
            stopWords.AddRange("im,youre,hes,shes,its,were,theyre,ive,youve,weve,theyve,youd,hed,theyd,youll,theyll,isnt,arent,wasnt,werent,hasnt,havent,hadnt,doesnt,dont,didnt,wont,wouldnt,shant,shouldnt,cant,couldnt,mustnt,lets,thats,whos,whats,heres,theres,whens,wheres,whys,hows,i,m,you,re,he,s,she,it,we,they,ve,d,ll,isn,t,aren,wasn,weren,hasn,haven,hadn,doesn,don,didn,won,wouldn,shan,shouldn,can,couldn,mustn,let,that,who,what,here,there,when,where,why,how".Split(','));
            stopWords.AddRange(Config.AdditionalStopWords.ToLower().Split(','));
            if (taskStopWords != null) { stopWords.AddRange(taskStopWords.Select(x => x.ToLower())); }
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

        private static Regex mUrlRegex
            = new Regex(@"http://\S*", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static string RemoveUrls(string tweet)
        {
            return mUrlRegex.Replace(tweet, "");
        }

        // Database utils

        public static DataTable CreateClustersTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("TableId", typeof(Guid));
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("StartTime", typeof(DateTime));
            table.Columns.Add("EndTime", typeof(DateTime));
            table.Columns.Add("Topic", typeof(long));
            table.Columns.Add("NumDocs", typeof(int));
            table.Columns.Add("SentimentBasicPos", typeof(int));
            table.Columns.Add("SentimentBasicNeg", typeof(int));
            table.Columns.Add("SentimentBasicNeutral", typeof(int));
            table.Columns.Add("SentimentBasicPosLowCfd", typeof(int));
            table.Columns.Add("SentimentBasicNegLowCfd", typeof(int));
            table.Columns.Add("SentimentPos", typeof(int));
            table.Columns.Add("SentimentNeg", typeof(int));
            table.Columns.Add("SentimentNeutral", typeof(int));
            table.Columns.Add("SentimentPosLowCfd", typeof(int));
            table.Columns.Add("SentimentNegLowCfd", typeof(int));
            table.Columns.Add("SentimentHandLabeledPos", typeof(int));
            table.Columns.Add("SentimentHandLabeledNeg", typeof(int));
            table.Columns.Add("SentimentHandLabeledNeutral", typeof(int));
            table.Columns.Add("RecordState", typeof(int));
            return table;        
        }

        public static DataTable CreateTermsTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("TableId", typeof(Guid));
            table.Columns.Add("ClusterId", typeof(Guid));
            table.Columns.Add("EndTime", typeof(DateTime));
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
            table.Columns.Add("RecordState", typeof(int));
            return table;
        }

        public static DataTable CreateTweetsTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("TableId", typeof(Guid));
            table.Columns.Add("ClusterId", typeof(Guid));
            table.Columns.Add("EndTime", typeof(DateTime));
            table.Columns.Add("TweetId", typeof(long));
            table.Columns.Add("SentimentBasic", typeof(double));
            table.Columns.Add("SentimentBasicPos", typeof(bool));
            table.Columns.Add("SentimentBasicNeg", typeof(bool));
            table.Columns.Add("SentimentBasicNeutral", typeof(bool));
            table.Columns.Add("SentimentBasicPosLowCfd", typeof(bool));
            table.Columns.Add("SentimentBasicNegLowCfd", typeof(bool));
            table.Columns.Add("Sentiment", typeof(double));
            table.Columns.Add("SentimentPos", typeof(bool));
            table.Columns.Add("SentimentNeg", typeof(bool));
            table.Columns.Add("SentimentNeutral", typeof(bool));
            table.Columns.Add("SentimentPosLowCfd", typeof(bool));
            table.Columns.Add("SentimentNegLowCfd", typeof(bool));
            table.Columns.Add("Basic", typeof(bool));
            table.Columns.Add("HandLabeled", typeof(bool));
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

        public static void AssignParamsToCommand(SqlCommand command, params object[] args)
        {
            for (int i = 0; i < args.Length; i += 2)
            {
                object val = args[i + 1];
                SqlParameter param = new SqlParameter((string)args[i], val == null ? DBNull.Value : val);
                command.Parameters.Add(param);
            }
        }

        public static void ExecSqlScript(string name, params object[] cmdParams)
        {
            string sqlTxt = LUtils.GetManifestResourceString(typeof(Program), name);
            sqlTxt = Regex.Replace(sqlTxt, "^GO", "", RegexOptions.Multiline);
            using (SqlConnection connection = new SqlConnection(Config.OutputConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(sqlTxt, connection))
                {
                    AssignParamsToCommand(cmd, cmdParams);
                    cmd.CommandTimeout = Config.CommandTimeout;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        // Default culture hack

        public static void SetDefaultCulture(CultureInfo culture)
        {
            Type type = typeof(CultureInfo);
            try
            {
                type.InvokeMember("s_userDefaultCulture",
                    BindingFlags.SetField | BindingFlags.NonPublic | BindingFlags.Static,
                    null,
                    culture,
                    new object[] { culture });
                type.InvokeMember("s_userDefaultUICulture",
                    BindingFlags.SetField | BindingFlags.NonPublic | BindingFlags.Static,
                    null,
                    culture,
                    new object[] { culture });
                //Console.WriteLine("Success");
            }
            catch { }
            try
            {
                type.InvokeMember("m_userDefaultCulture",
                    BindingFlags.SetField | BindingFlags.NonPublic | BindingFlags.Static,
                    null,
                    culture,
                    new object[] { culture });
                type.InvokeMember("m_userDefaultUICulture",
                    BindingFlags.SetField | BindingFlags.NonPublic | BindingFlags.Static,
                    null,
                    culture,
                    new object[] { culture });
                //Console.WriteLine("Success");
            }
            catch { }
        }
    }
}
