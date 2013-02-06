using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Text;

namespace TwitterMonitorDAL
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "TwitterMonitor" in code, svc and config file together.
    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    public class TwitterMonitor
    {
        [Flags]
        public enum FilterFlag
        {
            TermUnigram = 1, 
            TermBigram = 2, 
            UserUnigram = 4, 
            HashtagUnigram = 8, 
            HashtagBigram = 16
        }
        
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public TagCloudElement[] TagCloud(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxVolume, string windowsSize, int filterFlag)
        {
            FilterFlag filterFlagEnum = (FilterFlag)filterFlag;
            if (maxVolume == 0) maxVolume = 100;
            if (windowsSize == null) windowsSize = "D";

            return DataProvider.GetDataWithReplace<TagCloudElement>(
                "TagCloud.sql", 
                new List<Tuple<string, string>>( new Tuple<string, string>[]
                    {
                        new Tuple<string, string>("/*REM*/", "--"), 
                        new Tuple<string, string>("--ADD", ""),
                        new Tuple<string, string>("--TOP NN", string.Format("TOP {0}", maxVolume)),
                        new Tuple<string, string>("[AAPL_D_Terms]",string.Format("[{0}_{1}_Terms]",entity, windowsSize)), 
                        new Tuple<string, string>("[AAPL_D_Clusters]",string.Format("[{0}_{1}_Clusters]",entity, windowsSize)), 
                    }),
                new object[]
                    {
                        (int) filterFlagEnum,
                        dateTimeStart,
                        dateTimeEnd
                    }).ToArray();

        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public string GetDataPost(int value)
        {
            return string.Format("You entered: {0}", value);
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public CompositeType GetDataComplex(int value)
        {
            CompositeType composite = new CompositeType();

            if (composite == null)
            {
                throw new ArgumentNullException("composite");
            }
            if (composite.BoolValue)
            {
                composite.StringValue += "Suffix";
            }
            return composite;
        }

        [OperationContract]
        public CompositeType GetDataUsingDataContract(CompositeType composite)
        {
            if (composite == null)
            {
                throw new ArgumentNullException("composite");
            }
            if (composite.BoolValue)
            {
                composite.StringValue += "Suffix";
            }
            return composite;
        }
    }

    [DataContract]
    public class TagCloudElement
    {
        [DataMember]
        private string Term { get; set; }
        [DataMember]
        private double Weight { get; set; }
    }

    // Use a data contract as illustrated in the sample below to add composite types to service operations.
    [DataContract]
    public class CompositeType
    {
        bool boolValue = true;
        string stringValue = "Hello ";

        [DataMember]
        public bool BoolValue
        {
            get { return boolValue; }
            set { boolValue = value; }
        }

        [DataMember]
        public string StringValue
        {
            get { return stringValue; }
            set { stringValue = value; }
        }
    }

}
