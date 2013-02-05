using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;

namespace TwitterMonitorDAL
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "TwitterMonitor" in code, svc and config file together.
    public class TwitterMonitor : ITwitterMonitor
    {
        public string GetData(int value)
        {
            return string.Format("You entered: {0}", value);
        }

        public string GetDataPost(int value)
        {
            return string.Format("You entered: {0}", value);
        }

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
}
