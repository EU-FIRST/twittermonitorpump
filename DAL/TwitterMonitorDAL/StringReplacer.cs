using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace TwitterMonitorDAL
{
    public class StringReplacer : List<Tuple<string, string>>
    {
        public void AddReplacement(string replacee, string replacement)
        {
            Add(new Tuple<string, string>(replacee, replacement));
        }

        public string PerformReplace(string source)
        {
            return this.Aggregate(source, (current, replacement) => current.Replace(replacement.Item1, replacement.Item2));
        }
    }
}