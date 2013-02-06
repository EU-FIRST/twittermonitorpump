using Latino;
using Latino.TextMining;

namespace TwitterMonitorPump
{
    public class TwitterLemmatizer : IStemmer
    {
        private IStemmer mLemmatizer
            = new Lemmatizer(Language.English);

        //public MyLemmatizer()
        //{ 
        //}

        //public MyLemmatizer(BinarySerializer reader)
        //{
        //}

        public string GetStem(string word)
        {
            string lemma = mLemmatizer.GetStem(word).Trim();
            if (lemma == "") { return word; }
            return lemma;
        }

        public void Save(BinarySerializer writer)
        {
        }
    }
}
