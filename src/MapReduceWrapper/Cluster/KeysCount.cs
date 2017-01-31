using System.Collections.Generic;

namespace MapReduceWrapper.Cluster
{
    public class KeysCount
    {
        public List<string> Keys { get; } = new List<string>();
        public int TotalCount { get; private set; }

        public void Add(string key, int count)
        {
            Keys.Add(key);
            TotalCount += count;
        }
    }
}
