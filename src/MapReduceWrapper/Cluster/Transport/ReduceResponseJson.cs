using System.Collections.Generic;

namespace MapReduceWrapper.Cluster.Transport
{
    public class ReduceResponseJson
    {
        public List<ReduceResponseJsonItem> Results { get; set; }
    }
    public class ReduceResponseJsonItem
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}
