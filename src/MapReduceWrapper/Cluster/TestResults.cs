using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;

namespace MapReduceWrapper.Cluster
{
    public class TestResults : ReadOnlyDictionary<IPAddress, bool>
    {
        internal TestResults(IDictionary<IPAddress, bool> dictionary) : base(dictionary)
        {
        }
    }
}
