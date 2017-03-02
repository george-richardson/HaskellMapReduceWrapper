using System.Collections.Generic;
using System.Collections.ObjectModel;
using MapReduceWrapper.Manifest;

namespace MapReduceWrapper.Cluster
{
    public class TestResults : ReadOnlyDictionary<Node, bool>
    {
        internal TestResults(IDictionary<Node, bool> dictionary) : base(dictionary)
        {
        }
    }
}
