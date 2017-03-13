using System;

namespace MapReduceWrapper.Cluster.Exceptions
{
    public class MalformedManifestException : Exception
    {
        public MalformedManifestException() : base("Manifest json is malformed.")
        {
            
        }
    }
}
