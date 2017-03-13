using System;

namespace MapReduceWrapper.Cluster.Exceptions
{
    public class MissingManifestException : Exception
    {
        public MissingManifestException() : base("Manifest file was not found")
        {
            
        }
    }
}
