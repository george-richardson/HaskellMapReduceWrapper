using System;

namespace MapReduceWrapper.Manifest.Exceptions
{
    public class MalformedManifestException : Exception
    {
        public MalformedManifestException() : base("Manifest json is malformed.")
        {
            
        }
    }
}
