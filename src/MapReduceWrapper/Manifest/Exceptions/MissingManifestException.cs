using System;

namespace MapReduceWrapper.Manifest.Exceptions
{
    public class MissingManifestException : Exception
    {
        public MissingManifestException() : base("Manifest file was not found")
        {
            
        }
    }
}
