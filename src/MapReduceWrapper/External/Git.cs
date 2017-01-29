namespace MapReduceWrapper.External
{
    public class Git : External
    {
        public Git() : base("git")
        {
        }

        public void Clone(string cloneUrl = "https://github.com/sonotude/HaskellMapReduce.git", string path = ".")
        {
            Run(new []{"clone", cloneUrl, path});
        }
    }
}
