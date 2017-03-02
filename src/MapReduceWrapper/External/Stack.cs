namespace MapReduceWrapper.External
{
    public class Stack : External
    {
        public Stack() : base("stack")
        {

        }

        public void Build()
        {
            Run(new[] {"build"});
        }

        public void Setup()
        {
            Run(new[] {"setup"});
        }

        public void Install(string path = ".")
        {
            Run(new[] {"install", "--local-bin-path", path});
        }
    }
}
