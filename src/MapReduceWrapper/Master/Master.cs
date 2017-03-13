namespace MapReduceWrapper.Master
{
    public static class Master
    {
        public static void Run(string path)
        {
            Configuration.BuildInstall();
            Exec(path);
        }

        public static void Exec(string path)
        {
            Cluster.Cluster cluster = new Cluster.Cluster();
            cluster.LoadProgram("HaskellMapReduce-exe");
            cluster.ExecuteProgram(path);
        }
    }
}
