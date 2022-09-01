namespace RedisCloneNS;

public class SharedDictionary
{
    Dictionary<string, string>[] _dics;

    BlockingCollection<RedisClient>[] _workers;

    public int Factor => _dics.Length;

    public SharedDictionary(int shardingFactor)
    {
        _dics = new Dictionary<string, string>[shardingFactor];
        _workers = new BlockingCollection<RedisClient>[shardingFactor];

        for (var i=0;i < shardingFactor;i++)
        {
            var dic = new Dictionary<string, string>();
            var workder = new BlockingCollection<RedisClient>();
            _dics[i] = dic;
            _workers[i] = workder;
            //readers
            new Thread(() => 
            {
                ExecWorker(dic, index: i, workder);
            })
            {
                IsBackground = true
            }.Start();
        }
    }

    private void ExecWorker(Dictionary<string, string> dic, int index, BlockingCollection<RedisClient> worker)
    {
        while (true)
        {
            worker.Take().Execute(dic, index);
        }
    }

    public void Enqueue(RedisClient rc, int index)
    {
        _workers[index].Add(rc);
    }
}