var tcpListenser = new TcpListener(System.Net.IPAddress.Any, 6379);
tcpListenser.Start();

Console.WriteLine("Listening on port: 6379 ...");

SharedDictionary _state = new(Environment.ProcessorCount / 2);

while (true)
{
    var tcp = await tcpListenser.AcceptTcpClientAsync();
    var stream = tcp.GetStream();
    var client = new RedisClient(tcp, new StreamReader(stream), new StreamWriter(stream)
    {
        AutoFlush = true
    }, _state);
    var _ = client.ReadAsync();
}