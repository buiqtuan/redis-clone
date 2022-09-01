namespace RedisCloneNS;

public class RedisClient
{
    public readonly TcpClient? Tcp;

    public readonly StreamReader? Reader;

    public readonly StreamWriter? Writer;

    public readonly SharedDictionary Dic;

    public struct Command
    {
        public string Key;

        public string? Value;

        public bool Completed;
    }

    private List<string> _args = new();

    private Task<string?> _nextLine;

    private Command[] _commands = Array.Empty<Command>();

    private int _commandsLength = 0;

    private StringBuilder _buffer = new();

    private int _shardFactor;

    public RedisClient
    (
        TcpClient tcp,
        StreamReader reader,
        StreamWriter writer,
        SharedDictionary dic
    )
    {
        Tcp = tcp;
        Reader = reader;
        Writer = writer;
        Dic = dic;
        _shardFactor = dic.Factor;
    }

    public async Task ReadAsync()
    {
        try
        {
            while (true)
            {
                if (_buffer.Length > 0)
                {
                    await Writer!.WriteAsync(_buffer);
                    _buffer.Clear();
                }
                var lineTask = _nextLine ?? Reader!.ReadLineAsync();
                if (lineTask.IsCompleted is false)
                {
                    if (_commandsLength > 0)
                    {
                        _nextLine = lineTask;
                        Dic.Enqueue(this, Math.Abs(_commands[0].Key.GetHashCode()) % _shardFactor);
                        return;
                    }
                }
                var line = await lineTask;
                _nextLine!.Dispose();
                if (line == null)
                {
                    using (Tcp)
                    {
                        return;
                    }
                }
                await ReadCommand(line);

                AddCommand();
            }
        }
        catch (Exception e)
        {
            await this.HandleError(e);
        }
    }

    public async Task ReadCommand(string? line)
    {
        _args.Clear();
        if (line![0] != '*')
            throw new InvalidDataException("Cannot understand arg batch: " + line);
        var argsv = int.Parse(line.Substring(1));
        for (int i = 0; i < argsv; i++)
        {
            line = await Reader!.ReadLineAsync() ?? string.Empty;
            if (line[0] != '$')
                throw new InvalidDataException("Cannot understand arg length: " + line);
            var argLen = int.Parse(line.Substring(1));
            line = await Reader.ReadLineAsync() ?? string.Empty;
            if (line.Length != argLen)
                throw new InvalidDataException("Wrong arg length expected " + argLen + " got: " + line);

            _args.Add(line);
        }
    }

    private void AddCommand()
    {
        if (_commandsLength >= _commands.Length)
        {
            Array.Resize(ref _commands, _commands.Length + 8);
        }
        ref Command cmd = ref _commands[_commandsLength++];
        cmd.Completed = false;
        switch (_args[0])
        {
            case "GET":
                cmd.Key = _args[1];
                cmd.Value = null;
                break;
            case "SET":
                cmd.Key = _args[1];
                cmd.Value = _args[2];
                break;
            default:
                throw new ArgumentOutOfRangeException($"Unknow command: {_args[0]}");
        }
    }

    public async Task NextAsync()
    {
        try
        {
            WriteToBuffer();

            await ReadAsync();
        }
        catch (Exception e)
        {
            await this.HandleError(e);
        }
    }

    private void WriteToBuffer()
    {
        for (int i = 0;i < _commandsLength;i++)
        {
            ref Command cmd = ref _commands[i];
            if (cmd.Value is null)
            {
                _buffer.Append("$-1\r\n");
            }
            else
            {
                _buffer.Append($"{cmd.Value.Length}\r\n{cmd.Value}\r\n");
            }
        }
        _commandsLength = 0;
    }

    private async Task HandleError(Exception e)
    {
        using (Tcp)
        {
            try
            {
                string? line;
                var errReader = new StringReader(e.ToString());
                while ((line = errReader.ReadLine()) is not null)
                {
                    await Writer!.WriteAsync("-");
                    await Writer.WriteLineAsync(line);
                }
                await Writer!.FlushAsync();
            }
            catch (Exception)
            {
                //ignore
            }
        }
    }

    public void Execute(Dictionary<string, string> localDic, int index)
    {
        int? next = null;
        for (int i = 0;i < _commandsLength;i++)
        {
            ref var cmd = ref _commands[i];
            var cur = Math.Abs(cmd.Key.GetHashCode() % _shardFactor);
            if (cur == index)
            {
                cmd.Completed = true;
                if (cmd.Value is not null)
                {
                    localDic[cmd.Key] = cmd.Value;
                }
                else
                {
                    localDic.TryGetValue(cmd.Key, out cmd.Value);
                }
            }
            else if (cmd.Completed is false)
            {
                next = cur;
            }
        }
        if (next is not null) 
        {
            Dic.Enqueue(this, next.Value);
        }
        else
        {
            _ = NextAsync();
        }
    }
}