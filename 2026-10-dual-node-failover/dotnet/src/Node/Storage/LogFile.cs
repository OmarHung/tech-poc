using System.Text;
using System.Text.Json;
using Shared;

namespace Node.Storage;

/// <summary>
/// JSON-lines file behind <see cref="ReplicatedLog"/>. Every append is handed to the OS before it returns,
/// so a killed process loses nothing it acknowledged. Power loss is out of scope: there is no fsync.
/// </summary>
public sealed class LogFile : IDisposable
{
    private readonly string _path;
    private FileStream _stream;

    private LogFile(string path)
    {
        _path = path;
        _stream = OpenForAppend(path);
    }

    public static (LogFile File, List<LogEntry> Entries) Open(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        var entries = new List<LogEntry>();
        if (File.Exists(path))
        {
            var bytes = File.ReadAllBytes(path);
            // Every append ends in a newline, so a file that does not was cut off mid-write. Split leaves an empty
            // string after the final newline, which is not a line.
            var complete = bytes.Length == 0 || bytes[^1] == (byte)'\n';
            var lines = Encoding.UTF8.GetString(bytes).Split('\n');
            var count = complete ? lines.Length - 1 : lines.Length;
            for (var i = 0; i < count; i++)
            {
                if (TryParse(lines[i], out var entry) && entry.Index == entries.Count + 1)
                {
                    entries.Add(entry);
                    continue;
                }
                // A process killed mid-write leaves at most one partial line, and only at the end.
                if (i != count - 1)
                {
                    throw new InvalidDataException($"{path} line {i + 1} is corrupt");
                }
            }
            if (!complete || entries.Count != count)
            {
                // Rewrite without the partial line (or with the missing newline), so the next append starts clean.
                WriteAll(path, entries);
            }
        }
        return (new LogFile(path), entries);
    }

    public void Append(IReadOnlyList<LogEntry> entries)
    {
        var end = _stream.Position;
        try
        {
            _stream.Write(Serialize(entries));
        }
        catch
        {
            // Cut off whatever part made it to disk, so the next append does not land after half a line.
            TryTruncate(end);
            throw;
        }
    }

    /// <summary>Replaces the whole file. Used when a rejoining node drops entries the new primary never had.</summary>
    public void Rewrite(IReadOnlyList<LogEntry> entries)
    {
        _stream.Dispose();
        WriteAll(_path, entries);
        _stream = OpenForAppend(_path);
    }

    public void Dispose() => _stream.Dispose();

    /// <summary>Unbuffered: a failed write leaves nothing queued in .NET that a later write would flush out of order.</summary>
    private static FileStream OpenForAppend(string path)
    {
        var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, bufferSize: 0);
        stream.Seek(0, SeekOrigin.End);
        return stream;
    }

    private void TryTruncate(long length)
    {
        try
        {
            _stream.SetLength(length);
            _stream.Position = length;
        }
        catch (IOException)
        {
            // The write error is what the caller needs to see; a failing disk will fail the next write too.
        }
    }

    private static void WriteAll(string path, IReadOnlyList<LogEntry> entries)
    {
        var temp = path + ".tmp";
        File.WriteAllBytes(temp, Serialize(entries));
        File.Move(temp, path, overwrite: true);
    }

    private static byte[] Serialize(IReadOnlyList<LogEntry> entries)
    {
        var text = new StringBuilder();
        foreach (var entry in entries)
        {
            text.Append(JsonSerializer.Serialize(entry, Wire.Json)).Append('\n');
        }
        return Encoding.UTF8.GetBytes(text.ToString());
    }

    private static bool TryParse(string line, out LogEntry entry)
    {
        entry = null!;
        try
        {
            var parsed = JsonSerializer.Deserialize<LogEntry>(line, Wire.Json);
            if (parsed is not { Key: not null, Value: not null })
            {
                return false;
            }
            entry = parsed;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
