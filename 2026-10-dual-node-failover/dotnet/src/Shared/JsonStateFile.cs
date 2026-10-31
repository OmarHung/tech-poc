using System.Text.Json;

namespace Shared;

/// <summary>Small JSON state files that survive a killed process.</summary>
public static class JsonStateFile
{
    public static T? Load<T>(string path)
        where T : class
    {
        if (!File.Exists(path))
        {
            return null;
        }
        return JsonSerializer.Deserialize<T>(File.ReadAllBytes(path), Wire.Json)
            ?? throw new InvalidDataException($"{path} is empty");
    }

    /// <summary>Writes a temp file and renames it over the old one, so a crash leaves the old or the new state, never a mix.</summary>
    public static void Save<T>(string path, T value)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        var temp = path + ".tmp";
        File.WriteAllBytes(temp, JsonSerializer.SerializeToUtf8Bytes(value, Wire.Json));
        File.Move(temp, path, overwrite: true);
    }
}
