using System.Globalization;

namespace LoadTest.Client;

/// <summary>Minimal "--name value" / "--flag" argument parser.</summary>
internal sealed class CommandLine
{
    private readonly Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase);

    public CommandLine(IEnumerable<string> args)
    {
        var list = args.ToList();
        for (var i = 0; i < list.Count; i++)
        {
            if (!list[i].StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            var name = list[i][2..];
            if (i + 1 < list.Count && !list[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                values[name] = list[++i];
            }
            else
            {
                values[name] = null;
            }
        }
    }

    public bool Has(string name) => values.ContainsKey(name);

    public string? GetString(string name, string? defaultValue = null)
        => values.TryGetValue(name, out var value) && value is not null ? value : defaultValue;

    public int GetInt(string name, int defaultValue)
        => GetString(name) is { } value ? int.Parse(value, CultureInfo.InvariantCulture) : defaultValue;

    public double GetDouble(string name, double defaultValue)
        => GetString(name) is { } value ? double.Parse(value, CultureInfo.InvariantCulture) : defaultValue;
}
