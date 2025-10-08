using Parquet;
using System.Text;
using System.Linq;

namespace ParquetReader;

class Program
{
    static int Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  ParquetReader <path-to-file-or-folder>");
            Console.WriteLine("Example:");
            Console.WriteLine("  ParquetReader D:\\BIGPOPA\\ifs_autotune\\desktop\\output");
            return 1;
        }

        string path = args[0];
        if (File.Exists(path))
        {
            ConvertSingle(path);
        }
        else if (Directory.Exists(path))
        {
            foreach (var file in Directory.GetFiles(path, "*.parquet"))
            {
                ConvertSingle(file);
            }
        }
        else
        {
            Console.WriteLine($"❌ Path not found: {path}");
            return 1;
        }

        return 0;
    }

    private static void ConvertSingle(string parquetFile)
    {
        string csvPath = Path.ChangeExtension(parquetFile, ".csv");
        try
        {
            using Stream fs = File.OpenRead(parquetFile);
            using var reader = new ParquetReader(fs);
            var ds = reader.ReadAsTable();

            using var writer = new StreamWriter(csvPath, false, Encoding.UTF8);

            var columns = ds.Schema.Fields.Select(f => f.Name).ToArray();
            writer.WriteLine(string.Join(",", columns));

            foreach (var row in ds)
            {
                var line = string.Join(",", row.Select(v =>
                {
                    if (v == null) return string.Empty;
                    var s = v.ToString();
                    if (s is null) return string.Empty;
                    if (s.Contains(',') || s.Contains('"'))
                    {
                        s = "\"" + s.Replace("\"", "\"\"") + "\"";
                    }
                    return s;
                }));
                writer.WriteLine(line);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to convert {parquetFile}: {ex.Message}");
            return;
        }

        Console.WriteLine($"✅ Converted: {Path.GetFileName(parquetFile)} → {Path.GetFileName(csvPath)}");
    }
}
