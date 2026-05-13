using Dapper.Contrib.Extensions;

namespace TownSuite.DapperExtras.Tests;

/// <summary>
/// Represents a table where the upsert match key is a composite of non-[Key] columns
/// (e.g. Metric + ServerMac). This is the model class for the bug-report scenario where
/// both WHERE columns appear in the full entity, causing duplicate column names in the
/// generated MERGE INSERT list before the fix.
/// </summary>
[Dapper.Contrib.Extensions.Table("Dashboard.metrics")]
public class MetricsTable
{
    public string Metric { get; set; }

    public string ServerMac { get; set; }

    public int Count { get; set; }

    public DateTime TimeCreated { get; set; }
}

