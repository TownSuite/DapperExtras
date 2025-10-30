using Dapper.Contrib.Extensions;

namespace TownSuite.DapperExtras.Tests;

[Dapper.Contrib.Extensions.Table("MySchema.ExampleTable2")]
public class ExampleTable2
{
    [Dapper.Contrib.Extensions.Key()]
    public long Id { get; set; }

    public string Col1 { get; set; }

    public string Col2 { get; set; }

    public DateTime Col3 { get; set; }
    
    [Computed]
    public DateTime IgnoreMe { get; set; }
}