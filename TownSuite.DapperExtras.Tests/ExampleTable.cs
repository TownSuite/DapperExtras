namespace TownSuite.DapperExtras.Tests;
using Dapper.Contrib.Extensions;

[Dapper.Contrib.Extensions.Table("ExampleTable")]
public class ExampleTable
{
    [Dapper.Contrib.Extensions.Key()]
    public long Id { get; set; }

    public string Col1 { get; set; }

    public string Col2 { get; set; }

    public DateTime Col3 { get; set; }
    
    [Computed]
    public DateTime IgnoreMe { get; set; }
}