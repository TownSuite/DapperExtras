using Dapper.Contrib.Extensions;

namespace TownSuite.DapperExtras.Tests
{
    [Dapper.Contrib.Extensions.Table("ExampleTable3")]
    public class ExampleTable3
    {
        [System.ComponentModel.DataAnnotations.Key]
        public long Id { get; set; }

        public string Col1 { get; set; }

        public string Col2 { get; set; }

        public DateTime Col3 { get; set; }
    
        [Computed]
        public DateTime IgnoreMe { get; set; }
    }
}