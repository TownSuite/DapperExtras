namespace TownSuite.DapperExtras.Tests;

public class CustomId
{
    internal int Id { get; set; }
    
    public override string ToString()
    {
        return Id.ToString();
    }
}