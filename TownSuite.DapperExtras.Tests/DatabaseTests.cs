using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace TownSuite.DapperExtras.Tests;

public class DatabaseTests
{
    private string connectionString = "Data Source=:memory:";


    private SqliteConnection CreateSqliteDatabase()
    {
        // create in-memory database and table
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        var createTableSql = @"
            CREATE TABLE ExampleTable (
                Id INTEGER PRIMARY KEY,
                Col1 TEXT,
                Col2 TEXT,
                Col3 TEXT
            );";
        connection.Execute(createTableSql);

        var insertDataSql = @"
            INSERT INTO ExampleTable (Id, Col1, Col2, Col3) VALUES
            (1, 'Value1', 'ValueA', '2024-01-01'),
            (2, 'Value2', 'ValueB', '2024-02-01'),
            (3, 'Value3', 'ValueC', '2024-03-01');";
        connection.Execute(insertDataSql);

        return connection;
    }
    
    [Test]
    public void GetWhere_Test()
    {
        using var connection = CreateSqliteDatabase();
        var results = connection.GetWhere<ExampleTable>( new { Id = 1 });
        Assert.That(results.Count(), Is.EqualTo(1));
        var first = results.First();
        Assert.That(first.Col1, Is.EqualTo("Value1"));
        Assert.That(first.Col2, Is.EqualTo("ValueA"));
        Assert.That(first.Col3, Is.EqualTo(new DateTime(2024, 01, 01)));
    }
    
    [Test]
    public void UpdateWhere_and_GetWhereFirstOrDefault_Test()
    {
        using var connection = CreateSqliteDatabase();
        connection.UpdateWhere<ExampleTable>( new ExampleTable()
        {
            Id = 1,
            Col1 = "test1",
            Col2 = "test2",
            Col3 = new DateTime(2025, 12, 12)
        }, new { Id = 1 });
        
        var result = connection.GetWhereFirstOrDefault<ExampleTable>( new { Id = 1 });
        Assert.That(result.Col1, Is.EqualTo("test1"));
        Assert.That(result.Col2, Is.EqualTo("test2"));
        Assert.That(result.Col3, Is.EqualTo(new DateTime(2025, 12, 12)));
    }
    
    [Test]
    public async Task UpdateWhere_and_GetWhereFirstOrDefault_Async_Test()
    {
        await using var connection = CreateSqliteDatabase();
        await connection.UpdateWhereAsync<ExampleTable>( new ExampleTable()
        {
            Id = 1,
            Col1 = "test1",
            Col2 = "test2",
            Col3 = new DateTime(2025, 12, 12)
        }, new { Id = 1 });
        
        var result = await connection.GetWhereFirstOrDefaultAsync<ExampleTable>( new { Id = 1 });
        Assert.That(result.Col1, Is.EqualTo("test1"));
        Assert.That(result.Col2, Is.EqualTo("test2"));
        Assert.That(result.Col3, Is.EqualTo(new DateTime(2025, 12, 12)));
    }

    [Test]
    public async Task QueryDt_Test()
    {
        await using var connection = CreateSqliteDatabase();
        DataTable dt = connection.QueryDt( "select * from exampletable where id=@Id", new { Id = 1 });
        
        Assert.That(dt.Rows.Count, Is.EqualTo(1));
        DataRow row = dt.Rows[0];
        Assert.That(row["Col1"], Is.EqualTo("Value1"));
        Assert.That(row["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row["Col3"], Is.EqualTo("2024-01-01"));
    }
    
    [Test]
    public async Task QueryDt_WithSqlMapper_Test()
    {
        SqlMapper.AddTypeHandler(typeof(CustomId), new CustomIdDapperTypeHandler());
        
        await using var connection = CreateSqliteDatabase();
        var id = new CustomId { Id = 1 };
        DataTable dt = connection.QueryDt( "select * from exampletable where id=@Id", new { Id = id });
        
        Assert.That(dt.Rows.Count, Is.EqualTo(1));
        DataRow row = dt.Rows[0];
        Assert.That(row["Col1"], Is.EqualTo("Value1"));
        Assert.That(row["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row["Col3"], Is.EqualTo("2024-01-01"));
    }
}

public class CustomId
{
    internal int Id { get; set; }
    
    public override string ToString()
    {
        return Id.ToString();
    }
}

public class CustomIdDapperTypeHandler : SqlMapper.TypeHandler<CustomId>
{
    public override void SetValue(IDbDataParameter parameter, CustomId value)
    {
        if (parameter == null) throw new ArgumentNullException(nameof(parameter));
        if (value is null)
        {
            parameter.Value = DBNull.Value;
        }
        else
        {
            parameter.Value = value.Id;
            parameter.DbType = DbType.String;
        }
    }

    public override CustomId Parse(object value)
    {
        if (value == null || value is DBNull) throw new InvalidCastException("Cannot convert null/DBNULL to CustomId.");
        var s = value as string ?? value.ToString();
        return new CustomId()
        {
            Id = int.Parse(s)
        };
    }
}