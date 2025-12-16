using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace TownSuite.DapperExtras.Tests;

public class TownSuiteDapperExtensions_IntegrationTests
{
    private string connectionString = "Data Source=:memory:";

    private SqliteConnection CreateSqliteDatabase()
    {
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
    public async Task GetWhereAsync_ReturnsMatchingRow_Test()
    {
        await using var connection = CreateSqliteDatabase();
        var results = await connection.GetWhereAsync<ExampleTable>(new { Id = 1 });
        Assert.That(results.Count(), Is.EqualTo(1));
        var first = results.First();
        Assert.That(first.Col1, Is.EqualTo("Value1"));
    }

    [Test]
    public void DeleteWhere_RemovesRow_Sync_Test()
    {
        using var connection = CreateSqliteDatabase();
        connection.DeleteWhere<ExampleTable>(new { Id = 1 });
        var remaining = connection.Query<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 1 });
        Assert.That(remaining.Count(), Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteWhereAsync_RemovesRow_Async_Test()
    {
        await using var connection = CreateSqliteDatabase();
        await connection.DeleteWhereAsync<ExampleTable>(new { Id = 2 });
        var remaining = await connection.QueryAsync<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 2 });
        Assert.That(remaining.Count(), Is.EqualTo(0));
    }

    [Test]
    public void UpSert_InsertsWhenMissing_Sync_Test()
    {
        using var connection = CreateSqliteDatabase();
        var newItem = new ExampleTable()
        {
            Id = 4,
            Col1 = "New1",
            Col2 = "New2",
            Col3 = new DateTime(2025,1,1)
        };

        var affected = connection.UpSert<ExampleTable>(newItem, new { Id = newItem.Id });
        Assert.That(affected, Is.GreaterThanOrEqualTo(0));

        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 4 });
        Assert.That(row, Is.Not.Null);
        Assert.That(row.Col1, Is.EqualTo("New1"));
    }

    [Test]
    public void UpSert_UpdatesWhenPresent_Sync_Test()
    {
        using var connection = CreateSqliteDatabase();
        var updated = new ExampleTable()
        {
            Id = 1,
            Col1 = "Updated",
            Col2 = "Updated2",
            Col3 = new DateTime(2026,1,1)
        };

        var affected = connection.UpSert<ExampleTable>(updated, new { Id = updated.Id });
        Assert.That(affected, Is.GreaterThanOrEqualTo(0));

        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 1 });
        Assert.That(row.Col1, Is.EqualTo("Updated"));
        Assert.That(row.Col2, Is.EqualTo("Updated2"));
    }

    [Test]
    public async Task UpSertAsync_InsertsAndUpdates_Async_Test()
    {
        await using var connection = CreateSqliteDatabase();
        var insertItem = new ExampleTable()
        {
            Id = 5,
            Col1 = "AsyncNew",
            Col2 = "AsyncNew2",
            Col3 = new DateTime(2025,2,2)
        };

        var aff1 = await connection.UpSertAsync<ExampleTable>(insertItem, new { Id = insertItem.Id });
        Assert.That(aff1, Is.GreaterThanOrEqualTo(0));
        var row1 = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 5 });
        Assert.That(row1, Is.Not.Null);

        // update existing
        var updateItem = new ExampleTable()
        {
            Id = 3,
            Col1 = "AsyncUpdated",
            Col2 = "AsyncUpdated2",
            Col3 = new DateTime(2027,7,7)
        };

        var aff2 = await connection.UpSertAsync<ExampleTable>(updateItem, new { Id = updateItem.Id });
        Assert.That(aff2, Is.GreaterThanOrEqualTo(0));
        var row2 = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 3 });
        Assert.That(row2.Col1, Is.EqualTo("AsyncUpdated"));
    }

    [Test]
    public void TsInsert_InsertsRow_ReturnsAffectedRows_Sync_Test()
    {
        using var connection = CreateSqliteDatabase();
        var item = new { Id = 6, Col1 = "I1", Col2 = "I2", Col3 = new DateTime(2028,8,8) };
        var affected = connection.TsInsert<ExampleTable>(item);
        Assert.That(affected, Is.EqualTo(1));
        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 6 });
        Assert.That(row, Is.Not.Null);
    }

    [Test]
    public async Task TsInsertAsync_InsertsRow_ReturnsAffectedRows_Async_Test()
    {
        await using var connection = CreateSqliteDatabase();
        var item = new { Id = 7, Col1 = "AI1", Col2 = "AI2", Col3 = new DateTime(2029,9,9) };
        var affected = await connection.TsInsertAsync<ExampleTable>(item);
        Assert.That(affected, Is.EqualTo(1));
        var row = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 7 });
        Assert.That(row, Is.Not.Null);
    }
}

