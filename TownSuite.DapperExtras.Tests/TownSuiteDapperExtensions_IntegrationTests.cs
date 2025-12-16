using System.Collections;
using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace TownSuite.DapperExtras.Tests;

public class TownSuiteDapperExtensions_IntegrationTests
{
    [SetUp]
    public async Task Setup()
    {
#if ENABLE_TESTCONTAINERS
        await DatabaseTestCases.InitializeAsync();
#endif
    }

    [TearDown]
    public async Task TearDown()
    {
#if ENABLE_TESTCONTAINERS
        await DatabaseTestCases.DisposeAsync();
#endif
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task GetWhereAsync_ReturnsMatchingRow_Test(IDbConnection connection)
    {
        //await using var connection = CreateSqliteDatabase();
        var results = await connection.GetWhereAsync<ExampleTable>(new { Id = 1 });
        Assert.That(results.Count(), Is.EqualTo(1));
        var first = results.First();
        Assert.That(first.Col1, Is.EqualTo("Value1"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void DeleteWhere_RemovesRow_Sync_Test(IDbConnection connection)
    {
        connection.DeleteWhere<ExampleTable>(new { Id = 1 });
        var remaining = connection.Query<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 1 });
        Assert.That(remaining.Count(), Is.EqualTo(0));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task DeleteWhereAsync_RemovesRow_Async_Test(IDbConnection connection)
    {
        await connection.DeleteWhereAsync<ExampleTable>(new { Id = 2 });
        var remaining =
            await connection.QueryAsync<ExampleTable>("select * from ExampleTable where Id=@Id", new { Id = 2 });
        Assert.That(remaining.Count(), Is.EqualTo(0));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void UpSert_InsertsWhenMissing_Sync_Test(IDbConnection connection)
    {
        var newItem = new ExampleTable()
        {
            Id = 4,
            Col1 = "New1",
            Col2 = "New2",
            Col3 = new DateTime(2025, 1, 1)
        };

        var affected = connection.UpSert<ExampleTable>(newItem, new { Id = newItem.Id });
        Assert.That(affected, Is.GreaterThanOrEqualTo(0));

        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 4 });
        Assert.That(row, Is.Not.Null);
        Assert.That(row.Col1, Is.EqualTo("New1"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void UpSert_UpdatesWhenPresent_Sync_Test(IDbConnection connection)
    {
        var updated = new ExampleTable()
        {
            Id = 1,
            Col1 = "Updated",
            Col2 = "Updated2",
            Col3 = new DateTime(2026, 1, 1)
        };

        var affected = connection.UpSert<ExampleTable>(updated, new { Id = updated.Id });
        Assert.That(affected, Is.GreaterThanOrEqualTo(0));

        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 1 });
        Assert.That(row.Col1, Is.EqualTo("Updated"));
        Assert.That(row.Col2, Is.EqualTo("Updated2"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task UpSertAsync_InsertsAndUpdates_Async_Test(IDbConnection connection)
    {
        var insertItem = new ExampleTable()
        {
            Id = 5,
            Col1 = "AsyncNew",
            Col2 = "AsyncNew2",
            Col3 = new DateTime(2025, 2, 2)
        };

        var aff1 = await connection.UpSertAsync<ExampleTable>(insertItem, new { Id = insertItem.Id });
        Assert.That(aff1, Is.GreaterThanOrEqualTo(0));
        var row1 = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 5 });
        Assert.That(row1, Is.Not.Null);

        // update existing
        var updateItem = new ExampleTable()
        {
            Id = 3,
            Col1 = "AsyncUpdated",
            Col2 = "AsyncUpdated2",
            Col3 = new DateTime(2027, 7, 7)
        };

        var aff2 = await connection.UpSertAsync<ExampleTable>(updateItem, new { Id = updateItem.Id });
        Assert.That(aff2, Is.GreaterThanOrEqualTo(0));
        var row2 = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 3 });
        Assert.That(row2.Col1, Is.EqualTo("AsyncUpdated"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void TsInsert_InsertsRow_ReturnsAffectedRows_Sync_Test(IDbConnection connection)
    {
        var item = new { Id = 6, Col1 = "I1", Col2 = "I2", Col3 = new DateTime(2028, 8, 8) };
        var affected = connection.TsInsert<ExampleTable>(item);
        Assert.That(affected, Is.EqualTo(1));
        var row = connection.QueryFirstOrDefault<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 6 });
        Assert.That(row, Is.Not.Null);
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task TsInsertAsync_InsertsRow_ReturnsAffectedRows_Async_Test(IDbConnection connection)
    {
        var item = new { Id = 7, Col1 = "AI1", Col2 = "AI2", Col3 = new DateTime(2029, 9, 9) };
        var affected = await connection.TsInsertAsync<ExampleTable>(item);
        Assert.That(affected, Is.EqualTo(1));
        var row = await connection.QueryFirstOrDefaultAsync<ExampleTable>("select * from ExampleTable where Id=@Id",
            new { Id = 7 });
        Assert.That(row, Is.Not.Null);
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void GetWhere_Test(IDbConnection connection)
    {
        var results = connection.GetWhere<ExampleTable>(new { Id = 1 });
        Assert.That(results.Count(), Is.EqualTo(1));
        var first = results.First();
        Assert.That(first.Col1, Is.EqualTo("Value1"));
        Assert.That(first.Col2, Is.EqualTo("ValueA"));
        Assert.That(first.Col3, Is.EqualTo(new DateTime(2024, 01, 01)));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public void UpdateWhere_and_GetWhereFirstOrDefault_Test(IDbConnection connection)
    {
        connection.UpdateWhere<ExampleTable>(new ExampleTable()
        {
            Id = 1,
            Col1 = "test1",
            Col2 = "test2",
            Col3 = new DateTime(2025, 12, 12)
        }, new { Id = 1 });

        var result = connection.GetWhereFirstOrDefault<ExampleTable>(new { Id = 1 });
        Assert.That(result.Col1, Is.EqualTo("test1"));
        Assert.That(result.Col2, Is.EqualTo("test2"));
        Assert.That(result.Col3, Is.EqualTo(new DateTime(2025, 12, 12)));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task UpdateWhere_and_GetWhereFirstOrDefault_Async_Test(IDbConnection connection)
    {
        await connection.UpdateWhereAsync<ExampleTable>(new ExampleTable()
        {
            Id = 1,
            Col1 = "test1",
            Col2 = "test2",
            Col3 = new DateTime(2025, 12, 12)
        }, new { Id = 1 });

        var result = await connection.GetWhereFirstOrDefaultAsync<ExampleTable>(new { Id = 1 });
        Assert.That(result.Col1, Is.EqualTo("test1"));
        Assert.That(result.Col2, Is.EqualTo("test2"));
        Assert.That(result.Col3, Is.EqualTo(new DateTime(2025, 12, 12)));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task QueryDt_Test(IDbConnection connection)
    {
        DataTable dt = connection.QueryDt("select * from exampletable where id=@Id", new { Id = 1 });

        Assert.That(dt.Rows.Count, Is.EqualTo(1));
        DataRow row = dt.Rows[0];
        Assert.That(row["Col1"], Is.EqualTo("Value1"));
        Assert.That(row["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row["Col3"], Is.EqualTo("2024-01-01"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task QueryDt_WithSqlMapper_Test(IDbConnection connection)
    {
        SqlMapper.AddTypeHandler(typeof(CustomId), new CustomIdDapperTypeHandler());

        var id = new CustomId { Id = 1 };
        DataTable dt = connection.QueryDt("select * from exampletable where id=@Id", new { Id = id });
        Assert.That(dt.Rows.Count, Is.EqualTo(1));
        DataRow row = dt.Rows[0];
        Assert.That(row["Col1"], Is.EqualTo("Value1"));
        Assert.That(row["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row["Col3"], Is.EqualTo("2024-01-01"));
        
        DataTable dt2 = await connection.QueryDtAsync("select * from exampletable where id=@Id", new { Id = id });
        Assert.That(dt2.Rows.Count, Is.EqualTo(1));
        DataRow row2 = dt2.Rows[0];
        Assert.That(row2["Col1"], Is.EqualTo("Value1"));
        Assert.That(row2["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row2["Col3"], Is.EqualTo("2024-01-01"));
    }

    [TestCaseSource(typeof(DatabaseTestCases), nameof(DatabaseTestCases.TestCases))]
    public async Task QueryDtAsync_Test(IDbConnection connection)
    {
        DataTable dt = await connection.QueryDtAsync("select * from exampletable where id=@Id", new { Id = 1 });

        Assert.That(dt.Rows.Count, Is.EqualTo(1));
        DataRow row = dt.Rows[0];
        Assert.That(row["Col1"], Is.EqualTo("Value1"));
        Assert.That(row["Col2"], Is.EqualTo("ValueA"));
        Assert.That(row["Col3"], Is.EqualTo("2024-01-01"));
    }

    public static class DatabaseTestCases
    {
        private readonly static MsSqlContainer _msSqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest") // Specify the SQL Server image
            .WithPassword("Strong_password_123!") // Set a strong password
            .Build(); // Build the container configuration

        private static readonly PostgreSqlContainer _dbContainer = new PostgreSqlBuilder()
            .WithImage("postgres:16.1-alpine") // Use a lightweight image
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithDatabase("testdb")
            .Build();

        public static async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();
            await _dbContainer.StartAsync();
        }

        public static async Task DisposeAsync()
        {
            await _msSqlContainer.StopAsync();
            await _dbContainer.StopAsync();
        }

        private static SqliteConnection CreateSqliteDatabase()
        {
            var connection = new SqliteConnection("Data Source=:memory:");
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

        private static SqlConnection CreateSqlServerDatabase()
        {
            var connection = new SqlConnection(_msSqlContainer.GetConnectionString());
            connection.Open();
            var createTableSql = @"
IF OBJECT_ID(N'dbo.YourTableName', N'U') IS NULL
BEGIN
            CREATE TABLE ExampleTable (
                Id INT PRIMARY KEY,
                Col1 NVARCHAR(100),
                Col2 NVARCHAR(100),
                Col3 DATETIME
            );
END";
            connection.Execute(createTableSql);
            var insertDataSql = @"
            TRUNCATE TABLE ExampleTable;
            INSERT INTO ExampleTable (Id, Col1, Col2, Col3) VALUES
            (1, 'Value1', 'ValueA', '2024-01-01'),
            (2, 'Value2', 'ValueB', '2024-02-01'),
            (3, 'Value3', 'ValueC', '2024-03-01');";
            connection.Execute(insertDataSql);
            return connection;
        }

        private static NpgsqlConnection CreatePostgreSqlDatabase()
        {
            var connection = new NpgsqlConnection(_msSqlContainer.GetConnectionString());
            connection.Open();
            var createTableSql = @"
            CREATE TABLE IF NOT EXISTS ExampleTable (
                Id INT PRIMARY KEY,
                Col1 VARCHAR(100),
                Col2 VARCHAR(100),
                Col3 TIMESTAMP
            );";
            connection.Execute(createTableSql);
            var insertDataSql = @"
            TRUNCATE TABLE ExampleTable;
            INSERT INTO ExampleTable (Id, Col1, Col2, Col3) VALUES
            (1, 'Value1', 'ValueA', '2024-01-01'),
            (2, 'Value2', 'ValueB', '2024-02-01'),
            (3, 'Value3', 'ValueC', '2024-03-01');";
            connection.Execute(insertDataSql);
            return connection;
        }

        public static IEnumerable<IDbConnection> TestCases
        {
            get
            {
                yield return CreateSqliteDatabase();
#if ENABLE_TESTCONTAINERS
                yield return CreateSqlServerDatabase();
                yield return CreatePostgreSqlDatabase();
#endif
            }
        }
    }
}