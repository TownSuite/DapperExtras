using TownSuite.DapperExtras;

namespace TownSuite.DapperExtras.Tests;

public class TsExtrasCommonPostgreSqlGen_Test
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void Postgresql_GetWhere_Test()
    {
        var genSql = new TsExtrasPostgreAdapter();
        var sql = genSql.GenerateGetWhereSql<ExampleTable>(new { Id = 123 }, startQoute: "", endQoute: "");
        Assert.That(sql, Is.EqualTo("SELECT * FROM ExampleTable WHERE Id=@Id;"));
    }

    [Test]
    public void Postgresql_UpdateWhere_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasPostgreAdapter();
        var result = genSql.GenerateUpdateWhereSql<ExampleTable>(setParam: new { Col1 = "helloworld" },
            whereParam: new { Id = 123 },
            startQoute: "", endQoute: "");
        Assert.That(result.sql, Is.EqualTo("UPDATE ExampleTable SET Col1=@Col1_1 WHERE Id=@Id_2;"));
    }

    [Test]
    public void Postgresql_DeleteWhere_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasPostgreAdapter();
        var sql =
            genSql.GenerateDeleteWhereSql<ExampleTable>(new { Id = 123 }, startQoute: "", endQoute: "");
        Assert.That(sql, Is.EqualTo("DELETE FROM ExampleTable WHERE Id=@Id;"));
    }

    [Test]
    public void Postgresql_Upsert_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasPostgreAdapter();
        var sql =
            genSql.UpSertSqlGeneration<ExampleTable>(new ExampleTable()
            {
                Id = 123,
                Col1 = "abc",
                Col2 = "def",
                Col3 = DateTime.MinValue
            }, new { Id = 123 });
        Assert.That(sql, Is.EqualTo(@"INSERT INTO ExampleTable (
Id,Col1,Col2,Col3
 )
VALUES (
@Id_1, @Col1_1, @Col2_1, @Col3_1)
ON CONFLICT (Id) 
DO 
UPDATE  SET Id=EXCLUDED.Id, Col1=EXCLUDED.Col1, Col2=EXCLUDED.Col2, Col3=EXCLUDED.Col3;"));
    }
    
    [Test]
    public void Postgresql_Insert_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasPostgreAdapter();
        var sql =
            genSql.InsertGeneration<ExampleTable>(new ExampleTable()
            {
                Id = 123,
                Col1 = "abc",
                Col2 = "def",
                Col3 = DateTime.MinValue
            });
        Assert.That(sql, Is.EqualTo(@"INSERT INTO ExampleTable (
Id,Col1,Col2,Col3
 )
VALUES (
@Id, @Col1, @Col2, @Col3);"));
    }
    
    [Test]
    public void Postgresql_Insert2_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasPostgreAdapter();
        var sql =
            genSql.InsertGeneration<ExampleTable>(new 
            {
                Id = 123,
                Col1 = "abc",
                Col2 = "def",
                Col3 = DateTime.MinValue
            });
        Assert.That(sql, Is.EqualTo(@"INSERT INTO ExampleTable (
Id,Col1,Col2,Col3
 )
VALUES (
@Id, @Col1, @Col2, @Col3);"));
    }
}