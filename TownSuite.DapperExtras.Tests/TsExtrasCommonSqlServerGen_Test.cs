using TownSuite.DapperExtras;

namespace TownSuite.DapperExtras.Tests;

public class TsExtrasCommonSqlServerGen_Test
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void SqlServer_GetWhere_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasSqlServerAdapter();
        var sql = genSql.GenerateGetWhereSql<ExampleTable>(new { Id = 123 },
            startQoute: "[", endQoute: "]");
        Assert.That(sql, Is.EqualTo("SELECT * FROM [ExampleTable] WHERE [Id]=@Id;"));
    }

    [Test]
    public void SqlServer_UpdateWhere_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasSqlServerAdapter();
        var result =
            genSql.GenerateUpdateWhereSql<ExampleTable>(setParam: new { Col1 = "helloworld" },
                whereParam: new { Id = 123 },
                startQoute: "[", endQoute: "]");
        Assert.That(result.sql, Is.EqualTo("UPDATE [ExampleTable] SET [Col1]=@Col1_1 WHERE [Id]=@Id_2;"));
    }

    [Test]
    public void SqlServer_DeleteWhere_Test()
    {
        var poco = new ExampleTable();
        var genSql = new TsExtrasSqlServerAdapter();
        var sql =
            genSql.GenerateDeleteWhereSql<ExampleTable>(new { Id = 123 },
                startQoute: "[", endQoute: "]");
        Assert.That(sql, Is.EqualTo("DELETE FROM [ExampleTable] WHERE [Id]=@Id;"));
    }

    [Test]
    public void SqlServer_Upsert_Test()
    {
        var genSql = new TsExtrasSqlServerAdapter();
        var sql =
            genSql.UpSertSqlGeneration<ExampleTable>(new ExampleTable()
            {
                Id = 123,
                Col1 = "abc",
                Col2 = "def",
                Col3 = DateTime.MinValue
            }, new { Id = 123 },
                startQoute: "[", endQoute: "]");
        Assert.That(sql, Is.EqualTo(@"MERGE INTO 
[ExampleTable]
AS tgt 
USING
(SELECT @Id_1 Id) AS src 
ON tgt.[Id]=src.[Id]
WHEN MATCHED THEN
UPDATE SET [Id]=@Id_2, [Col1]=@Col1_2, [Col2]=@Col2_2, [Col3]=@Col3_2
WHEN NOT MATCHED THEN 
INSERT (
[Id], [Col1], [Col2], [Col3]) VALUES (
@Id_2, @Col1_2, @Col2_2, @Col3_2
);"));
    }
}