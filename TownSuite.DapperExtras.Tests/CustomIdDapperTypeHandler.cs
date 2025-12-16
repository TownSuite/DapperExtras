using System.Data;
using Dapper;

namespace TownSuite.DapperExtras.Tests;

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