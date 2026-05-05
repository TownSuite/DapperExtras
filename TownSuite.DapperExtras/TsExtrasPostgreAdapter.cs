using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace TownSuite.DapperExtras
{
    internal class TsExtrasPostgreAdapter : TsExtrasCommonSqlGen
    {
         public override IEnumerable<T> GetWhere<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "", endQoute: "");

            return connection.Query<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override T GetWhereFirstOrDefault<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "", endQoute: "");

            return connection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<IEnumerable<T>> GetWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "", endQoute: "");

            return await connection.QueryAsync<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<T> GetWhereFirstOrDefaultAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "", endQoute: "");

            return await connection.QueryFirstOrDefaultAsync<T>(sql, param, transaction,
                commandTimeout: commandTimeout);
        }

        public override void UpdateWhere<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "", endQoute: "");

            connection.Execute(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override async Task UpdateWhereAsync<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "", endQoute: "");

            await connection.ExecuteAsync(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override void DeleteWhere<T>(IDbConnection connection, object param, IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "", endQoute: "");
            connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task DeleteWhereAsync<T>(IDbConnection connection, object param, IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "", endQoute: "");
            await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }
        
        internal override string UpSertSqlGeneration<T>(object setParam, object whereParam,
            string startQoute = "", string endQoute = "")
        {
            var type = typeof(T);
            var setNames = new List<string>();
            var whereNames = new List<string>();
            TsExtrasCommonSqlGen.ParameterNameList(setParam, setNames, includeKeyColumn: true);
            TsExtrasCommonSqlGen.ParameterNameList(whereParam, whereNames, includeKeyColumn: true);

            var tableParts = TsExtrasCommonSqlGen.GetSchemaAndTableName(type);

            // Detect identity ([Key]) columns — PostgreSQL GENERATED ALWAYS AS IDENTITY requires:
            //   1. OVERRIDING SYSTEM VALUE on INSERT to allow supplying an explicit value
            //   2. Excluding those columns from DO UPDATE SET (cannot update an identity column)
            var identityKeyNames = setParam.GetType().GetProperties()
                .Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == "KeyAttribute"))
                .Select(p => p.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            bool hasIdentityKey = identityKeyNames.Count > 0;

            var sql = new StringBuilder();

            sql.Append("INSERT INTO ");
            if (!string.IsNullOrEmpty(tableParts.Schema))
            {
                sql.Append($"{startQoute}{tableParts.Schema}{endQoute}.");
            }
            sql.Append($"{startQoute}{tableParts.Table}{endQoute}");
            sql.AppendLine(" (");
            sql.AppendLine(string.Join(",", setNames.Select(p => $"{startQoute}{p}{endQoute}")));
            sql.AppendLine(" )");

            if (hasIdentityKey)
            {
                sql.AppendLine("OVERRIDING SYSTEM VALUE");
            }

            sql.AppendLine("VALUES (");
            bool setComma = false;
            foreach (var name in setNames)
            {
                if (setComma)
                {
                    sql.Append(", ");
                }

                sql.Append("@");
                sql.Append(name);
                sql.Append("_1");
                setComma = true;
            }

            sql.AppendLine(")");

            sql.AppendLine($"ON CONFLICT ({string.Join(",", whereNames.Select(p => $"{startQoute}{p}{endQoute}"))}) ");
            sql.AppendLine("DO ");
            sql.Append("UPDATE ");
            sql.Append(" SET ");

            setComma = false;
            foreach (var name in setNames)
            {
                // GENERATED ALWAYS AS IDENTITY columns cannot be updated — skip them in the SET clause
                if (identityKeyNames.Contains(name))
                    continue;

                if (setComma)
                {
                    sql.Append(", ");
                }

                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=");
                sql.Append($"EXCLUDED.{startQoute}{name}{endQoute}");
                setComma = true;
            }

            sql.Append(";");
            return sql.ToString();
        }

        public override int UpSert<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, startQoute: "", endQoute: "");
            var param = TsExtrasCommonSqlGen.Merge(setParam, whereParam);

            return connection.Execute(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> UpSertAsync<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, startQoute: "", endQoute: "");
            var param = TsExtrasCommonSqlGen.Merge(setParam, whereParam);

            return await connection.ExecuteAsync(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }
        
        public override int Insert<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "", endQoute: "");
            return connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> InsertAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "", endQoute: "");
            return await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }
    }
}