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
    internal class TsExtrasSqlServerAdapter : TsExtrasCommonSqlGen
    {
        public override IEnumerable<T> GetWhere<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "[", endQoute: "]");

            return connection.Query<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override T GetWhereFirstOrDefault<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "[", endQoute: "]");

            return connection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<IEnumerable<T>> GetWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "[", endQoute: "]");

            return await connection.QueryAsync<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<T> GetWhereFirstOrDefaultAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "[", endQoute: "]");

            return await connection.QueryFirstOrDefaultAsync<T>(sql, param, transaction,
                commandTimeout: commandTimeout);
        }

        public override void UpdateWhere<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "[", endQoute: "]");

            connection.Execute(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override async Task UpdateWhereAsync<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "[", endQoute: "]");

            await connection.ExecuteAsync(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override void DeleteWhere<T>(IDbConnection connection, object param, IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "[", endQoute: "]");
            connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task DeleteWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "[", endQoute: "]");
            await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override int UpSert<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, "[", "]");
            var param = TsExtrasCommonSqlGen.Merge(whereParam, setParam);

            return connection.Execute(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> UpSertAsync<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, "[", "]"); 
            var param = TsExtrasCommonSqlGen.Merge(whereParam, setParam);

            return await connection.ExecuteAsync(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }

        internal override string UpSertSqlGeneration<T>(object setParam, object whereParam,
            string startQoute = "", string endQoute = "")
        {
            var type = typeof(T);
            var setNames = new List<string>();
            var whereNames = new List<string>();
            TsExtrasCommonSqlGen.ParameterNameList(setParam, setNames, includeKeyColumn: false);
            TsExtrasCommonSqlGen.ParameterNameList(whereParam, whereNames);

            var tableParts = TsExtrasCommonSqlGen.GetSchemaAndTableName(type);

            // TODO: cache generate sql for input type
            var sql = new StringBuilder();

            sql.AppendLine("MERGE INTO ");
            if (!string.IsNullOrEmpty(tableParts.Schema))
            {
                sql.Append($"{startQoute}{tableParts.Schema}{endQoute}.");
            }
            sql.AppendLine($"{startQoute}{tableParts.Table}{endQoute}");
            sql.AppendLine("AS tgt ");
            sql.AppendLine("USING");
            sql.Append("(SELECT ");
            bool setComma = false;
            var sbJoin = new StringBuilder();
            sbJoin.Append("ON ");
            foreach (var name in whereNames)
            {
                if (setComma)
                {
                    sql.Append(", ");
                    sbJoin.Append(" AND ");
                }

                // SELECT
                sql.Append("@");
                sql.Append(name);
                sql.Append("_1 ");
                sql.Append($"{startQoute}{name}{endQoute}");
                // END SELECT

                // JOIN
                sbJoin.Append("tgt.");
                sbJoin.Append($"{startQoute}{name}{endQoute}");
                sbJoin.Append("=src.");
                sbJoin.Append($"{startQoute}{name}{endQoute}");
                // END JOIN

                setComma = true;
            }

            sql.AppendLine(") AS src ");
            sql.AppendLine(sbJoin.ToString());

            sql.AppendLine("WHEN MATCHED THEN");

            sql.Append("UPDATE ");
            sql.Append("SET ");

            bool setComma2 = false;
            foreach (var name in setNames)
            {
                if (setComma2)
                {
                    sql.Append(", ");
                }

                // SELECT
                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=@");
                sql.Append(name);
                sql.Append("_2");
                setComma2 = true;
            }

            sql.AppendLine();
            sql.AppendLine("WHEN NOT MATCHED THEN ");
            sql.AppendLine("INSERT (");

            bool setComma3 = false;
            var sbInsertValues = new StringBuilder();
            sbInsertValues.AppendLine(") VALUES (");
            foreach (var name in setNames)
            {
                if (setComma3)
                {
                    sql.Append(", ");
                    sbInsertValues.Append(", ");
                }

                // SELECT
                sql.Append($"{startQoute}{name}{endQoute}");

                sbInsertValues.Append("@");
                sbInsertValues.Append(name);
                sbInsertValues.Append("_2");

                setComma3 = true;
            }

            sql.AppendLine(sbInsertValues.ToString());
            sql.Append(");");
            return sql.ToString();
        }
        
        public override int Insert<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "[", endQoute: "]");
            return connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> InsertAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "[", endQoute: "]");
            return await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }
    }
}