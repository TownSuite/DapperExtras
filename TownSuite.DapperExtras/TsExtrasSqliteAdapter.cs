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
    internal class TsExtrasSqliteAdapter : TsExtrasCommonSqlGen
    {
        public override IEnumerable<T> GetWhere<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "\"", endQoute: "\"");

            return connection.Query<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override T GetWhereFirstOrDefault<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "\"", endQoute: "\"");

            return connection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<IEnumerable<T>> GetWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "\"", endQoute: "\"");

            return await connection.QueryAsync<T>(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<T> GetWhereFirstOrDefaultAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = GenerateGetWhereSql<T>(param, startQoute: "\"", endQoute: "\"");

            return await connection.QueryFirstOrDefaultAsync<T>(sql, param, transaction,
                commandTimeout: commandTimeout);
        }

        public override void UpdateWhere<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "\"", endQoute: "\"");

            connection.Execute(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override async Task UpdateWhereAsync<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var result = GenerateUpdateWhereSql<T>(setParam, whereParam, startQoute: "\"", endQoute: "\"");

            await connection.ExecuteAsync(result.sql, result.parameters, transaction, commandTimeout: commandTimeout);
        }

        public override void DeleteWhere<T>(IDbConnection connection, object param, IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "\"", endQoute: "\"");
            connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task DeleteWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var sql = GenerateDeleteWhereSql<T>(param, startQoute: "\"", endQoute: "\"");
            await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override int UpSert<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, startQoute: "\"", endQoute: "\"");
            var param = TsExtrasCommonSqlGen.Merge(whereParam, setParam);

            return connection.Execute(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> UpSertAsync<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = UpSertSqlGeneration<T>(setParam, whereParam, startQoute: "\"", endQoute: "\"");
            var param = TsExtrasCommonSqlGen.Merge(setParam, whereParam);

            return await connection.ExecuteAsync(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
        }
        
        public override int Insert<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "\"", endQoute: "\"");
            return connection.Execute(sql, param, transaction, commandTimeout: commandTimeout);
        }

        public override async Task<int> InsertAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var sql = InsertGeneration<T>(param, startQoute: "\"", endQoute: "\"");
            return await connection.ExecuteAsync(sql, param, transaction, commandTimeout: commandTimeout);
        }
    }
}