using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Data.Common;

namespace TownSuite.DapperExtras
{
    public static class TownSuiteDapperExtensions
    {
        private static readonly Dictionary<string, TsExtrasCommonSqlGen> AdapterDictionary
            = new Dictionary<string, TsExtrasCommonSqlGen>(6)
            {
                ["sqlconnection"] = new TsExtrasSqlServerAdapter(),
                ["npgsqlconnection"] = new TsExtrasPostgreAdapter(),
                ["sqliteconnection"] = new TsExtrasSqliteAdapter()
            };

        private static TsExtrasCommonSqlGen GetAdapter(IDbConnection connection)
        {
            var name = connection.GetType().Name.ToLower();

            return AdapterDictionary.TryGetValue(name, out var adapter)
                ? adapter
                : new TsExtrasSqlServerAdapter();
        }

        public static IEnumerable<T> GetWhere<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return adapter.GetWhere<T>(connection, param, transaction, commandTimeout);
        }

        public static T GetWhereFirstOrDefault<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return adapter.GetWhereFirstOrDefault<T>(connection, param, transaction, commandTimeout);
        }
        
        public static async Task<IEnumerable<T>> GetWhereAsync<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return await adapter.GetWhereAsync<T>(connection, param, transaction, commandTimeout);
        }
        
        public static async Task<T> GetWhereFirstOrDefaultAsync<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return await adapter.GetWhereFirstOrDefaultAsync<T>(connection, param, transaction, commandTimeout);
        }

        public static void UpdateWhere<T>(this IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            adapter.UpdateWhere<T>(connection, setParam, whereParam, transaction, commandTimeout);
        }

        public static async Task UpdateWhereAsync<T>(this IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            await adapter.UpdateWhereAsync<T>(connection, setParam, whereParam, transaction, commandTimeout);
        }

        public static void DeleteWhere<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            adapter.DeleteWhere<T>(connection, param, transaction, commandTimeout);
        }

        public static async Task DeleteWhereAsync<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            await adapter.DeleteWhereAsync<T>(connection, param, transaction, commandTimeout);
        }

        public static int UpSert<T>(this IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return adapter.UpSert<T>(connection, setParam, whereParam, transaction, commandTimeout);
        }

        public static async Task<int> UpSertAsync<T>(this IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return await adapter.UpSertAsync<T>(connection, setParam, whereParam, transaction, commandTimeout);
        }

        public static DataTable QueryDt(this IDbConnection connection, string sql, object param = null,
            IDbTransaction transaction = null, int? commandTimeout = null,
            CommandType commandType = CommandType.Text)
        {
            var cmd = connection.CreateCommand();
            cmd.Connection = connection;
            cmd.CommandType = commandType;
            cmd.CommandText = sql;
            cmd.Transaction = transaction;
            if (commandTimeout.HasValue)
            {
                cmd.CommandTimeout = commandTimeout.Value;
            }

            if (param != null)
            {
                var props = param.GetType().GetProperties();
                foreach (var prop in props)
                {
                    var value = prop.GetValue(param, null);
                    var parameter = cmd.CreateParameter();
                    parameter.ParameterName = $"@{prop.Name}";
                    parameter.Value = value;
                    cmd.Parameters.Add(parameter);
                }
            }

            return ExecuteCmdTable(cmd);
        }

        private static DataTable ExecuteCmdTable(IDbCommand cmd)
        {
            var origSate = cmd.Connection.State;
            if (cmd.Connection.State == ConnectionState.Closed)
            {
                cmd.Connection.Open();
            }

            var MainDatatable = new DataTable();
            using (var drSqlDataReader = cmd.ExecuteReader())
            {
                MainDatatable.Load(drSqlDataReader);
                drSqlDataReader.Close();
            }

            if (origSate == ConnectionState.Closed)
            {
                cmd.Connection.Close();
            }

            return MainDatatable;
        }
    }
}