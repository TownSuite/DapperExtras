using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Data.Common;
using System.Reflection;
using Dapper;

namespace TownSuite.DapperExtras
{
    public static class TownSuiteDapperExtensions
    {
        public static DapperExtensionSettings Settings { get; set; } = new DapperExtensionSettings();

        private static readonly Dictionary<string, TsExtrasCommonSqlGen> AdapterDictionary
            = new Dictionary<string, TsExtrasCommonSqlGen>(6)
            {
                ["sqlconnection"] = new TsExtrasSqlServerAdapter(Settings),
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

        public static int TsInsert<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return adapter.Insert<T>(connection, param, transaction, commandTimeout);
        }

        public static async Task<int> TsInsertAsync<T>(this IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var adapter = GetAdapter(connection);
            return await adapter.InsertAsync<T>(connection, param, transaction, commandTimeout);
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
                    var propType = prop.PropertyType;
                    var valueObj = prop.GetValue(param, null);

                    if (SqlMapper.HasTypeHandler(propType))
                    {
                        var handlerObj = GetDapperTypeHandler(propType);
                        if (handlerObj != null)
                        {
                            var dbParam = (IDbDataParameter)cmd.CreateParameter();
                            dbParam.ParameterName = $"@{prop.Name}";

                            // invoke SetValue(handler) via reflection to let the handler set DbType/Value/etc.
                            var setValueMethod = handlerObj.GetType()
                                .GetMethod("SetValue", BindingFlags.Public | BindingFlags.Instance);
                            if (setValueMethod != null)
                            {
                                setValueMethod.Invoke(handlerObj, new object[] { dbParam, valueObj });
                            }
                            else
                            {
                                dbParam.Value = valueObj ?? DBNull.Value;
                            }

                            cmd.Parameters.Add(dbParam);
                            continue;
                        }

                        // fallback if no handler instance available
                        var fallbackParam = cmd.CreateParameter();
                        fallbackParam.ParameterName = $"@{prop.Name}";
                        fallbackParam.Value = valueObj?.ToString();
                        cmd.Parameters.Add(fallbackParam);
                    }
                    else
                    {
                        var value = prop.GetValue(param, null);
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = $"@{prop.Name}";
                        parameter.Value = value;
                        cmd.Parameters.Add(parameter);
                    }
                }
            }

            return ExecuteCmdTable(cmd);
        }


        public static Task<DataTable> QueryDtAsync(this IDbConnection connection, string sql, object param = null,
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
                    var propType = prop.PropertyType;
                    var valueObj = prop.GetValue(param, null);

                    if (SqlMapper.HasTypeHandler(propType))
                    {
                        var handlerObj = GetDapperTypeHandler(propType);
                        if (handlerObj != null)
                        {
                            var dbParam = (IDbDataParameter)cmd.CreateParameter();
                            dbParam.ParameterName = $"@{prop.Name}";

                            // invoke SetValue(handler) via reflection to let the handler set DbType/Value/etc.
                            var setValueMethod = handlerObj.GetType()
                                .GetMethod("SetValue", BindingFlags.Public | BindingFlags.Instance);
                            if (setValueMethod != null)
                            {
                                setValueMethod.Invoke(handlerObj, new object[] { dbParam, valueObj });
                            }
                            else
                            {
                                dbParam.Value = valueObj ?? DBNull.Value;
                            }

                            cmd.Parameters.Add(dbParam);
                            continue;
                        }

                        // fallback if no handler instance available
                        var fallbackParam = cmd.CreateParameter();
                        fallbackParam.ParameterName = $"@{prop.Name}";
                        fallbackParam.Value = valueObj?.ToString();
                        cmd.Parameters.Add(fallbackParam);
                    }
                    else
                    {
                        var value = prop.GetValue(param, null);
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = $"@{prop.Name}";
                        parameter.Value = value;
                        cmd.Parameters.Add(parameter);
                    }
                }
            }

            return ExecuteCmdTableAsync(cmd);
        }

        private static async Task<DataTable> ExecuteCmdTableAsync(IDbCommand cmd)
        {
            System.Data.ConnectionState origSate = cmd.Connection.State;
            if (cmd.Connection.State == ConnectionState.Closed)
            {
                cmd.Connection.Open();
            }

            if (cmd is DbCommand dbCommand)
            {
                using (var drSqlDataReader = await dbCommand.ExecuteReaderAsync())
                {
                    DataTable dt = new DataTable();
                    dt.Load(drSqlDataReader);
                    drSqlDataReader.Close();

                    if (origSate == ConnectionState.Closed)
                    {
                        cmd.Connection.Close();
                    }

                    return dt;
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

            var dt = new DataTable();
            using (var drSqlDataReader = cmd.ExecuteReader())
            {
                dt.Load(drSqlDataReader);
                drSqlDataReader.Close();
            }

            if (origSate == ConnectionState.Closed)
            {
                cmd.Connection.Close();
            }

            return dt;
        }

        // Reflection helper: try public method first, then private/internal fields/methods to find a registered handler instance.
        private static object GetDapperTypeHandler(Type t)
        {
            var mapperType = typeof(SqlMapper);

            // try public or non-public static method GetTypeHandler(Type)
            var method = mapperType.GetMethod("GetTypeHandler", BindingFlags.Public | BindingFlags.Static)
                         ?? mapperType.GetMethod("GetTypeHandler", BindingFlags.NonPublic | BindingFlags.Static);
            if (method != null)
            {
                try
                {
                    return method.Invoke(null, new object[] { t });
                }
                catch
                {
                    // ignore and fallthrough to field-based lookup
                }
            }

            // common internal field names used in different Dapper versions
            var candidateFieldNames = new[] { "typeHandlers", "handlers", "typeHandlerCache", "TypeHandlerCache" };
            foreach (var name in candidateFieldNames)
            {
                var field = mapperType.GetField(name,
                    BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Public);
                if (field == null) continue;
                var dict = field.GetValue(null) as System.Collections.IDictionary;
                if (dict == null) continue;

                if (dict.Contains(t))
                {
                    return dict[t];
                }

                foreach (System.Collections.DictionaryEntry de in dict)
                {
                    if (de.Key is Type keyType && keyType == t)
                        return de.Value;
                }
            }

            return null;
        }
    }
}