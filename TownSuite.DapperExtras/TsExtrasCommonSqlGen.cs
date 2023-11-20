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
    internal abstract class TsExtrasCommonSqlGen
    {
        public abstract IEnumerable<T> GetWhere<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract T GetWhereFirstOrDefault<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract Task<IEnumerable<T>> GetWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract Task<T> GetWhereFirstOrDefaultAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract void UpdateWhere<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract Task UpdateWhereAsync<T>(IDbConnection connection, object setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract void DeleteWhere<T>(IDbConnection connection, object param, IDbTransaction transaction = null,
            int? commandTimeout = null);

        public abstract Task DeleteWhereAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null,
            int? commandTimeout = null);

        public abstract int UpSert<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract Task<int> UpSertAsync<T>(IDbConnection connection, T setParam, object whereParam,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract int Insert<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);

        public abstract Task<int> InsertAsync<T>(IDbConnection connection, object param,
            IDbTransaction transaction = null, int? commandTimeout = null);
        
        internal string GenerateGetWhereSql<T>(object param, string startQoute = "", string endQoute = "")
        {
            var type = typeof(T);
            var names = new List<string>();

            if (param.GetType() == typeof(DynamicParameters))
            {
                var p = (DynamicParameters)param;
                foreach (var item in p.ParameterNames)
                {
                    names.Add(item);
                }
            }
            else
            {
                var props = param.GetType().GetProperties();
                if (!props.Any())
                {
                    throw new DataException("GetWhere<T> must have param");
                }

                foreach (var prop in props)
                {
                    names.Add(prop.Name);
                }
            }

            var sql = new StringBuilder();
            var tableName = GetTableName(type);

            sql.Append("SELECT * FROM ");
            sql.Append($"{startQoute}{tableName}{endQoute}");
            sql.Append(" WHERE ");

            bool setAnd = false;
            foreach (var name in names)
            {
                if (setAnd)
                {
                    sql.Append(" AND ");
                }

                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=@");
                sql.Append(name);
                setAnd = true;
            }

            sql.Append(";");
            return sql.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="setParam">the set portion</param>
        /// <param name="whereParam">the where clause</param>
        /// <returns>sql parameters</returns>
        internal (string sql, object parameters) GenerateUpdateWhereSql<T>(object setParam, object whereParam,
            string startQoute = "", string endQoute = "")
        {
            var type = typeof(T);
            var setNames = new List<string>();
            var whereNames = new List<string>();
            ParameterNameList(setParam, setNames);
            ParameterNameList(whereParam, whereNames);

            var sql = new StringBuilder();
            var tableName = GetTableName(type);

            sql.Append("UPDATE ");
            sql.Append($"{startQoute}{tableName}{endQoute}");
            sql.Append(" SET ");

            bool setComma = false;
            foreach (var name in setNames)
            {
                if (setComma)
                {
                    sql.Append(", ");
                }

                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=@");
                sql.Append(name);
                sql.Append("_1");
                setComma = true;
            }

            sql.Append(" WHERE ");

            bool setAnd = false;
            foreach (var name in whereNames)
            {
                if (setAnd)
                {
                    sql.Append(" AND ");
                }

                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=@");
                sql.Append(name);
                sql.Append("_2");
                setAnd = true;
            }

            sql.Append(";");

            var param = Merge(setParam, whereParam);
            return (sql.ToString(), param);
        }

        protected static object Merge(object item1, object item2)
        {
            if (item1 == null || item2 == null)
                return item1 ?? item2 ?? new System.Dynamic.ExpandoObject();

            dynamic expando = new System.Dynamic.ExpandoObject();
            var result = expando as IDictionary<string, object>;
            foreach (System.Reflection.PropertyInfo fi in item1.GetType().GetProperties())
            {
                result[fi.Name + "_1"] = fi.GetValue(item1, null);
            }

            foreach (System.Reflection.PropertyInfo fi in item2.GetType().GetProperties())
            {
                result[fi.Name + "_2"] = fi.GetValue(item2, null);
            }

            return result;
        }

        protected static void ParameterNameList(object setParam, List<string> setNames)
        {
            if (setParam.GetType() == typeof(DynamicParameters))
            {
                var p = (DynamicParameters)setParam;
                foreach (var item in p.ParameterNames)
                {
                    setNames.Add(item);
                }
            }
            else
            {
                var props = setParam.GetType().GetProperties();
                if (!props.Any())
                {
                    throw new DataException("UpdateWhere<T> must have set and where param");
                }

                foreach (var prop in props)
                {
                    setNames.Add(prop.Name);
                }
            }
        }

        internal string GenerateDeleteWhereSql<T>(object param,
            string startQoute = "", string endQoute = "")
        {
            var type = typeof(T);

            var props = param.GetType().GetProperties();
            if (!props.Any())
            {
                throw new DataException("DeleteWhere<T> must have param");
            }

            var sql = new StringBuilder();
            var name = GetTableName(type);

            sql.Append("DELETE FROM ");
            sql.Append($"{startQoute}{name}{endQoute}");
            sql.Append(" WHERE ");

            bool setAnd = false;
            foreach (var prop in props)
            {
                if (setAnd)
                {
                    sql.Append(" AND ");
                }

                sql.Append($"{startQoute}{prop.Name}{endQoute}");
                sql.Append("=@");
                sql.Append(prop.Name);
                setAnd = true;
            }

            sql.Append(";");

            return sql.ToString();
        }

        /// <summary>
        /// The default insert or update method is compatible with postgresql and sqlite.
        /// </summary>
        /// <param name="setParam"></param>
        /// <param name="whereParam"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal virtual string UpSertSqlGeneration<T>(object setParam, object whereParam,
            string startQoute = "", string endQoute = "")
        {
            // TODO: cache generate sql for input type
            
            var type = typeof(T);
            var setNames = new List<string>();
            var whereNames = new List<string>();
            TsExtrasCommonSqlGen.ParameterNameList(setParam, setNames);
            TsExtrasCommonSqlGen.ParameterNameList(whereParam, whereNames);

            var tableName = TsExtrasCommonSqlGen.GetTableName(type);

            var sql = new StringBuilder();

            sql.Append("INSERT INTO ");
            sql.Append($"{startQoute}{tableName}{endQoute}");
            sql.AppendLine(" (");
            sql.AppendLine(string.Join(",", setNames.Select(p => $"{startQoute}{p}{endQoute}")));
            sql.AppendLine(" )");

            sql.AppendLine("VALUES (");
            bool setComma = false;
            foreach (var name in setNames)
            {
                if (setComma)
                {
                    sql.Append(", ");
                }

                // SELECT
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
                if (setComma)
                {
                    sql.Append(", ");
                }

                // SELECT
                sql.Append($"{startQoute}{name}{endQoute}");
                sql.Append("=");
                sql.Append($"EXCLUDED.{startQoute}{name}{endQoute}");
                setComma = true;
            }

            sql.Append(";");
            return sql.ToString();
        }

        protected static string GetTableName(Type type)
        {
            string name;

            //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
            var tableAttr = type.GetCustomAttributes(false)
                .SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic;
            if (tableAttr != null)
                name = tableAttr.Name;
            else
            {
                name = type.Name + "s";
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);
            }

            return name;
        }
        
        internal virtual string InsertGeneration<T>(object setParam,
            string startQoute = "", string endQoute = "")
        {
            // TODO: cache generate sql for input type
            
            var type = typeof(T);
            var setNames = new List<string>();
            TsExtrasCommonSqlGen.ParameterNameList(setParam, setNames);

            var tableName = TsExtrasCommonSqlGen.GetTableName(type);

            var sql = new StringBuilder();

            sql.Append("INSERT INTO ");
            sql.Append($"{startQoute}{tableName}{endQoute}");
            sql.AppendLine(" (");
            sql.AppendLine(string.Join(",", setNames.Select(p => $"{startQoute}{p}{endQoute}")));
            sql.AppendLine(" )");

            sql.AppendLine("VALUES (");
            bool setComma = false;
            foreach (var name in setNames)
            {
                if (setComma)
                {
                    sql.Append(", ");
                }

                // SELECT
                sql.Append("@");
                sql.Append(name);
                sql.Append("_1");
                setComma = true;
            }
            
            sql.Append(");");
            return sql.ToString();
        }
    }
}