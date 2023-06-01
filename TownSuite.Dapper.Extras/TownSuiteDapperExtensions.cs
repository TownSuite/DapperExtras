using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Diagnostics;
using System.Data.Common;

namespace TownSuite.Dapper.Extras
{
	public static partial class TownSuiteDapperExtensions
	{
		[DebuggerStepThrough]
		public static IEnumerable<T> GetWhere<T>(this IDbConnection connection, object param,
			IDbTransaction transaction = null, int? commandTimeout = null) where T : class
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

			sql.Append("select * from ");
			sql.Append(tableName);
			sql.Append(" where ");

			bool setAnd = false;
			foreach (var name in names)
			{
				if (setAnd)
				{
					sql.Append(" AND ");
				}

				sql.Append("[" + name + "]");
				sql.Append("=@");
				sql.Append(name);
				setAnd = true;
			}

			return connection.Query<T>(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="connection"></param>
		/// <param name="setParam">the set portion</param>
		/// <param name="whereParam">the where clause</param>
		/// <param name="transaction"></param>
		/// <param name="commandTimeout"></param>
		/// <returns></returns>
		// [DebuggerStepThrough]
		public static void UpdateWhere<T>(this IDbConnection connection, object setParam, object whereParam,
		   IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			var type = typeof(T);
			var setNames = new List<string>();
			var whereNames = new List<string>();
			ParameterNameList(setParam, setNames);
			ParameterNameList(whereParam, whereNames);

			var sql = new StringBuilder();
			var tableName = GetTableName(type);

			sql.Append("UPDATE ");
			sql.Append(tableName);
			sql.Append(" SET ");

			bool setComma = false;
			foreach (var name in setNames)
			{
				if (setComma)
				{
					sql.Append(", ");
				}

				sql.Append("[" + name + "]");
				sql.Append("=@");
				sql.Append(name);
				sql.Append("1");
				setComma = true;
			}

			sql.Append(" where ");

			bool setAnd = false;
			foreach (var name in whereNames)
			{
				if (setAnd)
				{
					sql.Append(" AND ");
				}

				sql.Append("[" + name + "]");
				sql.Append("=@");
				sql.Append(name);
				sql.Append("2");
				setAnd = true;
			}

			var param = Merge(setParam, whereParam);
			connection.Execute(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
		}

		public static object Merge(object item1, object item2)
		{
			if (item1 == null || item2 == null)
				return item1 ?? item2 ?? new System.Dynamic.ExpandoObject();

			dynamic expando = new System.Dynamic.ExpandoObject();
			var result = expando as IDictionary<string, object>;
			foreach (System.Reflection.PropertyInfo fi in item1.GetType().GetProperties())
			{
				result[fi.Name + "1"] = fi.GetValue(item1, null);
			}
			foreach (System.Reflection.PropertyInfo fi in item2.GetType().GetProperties())
			{
				result[fi.Name + "2"] = fi.GetValue(item2, null);
			}
			return result;
		}

		private static void ParameterNameList(object setParam, List<string> setNames)
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

		[DebuggerStepThrough]
		public static void DeleteWhere<T>(this IDbConnection connection, object param, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
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
			sql.Append(name);
			sql.Append(" WHERE ");

			bool setAnd = false;
			foreach (var prop in props)
			{
				if (setAnd)
				{
					sql.Append(" AND ");
				}

				sql.Append(prop.Name);
				sql.Append("=@");
				sql.Append(prop.Name);
				setAnd = true;
			}

			connection.Execute(sql.ToString(), param, transaction, commandTimeout: commandTimeout);
		}

		[DebuggerStepThrough]
		private static string GetTableName(Type type)
		{
			string name;

			//NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
			var tableAttr = type.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic;
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

	}

}


