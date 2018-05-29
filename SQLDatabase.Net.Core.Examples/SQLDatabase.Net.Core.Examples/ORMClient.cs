using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SQLDatabase.Net.SQLDatabaseClient;

namespace SQLDatabase.Net.ORMClient
{
    #region Attributes
    /// <summary>
    /// Use these attributes to decorate the properties on your class.
    /// </summary>
    /// <example>
    /// Example 1 : [DBColumn(AutoIncrement = true, PrimaryKey = true)] <para />
    /// Example 2 : [DBColumn] <para />
    /// Example 3 : [DBColumn(Unique = true)] <para />
    /// Example of foreign key : [DBColumn (IsForeignKey = true, ForeignKeyTable = "Jobs", ForeignKeyColumn = "JobId")]
    /// </example>
    public class DBColumnAttribute : Attribute
    {        
        /// <summary>
        /// Set true if the property is primary key in the table
        /// </summary>
        public bool PrimaryKey { get; set; }
        /// <summary>
        /// Set true if the property is part of either composite or compound primary key in the table
        /// </summary>
        public bool CombinedPrimaryKey { get; set; }
        /// <summary>
        /// Defines if column(s) contains unique values.
        /// </summary>
        public bool Unique { get; set; }
        /// <summary>
        /// Defines if column is not null.
        /// </summary>
        public bool NotNull { get; set; }
        /// <summary>
        /// Defines if column value will be auto incremented must be integer type with primary key.
        /// </summary>
        public bool AutoIncrement { get; set; }
        /// <summary>
        /// Defines if column is foreign key referenee, other two are also required if IsForeignKey is true.
        /// </summary>
        public bool ForeignKey { get; set; }
        /// <summary>
        /// Defines the table of foreign key.
        /// </summary>
        public string ForeignKeyTable { get; set; }
        /// <summary>
        /// Defines the column of foreign table.
        /// </summary>
        public string ForeignKeyColumn { get; set; }
    }
      
    #endregion

    public class SqlDatabaseOrmClient<T> where T : class, new()
    {
        public T _DatabaseObjectT = new T();
        public SqlDatabaseConnection Connection { get; set; }
        public bool ExtendedResultSet { get; set; } = false;
        public Int64 RowsAffected { get; set; }

        #region Constructor
        /// <summary>
        /// default constructor, opens connection to in memory database using @memory.
        /// </summary>
        public SqlDatabaseOrmClient()
        {
            Connection = new SqlDatabaseConnection
            {
                DatabaseFileMode = DatabaseFileMode.OpenOrCreate,
                DatabaseMode = DatabaseMode.ReadWrite,
                MultipleActiveResultSets = true,
                ConnectionString = "SchemaName=sdbn;uri=@memory"
            };
            Connection.Open();
            _DatabaseObjectT = new T();
        }
        /// <summary>
        /// Pass the SqlDatabaseConnection object with open Connection in constructor
        /// </summary>
        /// <param name="SqlDatabaseConnection"></param>
        public SqlDatabaseOrmClient(SqlDatabaseConnection ConnectionObject)
        {
            if (ConnectionObject.State != System.Data.ConnectionState.Open)
                throw new Exception("Open Connection is required.");

            if (!ConnectionObject.MultipleActiveResultSets)
                ConnectionObject.MultipleActiveResultSets = true;

            Connection = ConnectionObject;
            _DatabaseObjectT = new T();
        }
        /// <summary>
        /// Pass the Sqldatabase file path
        /// </summary>
        /// <param name="DatabaseFilePath"></param>
        public SqlDatabaseOrmClient(string DatabaseFilePath)
        {
            Connection = new SqlDatabaseConnection
            {
                DatabaseFileMode = DatabaseFileMode.OpenOrCreate,
                DatabaseMode = DatabaseMode.ReadWrite,
                MultipleActiveResultSets = true,
                ConnectionString = "SchemaName=sdbn;uri=file://" + DatabaseFilePath
            };
            Connection.Open();
            _DatabaseObjectT = new T();
        }
        /// <summary>
        /// Pass the schema name and Sqldatabase file path
        /// </summary>
        /// <param name="SchemaName"></param>
        /// <param name="DatabaseFilePath"></param>
        public SqlDatabaseOrmClient(string SchemaName, string DatabaseFilePath)
        {
            Connection = new SqlDatabaseConnection
            {
                DatabaseFileMode = DatabaseFileMode.OpenOrCreate,
                DatabaseMode = DatabaseMode.ReadWrite,
                MultipleActiveResultSets = true,
                ConnectionString = "SchemaName=" + SchemaName + "; uri=file://" + DatabaseFilePath
            };
            Connection.Open();
            _DatabaseObjectT = new T();
        }
        #endregion

        public IList<T> Find(IFilter<T> filter)
        {
            T entity = new T();
            return ExecuteGet<T>(filter);
        }
        
        /// <summary>
        /// Get all records from particular table
        /// </summary>
        public IList<T> GetAll()
        {
            T entity = new T();
            return ExecuteGet(string.Format("SELECT * FROM [{0}]", entity.GetType().Name));
        }

        /// <summary>
        /// returns type T when sql is supplied as commandText
        /// </summary>
        /// <param name="commandText"></param>
        /// <returns></returns>
        public IList<T> GetAll(string commandText)
        {
            return ExecuteGet(commandText);
        }

        /// <summary>
        /// returns type TEntity when sql is supplied as commandText
        /// </summary>
        /// <param name="commandText"></param>
        /// <returns></returns>
        public IList<TEntity> GetAll<TEntity>(string commandText)
            where TEntity : class, new()
        {
            return ExecuteGet<TEntity>(commandText);
        }
               

        /// <summary>
        /// Execute Only with optional return or No return
        /// </summary>
        /// <param name="commandText"></param>        
        private long Execute(string commandText, bool returnIdentity = false)
        {           
            using (var cmd = new SqlDatabaseCommand())
            {
                cmd.Connection = Connection;                
                cmd.CommandText = commandText;
                
                    try
                    {
                        if (returnIdentity)
                        {
                            cmd.ExecuteNonQuery();
                            if (!cmd.GetLastError().Equals("SQLDatabase_OK"))
                                throw new Exception(cmd.GetLastError());
                            else
                                return cmd.Connection.LastInsertRowId;
                        }
                        else
                        {
                            cmd.ExecuteNonQuery();
                            if (!cmd.GetLastError().Equals("SQLDatabase_OK"))
                                throw new Exception(cmd.GetLastError());
                        }
                    }
                    catch (SqlDatabaseException e)
                    {
                        throw e;
                    }
                
            }
            return 0;
        }
        /// <summary>
        /// Execute and return rows affected
        /// </summary>
        /// <param name="commandText"></param>        
        private long Execute(string commandText)
        {
            using (var cmd = new SqlDatabaseCommand())
            {
                cmd.Connection = Connection;
                SQLDatabaseResultSet[] reader;
                cmd.CommandText = commandText;
                reader = cmd.ExecuteNonQuery(ExtendedResultSet);
                if (reader != null)
                {
                    if (string.IsNullOrWhiteSpace(reader[0].ErrorMessage))
                        return reader[0].RowsAffected;
                    else
                        throw new Exception(reader[0].ErrorMessage);
                }
                else
                    return -1;
            }
        }
        /// <summary>
        /// Execute and get records as native T type
        /// </summary>
        /// <param name="commandText"></param>
        /// <returns></returns>
        private IList<T> ExecuteGet(string commandText)
        {
            using (var cmd = new SqlDatabaseCommand())
            {
                cmd.Connection = Connection;
                cmd.CommandText = commandText;
                SQLDatabaseResultSet[] reader = cmd.ExecuteReader(ExtendedResultSet);
                return new EntityMapper().Map<T>(reader[0]);
            }
        }
        /// <summary>
        /// Get list of items by specifying the type
        /// </summary>
        /// <param name="commandText"></param>
        /// <returns></returns>
        private IList<TEntity> ExecuteGet<TEntity>(string commandText)
            where TEntity : class, new()
        {
            using (var cmd = new SqlDatabaseCommand())
            {
                cmd.Connection = Connection;
                cmd.CommandText = commandText;
                SQLDatabaseResultSet[] reader = cmd.ExecuteReader(ExtendedResultSet);
                return new EntityMapper().Map<TEntity>(reader[0]);
            }
        }
        /// <summary>
        /// Pass filter to get records in entity format
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        private IList<TEntity> ExecuteGet<TEntity>(IFilter<TEntity> filter)
            where TEntity : class, new()
        {
            using (var cmd = new SqlDatabaseCommand())
            {
                cmd.Connection = Connection;
                cmd.CommandText = filter.Query;
                SQLDatabaseResultSet[] reader = cmd.ExecuteReader(ExtendedResultSet);
                return new EntityMapper().Map<TEntity>(reader[0]);
            }
        }

        private IList<TEntity> ExecuteGet<TEntity>(SQLDatabaseResultSet reader)
            where TEntity : class, new()
        {
            return new EntityMapper().Map<TEntity>(reader);
        }
        private IList<PropertyInfo> GetPropertyInfoList(T entity)
        {
            return entity.GetType().GetProperties()
                .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DBColumnAttribute)) != null).ToList();
        }
        private IList<PropertyInfo> GetPropertyInfoList<TEntity>(TEntity entity)
        {
            return entity.GetType().GetProperties()
                .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DBColumnAttribute)) != null).ToList();
        }

        private object GetActualValue(object memberValue)
        {
            object Value = new object();

            string typename = ClrToDBType(memberValue.GetType().Name);

            if (typename.Equals("Text"))
                Value = string.Format("'{0}'", memberValue.ToString().Replace("'", "''"));
            else if (typename.Equals("Integer"))
                Value = string.Format("{0}", memberValue);
            else if (typename.Equals("Real"))
                Value = string.Format("{0}", memberValue);
            else if (typename.Equals("None"))
                Value = string.Format("'{0}'", memberValue).ToString().Replace("'", "''");
            else
                Value = string.Format("'{0}'", memberValue.ToString().Replace("'", "''"));

            return Value;
        }

        /// <summary>
        /// Creates a table based on class name, e.g Class Databases then Databases will be table name.
        /// </summary>
        public void CreateTable()
        {
            IList<PropertyInfo> propertyInfos = GetPropertyInfoList(_DatabaseObjectT);
            if (propertyInfos != null)
            {
                CreateTable(_DatabaseObjectT);
            }
        }
        /// <summary>
        /// Creates a table based on class object name
        /// </summary>
        /// <param name="entity"></param>
        public void CreateTable(T entity)
        {

            StringBuilder primarykeys = new StringBuilder();
            StringBuilder foreignkeys = new StringBuilder();
            StringBuilder columns = new StringBuilder();
            StringBuilder Unique = new StringBuilder();

            IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);

            bool IsFirst = true;
            bool HasPrimaryKey = false;

            foreach (PropertyInfo i in propertyInfos)
            {

                if (i.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute ca)
                {
                    if (!IsFirst)
                    {
                        columns.Append(",");
                    }
                    columns.Append(string.Format("{0} {1} ", i.Name, ClrToDBType(i.PropertyType.Name)));

                    if ((ca.NotNull) && (!ca.PrimaryKey))
                        columns.Append(" NOT NULL ");

                    if ((ca.Unique) && (!ca.PrimaryKey))
                        columns.Append(" UNIQUE ");

                    if (ca.PrimaryKey)
                    {
                        HasPrimaryKey = true;
                        if (ca.AutoIncrement)
                            columns.Append(" Primary Key AutoIncrement NOT NULL ");
                        else
                            columns.Append(" Primary Key NOT NULL ");
                    }


                    if (ca.CombinedPrimaryKey)
                    {
                        if (HasPrimaryKey)
                            throw new Exception("Primary key already defined. A table cannot have both column level primary key and table level primary keys.");
                        else
                            primarykeys.Append(i.Name + ",");
                    }


                    if ((ca.ForeignKey) && (!string.IsNullOrWhiteSpace(ca.ForeignKeyTable)) && (!string.IsNullOrWhiteSpace(ca.ForeignKeyColumn)))
                    {
                        foreignkeys.Append(string.Format(" , FOREIGN KEY ([{0}]) REFERENCES [{1}]([{2}]) ", i.Name, ca.ForeignKeyTable, ca.ForeignKeyColumn));
                    }

                }
                IsFirst = false;
            }

            if (columns.ToString() != string.Empty)
            {

                if (columns.ToString().EndsWith(","))
                    columns.Remove(columns.Length - 1, 1);


                string primarykey = string.Empty;

                if ((!string.IsNullOrWhiteSpace(primarykeys.ToString()))
                    && (columns.ToString().IndexOf("Primary Key") == -1))
                {

                    primarykeys.Remove(primarykeys.Length - 1, 1);
                    primarykey = ", PRIMARY KEY (" + primarykeys.ToString() + ") ";
                }
                else
                {
                    primarykey = string.Empty;
                }


                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("CREATE TABLE IF NOT EXISTS [{0}] ( {1} {2} {3} );"
                    , entity.GetType().Name, columns, primarykey, foreignkeys.ToString()));

                              
                Execute(qry.ToString(), false);

            }


        }

        /// <summary>
        /// drop a table using class name
        /// </summary>        
        public void DropTable()
        {
            DropTable(_DatabaseObjectT);
        }

        /// <summary>
        /// drop a table using class object name
        /// </summary>
        /// <param name="entity"></param>
        public void DropTable(T entity)
        {
            StringBuilder qry = new StringBuilder();
            qry.Append(string.Format("DROP TABLE IF EXISTS [{0}];", entity.GetType().Name));
            Execute(qry.ToString(), false);
        }


        /// <summary>
        /// Insert a single record into table and returns new RowId or new value of auto increment column
        /// only if table contain autoincrement column.
        /// </summary>
        /// <param name="entity"></param>
        public long Add(T entity)
        {
            long identity = 0;
            bool hasIdentity = false;

            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();

            IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);

            foreach (PropertyInfo i in propertyInfos)
            {

                if (i.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute ca)
                {
                    if (!ca.AutoIncrement)
                    {
                        columns.Append(string.Format("[{0}],", i.Name));
                        values.Append(string.Format("{0},",
                               i.GetValue(entity) == null ? "NULL" : string.Format("{0}", GetActualValue(i.GetValue(entity)))));
                    }
                    else
                    {
                        hasIdentity = true;
                    }
                }
            }

            if (columns.ToString() != string.Empty)
            {

                columns.Remove(columns.Length - 1, 1);
                values.Remove(values.Length - 1, 1);

                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("INSERT INTO [{0}] ( {1} ) VALUES ( {2} );" 
                    , entity.GetType().Name, columns, values));


                identity = hasIdentity ? Execute(qry.ToString(), true) : Execute(qry.ToString());
            }

            return identity;
        }

        /// <summary>
        /// Inserts multiple records into a table
        /// </summary>
        /// <param name="entities"></param>
        public void AddRange(List<T> entities)
        {
            if (!Connection.MultipleActiveResultSets)
                Connection.MultipleActiveResultSets = true;

            StringBuilder qry = new StringBuilder();
            foreach (T entity in entities)
            {
                StringBuilder columns = new StringBuilder();
                StringBuilder values = new StringBuilder();

                IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);

                foreach (PropertyInfo i in propertyInfos)
                {

                    if (i.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute ca)
                    {
                        if (!ca.AutoIncrement)
                        {
                            columns.Append(string.Format("[{0}],", i.Name));

                            values.Append(string.Format("{0},",
                                i.GetValue(entity) == null ? "NULL" : string.Format("{0}", GetActualValue(i.GetValue(entity)))));
                        }
                    }
                }

                if (columns.ToString() != string.Empty)
                {
                    columns.Remove(columns.Length - 1, 1);
                    values.Remove(values.Length - 1, 1);

                    qry.AppendLine(string.Format(
                        "INSERT INTO [{0}] ( {1} ) VALUES ( {2} );"
                        , entity.GetType().Name, columns, values));
                }
            }

            try
            {
                Execute(qry.ToString());
            }
            catch (Exception ex)
            {

                throw ex;
            }

        }

        /// <summary>
        /// Removes a record based on filter        
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public void Remove(IFilter<T> filter)
        {
            RowsAffected = Execute(filter.QueryDelete);
        }
        /// <summary>
        /// Remove all records
        /// </summary>               
        public void RemoveAll<T>()
        {
            Type t = typeof(T);
            RowsAffected = Execute(string.Format("DELETE FROM [{0}]", t.GetType().Name));
        }
        /// <summary>
        /// Pass SQL text to delete
        /// </summary>
        /// <param name="commandText"></param>
        /// <returns></returns>
        public void Remove(string commandText)
        {
            RowsAffected = Execute(commandText);
        }
        
        /// <summary>
        /// Remove records based on entity
        /// </summary>           
        /// <param name="entity"></param>
        public void Remove(T entity)
        {

            StringBuilder clause = new StringBuilder();

            IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);
            foreach (PropertyInfo i in propertyInfos)
            {

                if (i.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute ca)
                {
                    if (clause.ToString() != string.Empty)
                        clause.Append(" AND ");

                    clause.Append(string.Format("[{0}] = {1}", i.Name, GetActualValue(i.GetValue(entity))));
                }
            }

            if (clause.ToString() != string.Empty)
            {

                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("DELETE FROM [{0}] WHERE {1} ;"
                    , entity.GetType().Name, clause));

                RowsAffected = Execute(qry.ToString());
            }
        }

        /// <summary>
        /// Remove single item based on id (primary key)
        /// </summary>
        /// <param name="id"></param>
        public void RemoveById(object id)
        {
            T entity = new T();
            StringBuilder clause = new StringBuilder();

            IList<PropertyInfo> pInfos = GetPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                if (pi.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute pk && pk.PrimaryKey)
                {
                    clause.Append(string.Format("[{0}] = {1} ", pi.Name, GetActualValue(id)));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("DELETE FROM [{0}] WHERE {1}", entity.GetType().Name, clause));
                RowsAffected = Execute(qry.ToString());
            }

        }

        /// <summary>
        /// Updates single entity
        /// </summary>
        /// <param name="entity"></param>
        public void Update(T entity)
        {
            StringBuilder columns = new StringBuilder();
            StringBuilder clause = new StringBuilder();

            IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);
            foreach (PropertyInfo i in propertyInfos)
            {

                if (i.GetCustomAttribute(typeof(DBColumnAttribute)) is DBColumnAttribute ca)
                {
                    if (!ca.PrimaryKey)
                    {
                        columns.Append(string.Format("[{0}] = {1},", i.Name,
                            i.GetValue(entity) == null ? "NULL" : GetActualValue(i.GetValue(entity))));
                    }
                    else
                    {
                        clause.Append(string.Format("[{0}] = {1}", i.Name, GetActualValue(i.GetValue(entity))));
                    }
                }
            }

            if (columns.ToString() != string.Empty)
            {
                if (columns.ToString().EndsWith(","))
                    columns.Remove(columns.Length - 1, 1);

                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("UPDATE [{0}] SET {1} WHERE {2};"
                    , entity.GetType().Name, columns, clause));

                RowsAffected = Execute(qry.ToString());
            }
        }
        /// <summary>
        /// Updates mutiple entities in single query
        /// </summary>
        /// <param name="entities"></param>
        public void UpdateRange(IList<T> entities)
        {
            StringBuilder qry = new StringBuilder();
            foreach (T entity in entities)
            {
                StringBuilder columns = new StringBuilder();
                StringBuilder clause = new StringBuilder();

                IList<PropertyInfo> propertyInfos = GetPropertyInfoList(entity);
                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DBColumnAttribute)) as DBColumnAttribute;

                    if (ca != null)
                    {
                        if (!ca.PrimaryKey)
                        {
                            columns.Append(string.Format("[{0}] = {1},", i.Name,
                                i.GetValue(entity) == null ? "NULL" : string.Format("'{0}'", GetActualValue(i.GetValue(entity)))));
                        }
                        else
                        {
                            clause.Append(string.Format("[{0}] = {1}", i.Name, GetActualValue(i.GetValue(entity))));
                        }
                    }
                }

                if (columns.ToString() != string.Empty)
                {
                    if (columns.ToString().EndsWith(","))
                        columns.Remove(columns.Length - 1, 1);

                    qry.AppendLine(string.Format("UPDATE [{0}] SET {1} WHERE {2};"
                        , entity.GetType().Name, columns, clause));
                }

            }

            RowsAffected = Execute(qry.ToString());
        }

        /// <summary>
        /// Find single item using primary key
        /// </summary>
        /// <param name="id"></param>
        public T GetById(object id)
        {
            T entity = new T();
            StringBuilder clause = new StringBuilder();

            IList<PropertyInfo> pInfos = GetPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                var pk = pi.GetCustomAttribute(typeof(DBColumnAttribute)) as DBColumnAttribute;
                if (pk != null && pk.PrimaryKey)
                {
                    clause.Append(string.Format("[{0}]='{1}'", pi.Name, id));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("SELECT * FROM [{0}] WHERE {1}", entity.GetType().Name, clause));
                var _entities = ExecuteGet(qry.ToString());
                if (_entities != null && _entities.Count > 0)
                    entity = _entities[0];
            }


            return entity;
        }

        public IList<T> GetByNaturalJoin(object Table1, object Table2) 
        {
            T entity = new T();
            var rc = ExecuteGet<T>(string.Format("SELECT * FROM [{0}] NATURAL JOIN [{1}] ", Table1.GetType().Name, Table2.GetType().Name));
            return rc;
        }
        
        /// <summary>
        /// Find multiple items using primary keys
        /// </summary>
        /// <param name="id"></param>
        public IList<T> Find(IEnumerable<object> ids)
        {
            IList<T> entities = new List<T>();
            StringBuilder clause = new StringBuilder();

            var entity = new T();
            IList<PropertyInfo> pInfos = GetPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                var pk = pi.GetCustomAttribute(typeof(DBColumnAttribute)) as DBColumnAttribute;
                if (pk != null && pk.PrimaryKey)
                {
                    string _ids = string.Empty;
                    foreach (var id in ids)
                    {
                        if (_ids != string.Empty)
                            _ids = _ids + ",";

                        _ids = _ids + id.ToString();
                    }

                    clause.Append(string.Format("[{0}] IN ({1})", pi.Name, _ids));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("SELECT * FROM [{0}] WHERE {1}", entity.GetType().Name, clause));
                entities = ExecuteGet(qry.ToString());
            }

            return entities;
        }



        #region Entity Mapper

        public class EntityMapper
        {
            /// <summary>
            /// maps object / entity with data
            /// </summary>
            /// <param name="reader"></param>
            public IList<T> Map<T>(SQLDatabaseResultSet reader)
                where T : class, new()
            {
                IList<T> collection = new List<T>();
                for (int r = 0; r < reader.RowCount; r++)
                {
                    T obj = new T();
                    int c = 0;
                    foreach (PropertyInfo i in obj.GetType().GetProperties()
                        .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DBColumnAttribute)) != null).ToList())
                    {

                        try
                        {
                            var ca = i.GetCustomAttribute(typeof(DBColumnAttribute));

                            if (ca != null)
                            {                                
                                try
                                {
                                    if (ColumnValue(reader, r, i.Name) != DBNull.Value)
                                        i.SetValue(obj, ColumnValue(reader, r, i.Name));
                                } catch(ArgumentException) // Argument Exception occurs when data types don't match.
                                {
                                    // Try changing type of other wise next Exception ex will still catch the exception.
                                    i.SetValue(obj, Convert.ChangeType(ColumnValue(reader, r, i.Name), i.PropertyType));
                                }   
                            }
                        }
                        catch (Exception ex)
                        {                            
                            throw ex;
                        }
                        c++;
                    }

                    collection.Add(obj);
                }

                return collection;
            }

            /// <summary>
            /// maps object / entity with data
            /// </summary>
            /// <param name="reader"></param>
            public IList<T> MapWithoutAttributes<T>(SQLDatabaseResultSet reader)
                where T : class, new()
            {
                IList<T> collection = new List<T>();
                for (int r = 0; r < reader.RowCount; r++)
                {
                    T obj = new T();
                    int c = 0;
                    foreach (PropertyInfo i in obj.GetType().GetProperties().ToList())
                    {
                        try
                        {                                               
                            if (ColumnValue(reader, r, i.Name) != DBNull.Value)
                                i.SetValue(obj, ColumnValue(reader, r, i.Name));
                            else
                                i.SetValue(obj, null);

                        }
                        catch (Exception ex)
                        {                         
                            throw ex;
                        }
                        c++;
                    }

                    collection.Add(obj);
                }

                return collection;
            }

            /// <summary>
            /// returns single value of specified row and column index
            /// </summary>
            /// <param name="rs"></param>
            /// <param name="RowIndex"></param>
            /// <param name="ColumnIndex"></param>
            public object ColumnValue(SQLDatabaseResultSet rs, int RowIndex, int ColumnIndex)
            {
                if ((RowIndex > -1) && (ColumnIndex > -1))
                {
                    if (RowIndex > rs.RowCount)
                        throw new Exception("Row index is out of range");

                    if (ColumnIndex > rs.ColumnCount)
                        throw new Exception("Column index is out of range");

                    return rs.Rows[RowIndex][ColumnIndex];
                }
                else
                {
                    throw new Exception("Invalid Row or Column index");
                }

            }

            /// <summary>
            /// returns single value of specified row index and column name
            /// </summary>
            /// <param name="rs"></param>
            /// <param name="RowIndex"></param>
            /// <param name="ColumnName"></param>
            public object ColumnValue(SQLDatabaseResultSet rs, int RowIndex, string ColumnName)
            {
                if ((RowIndex > -1) && (!string.IsNullOrWhiteSpace(ColumnName)))
                {
                    if (RowIndex > rs.RowCount)
                        throw new Exception("Row index is out of range");

                    int ColIndex = -1;
                    foreach (string col in rs.Columns)
                    {
                        ColIndex++;
                        if (col.Equals(ColumnName, StringComparison.CurrentCultureIgnoreCase))
                            break;
                    }

                    if ((ColIndex == -1) || (ColIndex > rs.ColumnCount))
                        throw new Exception(string.Format("Column {0} not found.", ColumnName));

                    return rs.Rows[RowIndex][ColIndex];

                }
                else
                {
                    throw new Exception("Column name and valid row index are required.");
                }

            }

        }


        #endregion

        #region Interfaces
        public interface IFilter<T> where T : class, new()
        {
            string EntityName { get; }
            string Query { get; }
            string Clause { get; }
            string QueryDelete { get; }

            void Add(Expression<Func<T, object>> memberExpression, object memberValue);
            void Add(Expression<Func<T, object>> memberExpression);
        }

        #endregion

        #region Filter
        public class Filter<T> : IFilter<T> where T : class, new()
        {
            public string EntityName { get; private set; }

            private readonly StringBuilder _Query;
            private readonly StringBuilder _OrderByClause;
            private string _LimitOffset = string.Empty;

            public Filter()
            {
                _Query = new StringBuilder();
                _OrderByClause = new StringBuilder();
                EntityName = typeof(T).Name;
            }

            private object GetMemberValue(object memberValue)
            {
                object Value = new object();

                string typename = ClrToDBType(memberValue.GetType().Name);

                if (typename.Equals("Text"))
                    Value = string.Format("'{0}'", memberValue.ToString().Replace("'", "''"));
                else if (typename.Equals("Integer"))
                    Value = string.Format("{0}", memberValue);
                else if (typename.Equals("Real"))
                    Value = string.Format("{0}", memberValue);
                else if (typename.Equals("None"))
                    Value = string.Format("'{0}'", memberValue.ToString().Replace("'", "''"));
                else
                    Value = string.Format("'{0}'", memberValue.ToString().Replace("'", "''"));

                return Value;
            }

            public void Add(Expression<Func<T, object>> memberExpression, object memberValue)
            {
                WhereWithAND(memberExpression, memberValue);
            }
            public void Add(Expression<Func<T, object>> memberExpression)
            {
                WhereWithAND(memberExpression);
            }
            /// <summary>
            /// create new filter using WHERE clause AND
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void WhereWithAND(Expression<Func<T, object>> memberExpression, object memberValue)
            {

                if (_Query.ToString() != string.Empty)
                    _Query.Append(" AND ");
                           
                _Query.Append(string.Format(" [{0}] = {1}", NameOf(memberExpression), memberValue == null ? "NULL" : GetMemberValue(memberValue)));
            }

            public void WhereWithAND(Expression<Func<T, object>> memberExpression)
            {

                if (_Query.ToString() != string.Empty)
                    _Query.Append(" AND ");

               
                UnaryExpression unrbody = (UnaryExpression)memberExpression.Body;
                BinaryExpression BinaryExp = (BinaryExpression)unrbody.Operand;
                MemberExpression MemberExp = (MemberExpression)BinaryExp.Left;
                           
                string OperatorSymbol = "=";
                switch (BinaryExp.NodeType)
                {
                    case ExpressionType.GreaterThan:
                        OperatorSymbol = ">";
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        OperatorSymbol = ">=";
                        break;
                    case ExpressionType.LessThan:
                        OperatorSymbol = "<";
                        break;
                    case ExpressionType.LessThanOrEqual:
                        OperatorSymbol = "<=";
                        break;
                    case ExpressionType.Equal:
                        OperatorSymbol = "=";
                        break;
                    case ExpressionType.NotEqual:
                        OperatorSymbol = "!=";
                        break;
                    case ExpressionType.Default:
                        OperatorSymbol = "=";
                        break;
                    default:
                        OperatorSymbol = "=";
                        break;
                }
                
                _Query.Append(string.Format(" [{0}] {1} {2}", NameOf(memberExpression), OperatorSymbol, ValueOf(memberExpression)));
            }

            /// <summary>
            /// create new filter using WHERE clause AND Or
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void WhereWithOR(Expression<Func<T, object>> memberExpression, object memberValue)
            {

                if (_Query.ToString() != string.Empty)
                    _Query.Append(" OR ");

                _Query.Append(string.Format(" [{0}] = {1}", NameOf(memberExpression), memberValue == null ? "NULL" : GetMemberValue(memberValue)));

            }
            public void WhereWithOR(Expression<Func<T, object>> memberExpression)
            {

                if (_Query.ToString() != string.Empty)
                    _Query.Append(" OR ");


                UnaryExpression unrbody = (UnaryExpression)memberExpression.Body;
                BinaryExpression BinaryExp = (BinaryExpression)unrbody.Operand;
                MemberExpression MemberExp = (MemberExpression)BinaryExp.Left;
                string OperatorSymbol = "=";
                switch (BinaryExp.NodeType)
                {
                    case ExpressionType.GreaterThan:
                        OperatorSymbol = ">";
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        OperatorSymbol = ">=";
                        break;
                    case ExpressionType.LessThan:
                        OperatorSymbol = "<";
                        break;
                    case ExpressionType.LessThanOrEqual:
                        OperatorSymbol = "<=";
                        break;
                    case ExpressionType.Equal:
                        OperatorSymbol = "=";
                        break;
                    case ExpressionType.NotEqual:
                        OperatorSymbol = "!=";
                        break;
                    case ExpressionType.Default:
                        OperatorSymbol = "=";
                        break;
                    default:
                        OperatorSymbol = "=";
                        break;
                }
                _Query.Append(string.Format(" [{0}] {1} {2}", NameOf(memberExpression), OperatorSymbol,  ValueOf(memberExpression)));
                //_Query.Append(string.Format(" [{0}] {1} {2}", MemberExp.Member.Name, OperatorSymbol, BinaryExp.Right));
            }
            /// <summary>
            /// create new filter using like statement
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void Contains(Expression<Func<T, object>> memberExpression, object memberValue)
            {

                if (_Query.ToString() != string.Empty)
                    _Query.Append(" AND ");

                _Query.Append(string.Format(" [{0}] LIKE {1}", NameOf(memberExpression), memberValue == null ? "NULL" : string.Format("'%{0}%'", memberValue)));

            }

            /// <summary>
            /// Add sql ORDER BY 
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void OrderBy(Expression<Func<T, object>> memberExpression, object sortOrder)
            {                                
                if (_OrderByClause.ToString().IndexOf(" ORDER BY ") > -1)
                    _OrderByClause.Append(" , ");
                else
                    _OrderByClause.Append(" ORDER BY ");
                

                _OrderByClause.Append(string.Format("{0} {1}", NameOf(memberExpression), sortOrder == null ? "ASC" : string.Format("{0}", sortOrder)));
            }

            /// <summary>
            /// Add sql ORDER BY 
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void OrderBy(Expression<Func<T, object>> memberExpression)
            {
                OrderBy(memberExpression);
            }

            /// <summary>
            /// Add sql ORDER BY Descending
            /// </summary>
            /// <param name="memberExpression"></param>
            /// <param name="memberValue"></param>
            public void OrderByDescending(Expression<Func<T, object>> memberExpression)
            {
                OrderBy(memberExpression, "DESC");
            }

            /// <summary>
            /// Add sql LIMIT and OFFSET to reduce the number of records returned.
            /// </summary>
            /// <param name="Limit"></param>
            /// <param name="OffSet"></param>
            public void LimitAndOffSet(int Limit, int OffSet = 0)
            {
                _LimitOffset = string.Format("LIMIT {0} OFFSET {1}", Limit, OffSet);
            }

            
            /// <summary>
            /// Returns SELECT statement with WHERE clause based on the expression
            /// </summary>
            public string Query
            {
                get
                {
                    return string.Format("SELECT * FROM [{0}] {1} {2} {3} {4};"
                        , EntityName
                        , _Query.ToString() == string.Empty ? string.Empty : "WHERE"
                        , _Query.ToString()
                        , _OrderByClause.ToString() == string.Empty ? string.Empty : _OrderByClause.ToString()
                        , _LimitOffset == string.Empty ? string.Empty : _LimitOffset);
                }
            }

            /// <summary>
            /// Returns where clause from sql query
            /// </summary>
            public string Clause
            {
                get
                {
                    return string.Format("{0} {1} ;"
                        , _Query.ToString() == string.Empty ? string.Empty : "WHERE"
                        , _Query.ToString());
                }
            }

            /// <summary>
            /// Returns DELETE statement with WHERE clause based on the expression
            /// </summary>
            public string QueryDelete
            {
                get
                {
                    return string.Format("DELETE FROM [{0}] {1} {2};"
                        , EntityName
                        , _Query.ToString() == string.Empty ? string.Empty : "WHERE"
                        , _Query.ToString());
                }
            }

            private string NameOf(Expression<Func<T, object>> exp)
            {
                MemberExpression body = exp.Body as MemberExpression;

                if (body == null)
                {
                    UnaryExpression ubody = (UnaryExpression)exp.Body;
                    body = ubody.Operand as MemberExpression;
                }

                if (body == null)
                {
                    UnaryExpression unrbody = (UnaryExpression)exp.Body;
                    BinaryExpression BinaryExp = (BinaryExpression)unrbody.Operand;
                    MemberExpression MemberExp = (MemberExpression)BinaryExp.Left;
                    body = MemberExp;
                }
                return body.Member.Name;
            }

            private object ValueOf(Expression<Func<T, object>> exp)
            {
                UnaryExpression unrbody = (UnaryExpression)exp.Body;
                BinaryExpression BinaryExp = (BinaryExpression)unrbody.Operand;

                return BinaryExp.Right;
            }
        }

        #endregion


        private static string ClrToDBType(string ClrType)
        {
            ClrType = ClrType.ToLowerInvariant().Trim();

            switch (ClrType)
            {
                case "dateTimeOffset":
                case "dateTime":
                case "char":
                case "string":
                    return "Text";
                case "double":
                case "decimal":
                case "float":
                    return "Real";
                case "integer":
                case "int":
                case "int16":
                case "int32":
                case "int64":
                case "uint":
                case "uint16":
                case "uint32":
                case "uint64":
                case "bool":
                case "boolean":
                    return "Integer";
                default:
                    return "None";
            }
        }
        private static string DBTypeToClrType(string DBType)
        {
            DBType = DBType.ToLowerInvariant().Trim();

            switch (DBType)
            {
                case "Text":
                    return "string";
                case "Real":
                    return "double";
                case "integer":
                    return "Int64";
                default:
                    return "object";
            }
        }

    }
}

