using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using SQLDatabase.Net.SQLDatabaseClient;


// Serializable : https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/serialization/index
namespace SQLDatabase.Net.Core.Examples
{
    // Example class Settings Example should be removed from production
    //Start of Example Class
    [Serializable]
    class SettingsExample
    {
        public int Id { get; set; } 
        public string Name { get; set; }
        public DateTime Joined { get; set; }
        public List<object> Pictures { get; set; } = new List<object>();
    }

    //End of Example Class


    sealed class BindChanger : System.Runtime.Serialization.SerializationBinder
    {
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type typeToDeserialize = null;
            string currentAssembly = Assembly.GetExecutingAssembly().FullName;
            typeToDeserialize = Type.GetType(string.Format("{0}, {1}", typeName, currentAssembly));
            return typeToDeserialize;
        }
    }
    class KeyValueStore
    {
        /// <summary>
        /// Converts and object to byte array
        /// </summary>
        /// <param name="obj"></param>
        public static byte[] ObjectToByteArray(Object obj)
        {
            try
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.AssemblyFormat = System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple;
                using (var ms = new MemoryStream())
                {
                    bf.Serialize(ms, obj);
                    return ms.ToArray();
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

        }

        /// <summary>
        /// Converts byte array to object
        /// </summary>
        /// <param name="BytesArray"></param>
        public Object ObjectFromByteArray(byte[] BytesArray)
        {
            using (var memStream = new MemoryStream())
            {
                BinaryFormatter binForm = new BinaryFormatter();
                binForm.Binder = new BindChanger();
                memStream.Write(BytesArray, 0, BytesArray.Length);
                memStream.Seek(0, 0);
                var obj = binForm.Deserialize(memStream);

                return obj;
            }
        }

        /// <summary>
        /// Converts byte array to object
        /// </summary>
        /// <param name="BytesArray"></param>
        public T ObjectFromByteArray<T>(byte[] BytesArray)
        {
            if (BytesArray == null)
                return default(T);

            try
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.Binder = new BindChanger();
                using (MemoryStream ms = new MemoryStream(BytesArray))
                {
                    ms.Write(BytesArray, 0, BytesArray.Length);
                    ms.Seek(0, 0);
                    return (T)bf.Deserialize(ms);
                }
            }
            catch (Exception)
            {                
                return default(T);
            }
        }


        private string ConnectionStringUri1 = string.Empty;
        SqlDatabaseConnection cnn = new SqlDatabaseConnection();
        private bool TableExistsOrCreated = false;
        private static object LockObject = new object();
        public string DatabaseFile { get; set; }
        

        void OpenConnectionAndCreateTableIfNotExists()
        {
            if (string.IsNullOrWhiteSpace(DatabaseFile))
                throw new Exception("DatabaseFile property must be set");

            if (cnn.State == System.Data.ConnectionState.Closed)
            {
                cnn.ConnectionString = "SchemaName=db;uri=" + (DatabaseFile == "@memory" ? "@memory;" : "file://" + DatabaseFile + ";");
                cnn.Open();
            }

            if (cnn.State == System.Data.ConnectionState.Open)
            {               
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "Create Table If Not Exists KeyValueStore(CollectionName Text, Key Text, Value BLOB);";                    
                    cmd.CommandText += "Create Index If Not Exists IdxKeyValueStore ON KeyValueStore(CollectionName, Key);";
                    cmd.ExecuteNonQuery();
                    TableExistsOrCreated = true;
                }
            } else
            {
                throw new Exception("Unable to open connection");
            }
        }

        public int AddOrUpdate(string CollectionName, string Key, object Value)
        {
            int RowsAffected = -1;

            if (!TableExistsOrCreated)
            {
                OpenConnectionAndCreateTableIfNotExists();
            }

            if ((string.IsNullOrWhiteSpace(CollectionName)) || (string.IsNullOrWhiteSpace(Key)))
                throw new Exception("CollectionName and Key are required.");
                

            byte[] b = new byte[0];

            if (Value != null)
            {
                if (!(Value is byte[]))
                    b = ObjectToByteArray(Value);
                else
                    b = (byte[])Value;
            }

            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
            {
                cmd.CommandText = "SELECT RowId FROM KeyValueStore WHERE CollectionName = @CollectionName AND Key = @Key LIMIT 1;";
                cmd.Parameters.AddWithValue("@CollectionName", CollectionName);
                cmd.Parameters.AddWithValue("@Key", Key);
                cmd.Parameters.AddWithValue("@Value", b);
                object ObjRowId = cmd.ExecuteScalar();

                lock (LockObject)
                {     
                    if (ObjRowId != null)
                    {
                        int Id = int.Parse(ObjRowId.ToString());

                        cmd.CommandText = "UPDATE KeyValueStore SET Value = @Value WHERE RowId = (SELECT RowId FROM KeyValueStore WHERE CollectionName = @CollectionName AND Key = @Key LIMIT 1);";
                        RowsAffected = cmd.ExecuteNonQuery();
                    } else
                    {
                        cmd.CommandText = "INSERT INTO KeyValueStore(CollectionName, Key, Value) VALUES(@CollectionName, @Key, @Value);";
                        RowsAffected = cmd.ExecuteNonQuery();
                    }
                }
            }

            return RowsAffected;
        }

        public int Delete(string CollectionName, string Key)
        {            

            if (!TableExistsOrCreated)
            {
                OpenConnectionAndCreateTableIfNotExists();
            }

            if ((string.IsNullOrWhiteSpace(CollectionName)) || (string.IsNullOrWhiteSpace(Key)))
                throw new Exception("CollectionName and Key are required.");

            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
            {
                cmd.CommandText = "DELETE FROM KeyValueStore WHERE RowId = (SELECT RowId FROM KeyValueStore WHERE CollectionName = @CollectionName AND Key = @Key LIMIT 1);";
                cmd.Parameters.AddWithValue("@CollectionName", CollectionName);
                cmd.Parameters.AddWithValue("@Key", Key);
                return cmd.ExecuteNonQuery();
            }
        }

        public object Get(string CollectionName, string Key)
        {

            if (!TableExistsOrCreated)
            {
                OpenConnectionAndCreateTableIfNotExists();
            }

            if ((string.IsNullOrWhiteSpace(CollectionName)) || (string.IsNullOrWhiteSpace(Key)))
                throw new Exception("CollectionName and Key are required.");

            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
            {
                cmd.CommandText = "SELECT Value FROM KeyValueStore WHERE CollectionName = @CollectionName AND Key = @Key LIMIT 1;";
                cmd.Parameters.AddWithValue("@CollectionName", CollectionName);
                cmd.Parameters.AddWithValue("@Key", Key);
                byte[] b = (byte[])cmd.ExecuteScalar();
                if (b != null)
                    return ObjectFromByteArray(b);
                else
                    return b;
            }
        }

        public Dictionary<string, object> GetAll(string CollectionName)
        {
            Dictionary<string, object> KeyValuePairs = new Dictionary<string, object>();
            if (!TableExistsOrCreated)
            {
                OpenConnectionAndCreateTableIfNotExists();
            }

            if (string.IsNullOrWhiteSpace(CollectionName))
                throw new Exception("CollectionName is required.");

            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
            {
                cmd.CommandText = "SELECT Key, Value FROM KeyValueStore WHERE CollectionName = @CollectionName;";
                cmd.Parameters.AddWithValue("@CollectionName", CollectionName);
                SqlDatabaseDataReader dr = cmd.ExecuteReader();
                while (dr.Read())
                {                    
                    byte[] b = (byte[])dr["Value"];
                    if (b != null && b.Length > 1)
                        KeyValuePairs[dr["Key"].ToString()] = ObjectFromByteArray(b);
                    else
                        KeyValuePairs[dr["Key"].ToString()] = null;
                }                
            }

            return KeyValuePairs;
        }
    }
}

