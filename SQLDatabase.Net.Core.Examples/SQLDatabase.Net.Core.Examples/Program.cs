using System;
using System.IO;
using System.Data;
using System.Threading.Tasks;
using System.Text;
using System.Linq;
using SQLDatabase.Net.SQLDatabaseClient;
using SQLDatabase.Net.ORMClient;
using System.Dynamic;
using System.Collections.Generic;
namespace SQLDatabase.Net.Core.Examples
{
    // TODO
    // Transaction IsActive
    // SqlDatabaseReader.HasRows is incorrect
    // Connection. GetSchema does not permit schema name
    // Connection. GetSchema should not return any column or values if it does not exists.

    class Program
    {

        //holds example database file name.
        static string ExampleDatabaseFile = "Orders.db";

        //Extended Result Set and Active ResultSet are 
        static SqlDatabaseConnectionStringBuilder cb = new SqlDatabaseConnectionStringBuilder();

        static void Main(string[] args)
        {

            Console.WriteLine("**** Example and tests for wwww.sqldatabase.net ****\n");
            Console.WriteLine();

            Console.WriteLine("* Example for key value like behavior to store raw bytes and .net objects");
            KeyValueStoreLikeBehavior();
            Console.WriteLine();

            Console.WriteLine("* KeyValueStoreLikeBehavior example completed. Press Enter key to start example of how to add images/files.");
            Console.ReadLine();

            AddImage();
            Console.WriteLine();
            Console.WriteLine("* Image / File example completed. Press Enter key to start Import Export CSV example.");
            Console.WriteLine();
            Console.ReadLine();

            ImportExportCSV();
            Console.WriteLine();
            Console.WriteLine("* Import Export CSV example completed. Press Enter key to start ORM client example.");
            Console.ReadLine();
            Console.WriteLine("Starting ORM Based Examples...");
            Console.WriteLine();
            ORMClientExamples.StartExamples();
            

            Console.WriteLine("* ORM Client examples completed. Press Enter key to start SQL based examples.");
            Console.WriteLine();
            Console.ReadLine();
            Console.WriteLine("Starting SQL Based Examples...");
            Console.WriteLine();
            SQLExamples.StartExamples();


            Console.WriteLine("* ORM Client and SQL examples completed. press Enter key to see meta data.");
            Console.ReadLine();
            MetaCollections();
            Console.WriteLine("* All Examples Executed, press enter key to exit.");
            Console.ReadLine();
            Environment.Exit(0);
        }

        static void MetaCollections()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("uri=@memory"))
            {
                cnn.Open();

                //Available Meta Data Collections in this connection.
                // Let's use foreach loop to get all the meta collections
                foreach (DataRow r in cnn.GetSchema("METADATACOLLECTIONS").Rows)
                {
                    if (r["NumberOfRestrictions"].Equals(0))
                    {
                        Console.WriteLine();
                        Console.WriteLine("Starting... {0}", r["CollectionName"].ToString());
                        Console.WriteLine();
                        //Load another datatable CollectionDT with values
                        DataTable CollectionDT = cnn.GetSchema(r["CollectionName"].ToString());
                        foreach (DataColumn column in CollectionDT.Columns)
                        {
                            Console.Write(column.ColumnName + "\t");
                        }
                        Console.WriteLine(Environment.NewLine);
                        foreach (DataRow row in CollectionDT.Rows)
                        {
                            foreach (DataColumn column in CollectionDT.Columns)
                            {
                                Console.Write(row[column.ColumnName] + "\t");
                            }
                            Console.WriteLine();
                        }
                    }     
                }
            }
        }

        static void ImportExportCSV()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("uri=file://" + ExampleDatabaseFile))
            {
                cnn.DatabaseFileMode = DatabaseFileMode.OpenIfExists;
                cnn.Open();

                if (File.Exists("Products.csv"))
                    File.Delete("Products.csv");

                CSVFile.CsvImportExport importExport = new CSVFile.CsvImportExport(cnn, "Products", "");

                //Export Example
                int rowcount = importExport.ExportTable("Products.csv", false);
                Console.WriteLine("Number of Rows Imported : {0}", rowcount);

                //Import Example
                importExport.TableName = "Products1";
                rowcount = importExport.ImportTable("Products.csv", true);
                Console.WriteLine("Number of Rows Exported : {0}", rowcount);
            }
        }


        static void AddImage()
        {
            // Following example code inserts and retrive files/images from database
            Console.WriteLine("Example: How to add images or files in database.");
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("SchemaName=db;Uri=@memory")) //We will strore in memory for the example
            {
                cnn.Open();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "CREATE TABLE Images (Id Integer Primary Key, FileName Text, ActualImage BLOB);";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO Images VALUES(@Id, @FileName, @Image);";
                    cmd.Parameters.Add("@Id");
                    cmd.Parameters.Add("@FileName");
                    cmd.Parameters.Add("@Image");

                    //Read the file
                    if (File.Exists("ImageFile.png"))
                    {
                        byte[] byteArray = File.ReadAllBytes("ImageFile.png");
                        // Assign values to parameters.
                        cmd.Parameters["@Id"].Value = 1;
                        cmd.Parameters["@FileName"].Value = "ImageFile.png";
                        cmd.Parameters["@Image"].Value = byteArray;
                        cmd.ExecuteNonQuery(); // Execute insert query

                        Console.WriteLine("Image / File example read..");
                        cmd.CommandText = "SELECT FileName , ActualImage FROM Images WHERE Id = 1 LIMIT 1;";
                        SqlDatabaseDataReader dr = cmd.ExecuteReader();
                        while (dr.HasRows)
                        {
                            while (dr.Read())
                            {
                                // Create Random file name so we won't overwrite the original actual file.
                                string NewFileName = Path.GetRandomFileName() + dr["FileName"].ToString();
                                byte[] FileBytes = (byte[])dr.GetValue(dr.GetOrdinal("ActualImage"));
                                File.WriteAllBytes(NewFileName, FileBytes);
                                if (File.Exists(NewFileName))
                                    Console.WriteLine("File {0} created successfully.", NewFileName);
                                else
                                    Console.WriteLine("Unable to create file {0}.", NewFileName);
                            }
                        }

                    }
                    else
                    {
                        Console.WriteLine("File ImageFile.png not found.");
                    }
                }
            }
        }
        
        static void KeyValueStoreLikeBehavior()
        {
            // Duplicate keys are not permitted.
            // Each key value belongs to a collection.
            // Value is always stored as bytes array.
            // Key is treated as string value.

            // Multiple keys can be supported by extending Get() function and converting bytes to desired object

            //KeValueStore Class have some helper functions to make Sql database act as key value store to store data


            KeyValueStore kvstore = new KeyValueStore();
            kvstore.DatabaseFile = "kvstore.db";

            //SettingsExample Class is Serializable and defined in KeyValueStore.cs but can be anywhere.
            SettingsExample settings = new SettingsExample();
            settings.Id = 1;
            settings.Name = "John Doe";
            settings.Joined = DateTime.Now;
            settings.Pictures.Add(new object());
            settings.Pictures.Add(new object());
            kvstore.AddOrUpdate("Test", "JohnDoe", settings);

            //Read Example
            SettingsExample settingsget = (SettingsExample)kvstore.Get("Test", "JohnDoe");
            Console.WriteLine("settingsget Id: {0}", settingsget.Id);

            //Loop through all keys
            foreach (KeyValuePair<string, object> kv in kvstore.GetAll("Test"))
            {
                SettingsExample se = (SettingsExample)kv.Value;
                Console.WriteLine("Key: {0} \t HasValue: {1}", kv.Key, se == null ? "No" : "Yes");
            }

            //Delete by key
            Console.WriteLine("Number of records deleted: {0}", kvstore.Delete("Test", "JohnDoe"));
        }
    }
}
