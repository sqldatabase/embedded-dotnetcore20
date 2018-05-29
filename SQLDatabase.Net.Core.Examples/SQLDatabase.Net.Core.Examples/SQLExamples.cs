using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Data;
using System.Text;
using SQLDatabase.Net.SQLDatabaseClient;
namespace SQLDatabase.Net.Core.Examples
{
    class SQLExamples
    {

        //ExampleDatabaseFile simply holds example database file name.
        static string ExampleDatabaseFile = "Orders.db";

        //Extended Result Set and Active ResultSet are not enabled by default
        static SqlDatabaseConnectionStringBuilder cb = new SqlDatabaseConnectionStringBuilder();


        public static void StartExamples()
        {
            
            //Console.OutputEncoding = Encoding.UTF8;

            // The Orders.db can also be downloaded from the www.sqldatabase.net website.
            //ExampleDatabaseFile = Path.Combine(
            //    Directory.GetParent(
            //        Path.GetDirectoryName(
            //            AppDomain.CurrentDomain.BaseDirectory)).Parent.Parent.FullName, "Orders.db"
            //            );

            

            if (!File.Exists(ExampleDatabaseFile))
            {
                ExampleDatabaseFile = string.Empty;
                Console.WriteLine("Example database file not found, some example will produce error.");
            }
            else
            {
                Console.WriteLine("SQLDatabase.Net DoNet Standard 2.0 Examples Library Version {0}", new SqlDatabaseConnection().ServerVersion);
                Console.WriteLine("Example Database File: {0}", ExampleDatabaseFile);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine("* Press Any key to start Simple Transaction Example:");
            Console.ReadKey();
            SimpleTransaction();
            Console.WriteLine();

            Console.WriteLine();
            Console.WriteLine("* Press Any key to start Multiple Command Transaction Example:");
            Console.ReadKey();
            MultipleCommandTransaction();
            Console.WriteLine();

            Console.WriteLine("* Press Any key to start SavePoint Example:");
            Console.ReadKey();
            SavePoint();
            Console.WriteLine();


            Console.WriteLine("* Press Any key to start SELECT INSERT UPDATE DELETE Example:");
            Console.ReadKey();
            SIUDOperations(); //Select Insert Update Delete
            Console.WriteLine();

            Console.WriteLine("* Press Any key to start UTF8 Encoding Example:");
            Console.ReadKey();
            MixedLanguagesUTF8();
            Console.WriteLine();

            Console.WriteLine("* Press Any key to start MultiThreading Example:");
            Console.ReadKey();
            MultiThreading();
            Console.WriteLine();

            Console.WriteLine("* Press Any key to start Encrypt Decrypt Example:");
            Console.ReadKey();
            EncryptionDecryption();
            Console.WriteLine();

            Console.WriteLine("* Press Any key to start Multiple Active Result Set Example:");
            Console.ReadKey();
            MarsEnabled();
            Console.WriteLine();


            Console.WriteLine("* Press Any key to start Extended Result Set Example:");
            Console.ReadKey();
            ExtendedResultSet();
            Console.WriteLine();


            Console.WriteLine("* Press Any key to start Track Data Changes Example:");
            Console.ReadKey();
            TrackDataChanges();
            Console.WriteLine();
        }


        static void SavePoint()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;"))
            {
                cnn.Open();
                SqlDatabaseTransaction trans = cnn.BeginTransaction();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.Transaction = trans;
                    cmd.CommandText = "CREATE TABLE SavePointExample (id Integer); ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO SavePointExample VALUES (1); ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "SAVEPOINT a; ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO SavePointExample VALUES (2); ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "SAVEPOINT b; ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO SavePointExample VALUES (3); ";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "SAVEPOINT c; ";
                    cmd.ExecuteNonQuery();

                    //should return 1, 2, 3 since no rollback or released has occured.
                    cmd.CommandText = "SELECT * FROM SavePointExample; ";
                    SqlDatabaseDataReader dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    }

                    //rollback save point to b without committing transaction. The value 3 and savepoint c will be gone.
                    cmd.CommandText = "ROLLBACK TO b"; //b 
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM SavePointExample; ";
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine(""); // line break.
                    }

                    //if we uncomment and release c it wil produce logical error as savepoint c does not exists due to rollback to b.
                    //cmd.CommandText = "RELEASE c"; //c 
                    //cmd.ExecuteNonQuery();

                    cmd.CommandText = "RELEASE b;"; //release b means commit the deffered transaction.
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "SELECT * FROM SavePointExample; ";
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine(""); // line break.
                    }

                    
                    //We can still rollback entire transaction
                    //trans.Rollback();

                    //commit an entire transaction
                    trans.Commit();

                    //If we rollback transaction above regardless of release savepoint (i.e. saving)
                    //following will produce an error that SavePointExample table not found.
                    cmd.CommandText = "SELECT * FROM SavePointExample; ";
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine(""); // line break.
                    }

                }


            }
        }

        static void SimpleTransaction()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;"))
            {

                cnn.Open();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "Create Table If not exists temptable(Id Integer, TextValue Text) ; ";
                    cmd.ExecuteNonQuery();

                    SqlDatabaseTransaction trans = cnn.BeginTransaction();
                    cmd.Transaction = trans;

                    try
                    {
                        for (int i = 0; i < 1000; i++)
                        {
                            cmd.CommandText = "INSERT INTO temptable VALUES (" + i + ", 'AValue" + i + "');";
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (SqlDatabaseException sqlex)
                    {
                        trans.Rollback();
                        Console.WriteLine(sqlex.Message);
                    }
                    finally
                    {
                        trans.Commit();
                    }

                    cmd.CommandText = "SELECT COUNT(*) FROM temptable;";
                    Console.WriteLine("Table Record Count using COUNT(*) : {0}", cmd.ExecuteScalar());

                    // Pure SQL Way of starting and committing transaction.
                    cmd.CommandText = "BEGIN";
                    cmd.ExecuteNonQuery();
                    // Your code and commands can reside here to run in a transaction.
                    cmd.CommandText = "COMMIT"; //ROLLBACK
                    cmd.ExecuteNonQuery();

                    
                }
            }
        }

        static void MultipleCommandTransaction()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;"))
            {
                cnn.Open();

                // Create a table 
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "Create Table If not exists temptable(Id Integer, TextValue Text) ; ";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "Create Table If not exists temptable2(Id Integer, TextValue Text) ; ";
                    cmd.ExecuteNonQuery();
                }

                // Start a transaction on this connection
                SqlDatabaseTransaction trans = cnn.BeginTransaction();

                try
                {
                    using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                    {   
                        cmd.Transaction = trans; // attach this Command object to transaction.
                    
                        for (int i = 0; i < 10; i++)
                        {
                            cmd.CommandText = "INSERT INTO temptable VALUES (" + i + ", 'AValue" + i + "');";
                            cmd.ExecuteNonQuery();
                        }             
                    }

                    // Other processes can run here.
                    // Transaction stays active even after command object is closed and can be attached to other objects.

                    //Create another command object and insert in temptable2 using same transaction.
                    using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                    {
                        cmd.Transaction = trans; // attach this Command object to transaction.

                        for (int i = 0; i < 10; i++)
                        {
                            cmd.CommandText = "INSERT INTO temptable2 VALUES (" + i + ", 'AValue" + i + "');";
                            cmd.ExecuteNonQuery();
                        }
                    }

                }
                catch (SqlDatabaseException sqlex)
                {
                    trans.Rollback();
                    Console.WriteLine(sqlex.Message);
                }
                finally
                {
                    trans.Commit();
                }

                // Let's check the record count.
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "SELECT COUNT(*) FROM temptable;";
                    Console.WriteLine("Record Count temptable : {0}", cmd.ExecuteScalar());
                    cmd.CommandText = "SELECT COUNT(*) FROM temptable2;";
                    Console.WriteLine("Record Count temptable2 : {0}", cmd.ExecuteScalar());
                }

            }
        }

        static void SIUDOperations()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;")) // In Memory database.
            {
                cnn.Open();

                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand())
                {
                    cmd.Connection = cnn;
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS TestTable (Username TEXT PRIMARY KEY, FirstName TEXT, LastName TEXT);";
                    cmd.ExecuteNonQuery();

                    // INSERT
                    cmd.CommandText = "INSERT INTO TestTable VALUES ('jdoe', 'John' , 'DOE');";
                    cmd.ExecuteNonQuery();

                    // SELECT - Load DataTable
                    DataTable dt = new DataTable();
                    cmd.CommandText = "SELECT Username, FirstName, LastName FROM TestTable;";
                    using (SqlDatabaseDataAdapter da = new SqlDatabaseDataAdapter())
                    {
                        da.SelectCommand = cmd;
                        da.Fill(dt);
                    }
                    if (dt.Rows.Count > 0)
                        Console.WriteLine(string.Format("Total Rows {0}", dt.Rows.Count));

                    // UPDATE
                    cmd.CommandText = "UPDATE TestTable SET LastName = 'Doe' WHERE Username = 'jdoe'; ";
                    cmd.ExecuteNonQuery();

                    // DELETE 
                    cmd.CommandText = "DELETE FROM TestTable WHERE Username = 'jdoe'; ";
                    cmd.ExecuteNonQuery();


                    // TRUNCATE - Library does not support truncate but it can be achived by recreating the table
                    cmd.CommandText = "SELECT sqltext FROM SYS_OBJECTS Where type = 'table' AND tablename = 'TestTable' LIMIT 1;";
                    object TableSQLText = cmd.ExecuteScalar();
                    if (!string.IsNullOrWhiteSpace(TableSQLText.ToString()))
                    {
                        cmd.CommandText = "DROP TABLE IF EXISTS TestTable;";
                        cmd.ExecuteNonQuery();
                        // Now recreate the table....
                        cmd.CommandText = TableSQLText.ToString();
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        static void MixedLanguagesUTF8()
        {
            //Uncomment if your console window does not show all utf8 characters.
            //Console.OutputEncoding = Encoding.UTF8;

            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;"))
            {
                cnn.Open();
                SqlDatabaseTransaction trans = cnn.BeginTransaction();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "CREATE TABLE Languages (Id Integer Primary Key AutoIncrement, LanguageName Text, LangText Text);";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO Languages VALUES (null, @Language, @LangText);";
                    cmd.Parameters.Add(new SqlDatabaseParameter { ParameterName = "@Language" });
                    cmd.Parameters.Add(new SqlDatabaseParameter { ParameterName = "@LangText" });

                    cmd.Parameters["@Language"].Value = "English";
                    cmd.Parameters["@LangText"].Value = "Hello World";
                    cmd.ExecuteNonQuery();

                    //Languages written right to left must use parameters intead of string concatenation of sql text.
                    cmd.Parameters["@Language"].Value = "Urdu";
                    cmd.Parameters["@LangText"].Value = "ہیلو ورلڈ";
                    cmd.ExecuteNonQuery();

                    cmd.Parameters["@Language"].Value = "Arabic";
                    cmd.Parameters["@LangText"].Value = "مرحبا بالعالم";
                    cmd.ExecuteNonQuery();

                    cmd.Parameters["@Language"].Value = "Chinese Traditional";
                    cmd.Parameters["@LangText"].Value = "你好，世界";
                    cmd.ExecuteNonQuery();

                    cmd.Parameters["@Language"].Value = "Japanese";
                    cmd.Parameters["@LangText"].Value = "こんにちは世界";
                    cmd.ExecuteNonQuery();

                    cmd.Parameters["@Language"].Value = "Russian";
                    cmd.Parameters["@LangText"].Value = "Привет мир";
                    cmd.ExecuteNonQuery();

                    cmd.Parameters["@Language"].Value = "Hindi";
                    cmd.Parameters["@LangText"].Value = "नमस्ते दुनिया";
                    cmd.ExecuteNonQuery();


                    cmd.CommandText = "SELECT * FROM Languages; ";
                    SqlDatabaseDataReader dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    }

                    Console.WriteLine("---- Search -----");
                    //Urdu and Arabic should return when searching like ر which is R character in english.
                    cmd.CommandText = "SELECT * FROM Languages WHERE LangText LIKE @LikeSearch;"; //note no single quotes around @LikeSearch parameter LIKE '%w%'
                    cmd.Parameters.Add(new SqlDatabaseParameter { ParameterName = "@LikeSearch", Value = "%ر%" });
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    }

                    Console.WriteLine("---- Search With OR operator -----");

                    //Now it should return English, Urdu, Arabic and Russian due to OR operator
                    cmd.CommandText = "SELECT * FROM Languages WHERE (LangText LIKE '%W%') OR (LangText LIKE @LikeSearch) OR (LangText = @LangText);"; //note no single quotes around @LikeSearch parameter LIKE '%w%'

                    //Parameters can be cleared using : cmd.Parameters.Clear(); //however we are reusing existing parameter names.
                    cmd.Parameters["@LikeSearch"].Value = "%ر%"; //since parameter @LikeSearch already exist assign new value.
                    cmd.Parameters["@LangText"].Value = "Привет мир"; //parameter @LangText already exist in this Command object.
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    }
                    
                }
            }
        }


        static void MultiThreading()
        {
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("schemaname=db;uri=file://@memory;")) // In Memory database.
            {
                try
                {
                    cnn.Open();
                    if (cnn.State != ConnectionState.Open)
                    {
                        Console.WriteLine("Unable to open connection.");
                        return;
                    }
                }
                catch (SqlDatabaseException e)
                {
                    Console.WriteLine("Error: {0}", e.Message);
                    return;
                }

                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand())
                {
                    cmd.Connection = cnn;
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS TestTable (ThreadId Integer, Id Integer, RandomText Text, ByteArray Blob);";
                    cmd.ExecuteNonQuery();
                }

                Random rnd = new Random();
                Parallel.For(0, Environment.ProcessorCount,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = Environment.ProcessorCount
                    },
                    i => {

                        using (SqlDatabaseCommand cmd = new SqlDatabaseCommand())
                        {
                            cmd.Connection = cnn;

                            string RandomPathForText = System.IO.Path.GetRandomFileName();

                            cmd.CommandText = "INSERT INTO TestTable VALUES (@ThreadId, @Id, @RandomText, @ByteArray);";
                            if (!cmd.Parameters.Contains("@ThreadId"))
                                cmd.Parameters.AddWithValue("@ThreadId", System.Threading.Thread.CurrentThread.ManagedThreadId);
                            else
                                cmd.Parameters["@ThreadId"].Value = System.Threading.Thread.CurrentThread.ManagedThreadId;

                            if (!cmd.Parameters.Contains("@Id"))
                                cmd.Parameters.AddWithValue("@Id", rnd.Next(1, 100));
                            else
                                cmd.Parameters["@Id"].Value = rnd.Next(1, 100);

                            if (!cmd.Parameters.Contains("@RandomText"))
                                cmd.Parameters.AddWithValue("@RandomText", RandomPathForText);
                            else
                                cmd.Parameters["@RandomText"].Value = RandomPathForText;

                            if (!cmd.Parameters.Contains("@ByteArray"))
                                cmd.Parameters.AddWithValue("@ByteArray", Encoding.UTF8.GetBytes(RandomPathForText));
                            else
                                cmd.Parameters["@ByteArray"].Value = Encoding.UTF8.GetBytes(RandomPathForText);

                            cmd.ExecuteNonQuery();

                        }
                    });


                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "SELECT * FROM TestTable; ";
                    cmd.ExecuteNonQuery();
                    SqlDatabaseDataReader dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            //Console.Write(Encoding.UTF8.GetString(dr.GetFieldValue<byte[]>(c)) + "\t");
                            //byte[] byteArray = (byte[])dr.GetValue(c);
                            if (dr.GetName(c).Equals("ByteArray"))
                                Console.Write(Encoding.UTF8.GetString(dr.GetFieldValue<byte[]>(c)) + "\t");
                            else
                                Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    }
                }

            }
        }

        static void EncryptionDecryption()
        {
            Console.WriteLine("*************** Encrypted File Example *******************");
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("SchemaName=db;uri=file://Encrypted.db;"))
            {
                cnn.Open();

                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    //Entire Database File will be encrypted using AES 256
                    cmd.CommandText = "SYSCMD Key='SecretPassword';";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "Create table if not exists Users(id integer primary key autoincrement, Username Text, Password Text); ";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO Users values(NULL, @username, @password);";
                    cmd.Parameters.AddWithValue("@username", "sysdba");
                    cmd.Parameters.AddWithValue("@password", "SecretPassword");
                    cmd.ExecuteNonQuery();
                }
            }


            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("SchemaName=db;uri=file://Encrypted.db;"))
            {
                cnn.Open();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    //Entire Database File will be encrypted using AES 256
                    cmd.CommandText = "SYSCMD Key = 'SecretPassword'; ";  //If incorrect password library will not respond.
                    cmd.ExecuteNonQuery();

                    // COLLATE BINARY performs case sensitive search for password
                    // see http://www.sqldatabase.net/docs/syscmd.aspx for available collation sequences.

                    cmd.CommandText = "SELECT Id FROM Users WHERE Username = @username AND Password = @password COLLATE BINARY;";
                    cmd.Parameters.AddWithValue("@username", "sysdba");
                    cmd.Parameters.AddWithValue("@password", "SecretPassword");
                    Console.WriteLine("User Found {0}", cmd.ExecuteScalar() == null ? "No" : "Yes");
                }
            }

            Console.Write(string.Empty);
            Console.WriteLine("*************** Encrypted Column Example *******************");

            string RandomUserName = "u-" + System.IO.Path.GetRandomFileName();
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("SchemaName=db;uri=file://EncryptedColumn.db;"))
            {
                cnn.Open();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS UsersCreditCards(Name Text Primary Key, CreditCardNumber Text); ";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO UsersCreditCards values(@Name, EncryptText(@CreditCardNumber , 'SecretPassword'));";
                    cmd.Parameters.AddWithValue("@Name", RandomUserName);
                    cmd.Parameters.AddWithValue("@CreditCardNumber", "1234-5678");
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection("SchemaName=db;uri=file://EncryptedColumn.db;"))
            {
                cnn.Open();
                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(cnn))
                {
                    cmd.CommandText = "SELECT DecryptText(CreditCardNumber , 'SecretPassword') AS [CreditCardNumber] FROM UsersCreditCards WHERE Name = @Name LIMIT 1;";
                    cmd.Parameters.AddWithValue("@Name", RandomUserName);
                    Console.WriteLine("User {0} Credit Card Number is : {1}", RandomUserName, cmd.ExecuteScalar());

                    Console.WriteLine("*************** All Users *******************");
                    cmd.CommandText = "SELECT Name, DecryptText(CreditCardNumber , 'SecretPassword') AS CreditCardNumber FROM UsersCreditCards;";
                    SqlDatabaseDataReader dr = cmd.ExecuteReader();
                    for (int c = 0; c < dr.VisibleFieldCount; c++)
                    {
                        Console.Write(dr.GetName(c) + "\t");
                    }
                    Console.WriteLine(Environment.NewLine + "----------------------");
                    while (dr.Read())
                    {
                        for (int c = 0; c < dr.VisibleFieldCount; c++)
                        {
                            Console.Write(dr.GetValue(c) + "\t");
                        }
                        Console.WriteLine("");
                    };
                }
            }

            if (File.Exists("Encrypted.db"))
                File.Delete("Encrypted.db");

            if (File.Exists("EncryptedColumn.db"))
                File.Delete("EncryptedColumn.db");
        }



        static void MarsEnabled()
        {
            // MARS (MultipleActiveResultSets) can decrease read time when there are multiple queries
            // It results in better performance since queries can be combined.
            // Very useful for large forms and web pages which require data from multiple tables.
            // Instead of running each query and processing results get all results at once.

            if (string.IsNullOrWhiteSpace(ExampleDatabaseFile))
                return;



            //build connection string
            cb.Clear(); //clear any previous settings
            cb.Uri = "file://" +ExampleDatabaseFile; //Set the database file
            cb.MultipleActiveResultSets = true; //We need multiple result sets
            cb.ExtendedResultSets = false; //extended result set is false but can be set during command execution e.g. command.ExecuteReader(true)
            cb.SchemaName = "db"; //schema name

            //"SchemaName=db;uri=file://" + ExampleDatabaseFile + ";MultipleActiveResultSets=true;"
            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection(cb.ConnectionString))
            {
                cnn.Open();

                try
                {
                    using (SqlDatabaseCommand command = new SqlDatabaseCommand())
                    {
                        command.Connection = cnn;

                        //execute two queries against two different tables.
                        //command.CommandText = "SELECT ProductId, ProductName FROM Products; SELECT CustomerId, LastName || ' , ' || FirstName FROM Customers LIMIT 10;";

                        // For easy to read above command can also be written as following:
                        command.CommandText = "SELECT ProductId, ProductName FROM Products LIMIT 10 OFFSET 10 ; "; // Query index 0
                        command.CommandText += "SELECT CustomerId, LastName || ',' || FirstName FROM Customers LIMIT 10 ; "; //Query index 1



                        SQLDatabaseResultSet[] cmdrs = command.ExecuteReader(false);// parameter bool type is for ExtendedResultSet
                        if ((cmdrs != null) && (cmdrs.Length > 0))
                        {
                            if (!string.IsNullOrWhiteSpace(cmdrs[0].ErrorMessage))
                            {
                                Console.WriteLine(cmdrs[0].ErrorMessage);
                                return;
                            }
                            //this loop is just an example how to loop through all rows and columns.
                            for (int r = 0; r < cmdrs[0].RowCount; r++) //loop through each row of result set query index zero ( 0 )
                            {
                                for (int c = 0; c < cmdrs[0].ColumnCount; c++)
                                {
                                    //Console.WriteLine(cmdrs[0].Rows[r][c]);
                                }
                            }

                            //Loading data from products table which is at index 0 : cmdrs[0]
                            for (int r = 0; r < cmdrs[0].RowCount; r++) //loop through each row of result set index zero ( 0 ) which is products table
                            {
                                //cmdrs[0].Rows[r][0] : in Rows[r][0] r = row and [0] is column index.
                                Console.WriteLine(string.Format("{0} - {1}", cmdrs[0].Rows[r][0], cmdrs[0].Rows[r][1]));
                            }


                            //Loading data from customers table which is at index 1 cmdrs[1]
                            for (int r = 0; r < cmdrs[1].RowCount; r++) //loop through each row of result set index one ( 1 ) which is customers table
                            {
                                //cmdrs[0].Rows[r][0] : Rows[r][0] r = row and [0] is column index.
                                Console.WriteLine(string.Format("{0} - {1}", cmdrs[1].Rows[r][0], cmdrs[1].Rows[r][1]));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }


            }
        }

        static void ExtendedResultSet()
        {
            // Old fashioned connection string example
            string ConnectionString = "SchemaName = db;";
            ConnectionString += "MultipleActiveResultSets = true;";
            ConnectionString += "ExtendedResultSets = true;";
            ConnectionString += "Mode = readwrite;";
            ConnectionString += "FileMode = OpenIfExists;";
            ConnectionString += "uri = file://" + ExampleDatabaseFile + ";";
            //ConnectionString += "DatabaseJournalDirectory = " + Path.GetDirectoryName(ExampleDatabaseFile) + ";";


            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection(ConnectionString))
            {
                cnn.Open();

                try
                {
                    using (SqlDatabaseCommand cmd = new SqlDatabaseCommand())
                    {
                        cmd.Connection = cnn;

                        ////execute two queries against two tables.
                        ////query can have parameters they are not declared here since code is commented.
                        //command.CommandText = "UPDATE Suppliers SET CompanyName = CompanyName Where SupplierId = 1;SELECT ProductId, ProductName FROM Products; SELECT CustomerId, LastName || ' , ' || FirstName FROM Customers LIMIT 10;";

                        // For easy to read above command can also be written as following:
                        cmd.CommandText = "UPDATE db.Suppliers SET CompanyName = CompanyName Where SupplierId = 1 ; "; //First Query will be at index 0
                        cmd.CommandText += "SELECT ProductId [Product Id], ProductName FROM db.Products LIMIT 10 OFFSET @Limit ; ";  //Second Query will be at index 1
                        cmd.CommandText += "SELECT CustomerId, LastName || ',' || FirstName FROM Customers LIMIT @Limit ; "; //Third Query will be at index 2

                        cmd.Parameters.AddWithValue("@Limit", 10);

                        //When SQLDatabaseResultSet is needed a boolean type must be passed to command execution object.
                        SQLDatabaseResultSet[] cmdrs = cmd.ExecuteReader(true);// true for ExtendedResultSet
                        if ((cmdrs != null) && (cmdrs.Length > 0))
                        {
                            foreach (SQLDatabaseResultSet rs in cmdrs)
                            {
                                Console.WriteLine("---------------------------------\n" + rs.SQLText);
                                Console.WriteLine("Execution time in Milliseconds: {0} ", rs.ProcessingTime);
                                Console.WriteLine("Rows Affected: {0}", rs.RowsAffected); //RowsAffected is non zero for update or delete only.

                                if (string.IsNullOrWhiteSpace(rs.ErrorMessage))
                                    Console.WriteLine("No error");
                                else
                                    Console.WriteLine(rs.ErrorMessage);

                                //All the schemas in the query
                                foreach (object schema in rs.Schemas)
                                    Console.WriteLine("Schema Name: {0} ", schema);

                                //All the tables in the query
                                foreach (object table in rs.Tables)
                                    Console.WriteLine("Table Name: {0} ", table);

                                //parameters if any 
                                foreach (object Parameter in rs.Parameters)
                                    Console.WriteLine("Parameter Name: {0} ", Parameter);


                                //data type for returned column, datatype is what is defined during create table statement.
                                foreach (string datatype in rs.DataTypes)
                                {
                                    Console.Write(datatype + "\t");
                                }
                                Console.WriteLine(""); //add empty line to make it easy to read

                                //Column names or aliases
                                foreach (string ColumnName in rs.Columns)
                                {
                                    Console.Write(ColumnName + "\t");
                                }
                                Console.WriteLine("");//add empty line to make it easy to read

                                //all columns and rows.
                                foreach (object[] row in rs.Rows)
                                {
                                    foreach (object column in row)
                                    {
                                        Console.Write(column + "\t"); // \t will add tab
                                    }
                                    Console.WriteLine(""); //break line for each new row.
                                }
                            }

                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        static void TrackDataChanges()
        {
            //Change tracking allow users to track data changes in user defined tables.
            // Tracking is done for all schema's and all user tables for INSERT, UPDATE and DELETE queries

            using (SqlDatabaseConnection cnn = new SqlDatabaseConnection())
            {
                cnn.ConnectionString = "SchemaName=db;uri=file://" + ExampleDatabaseFile + ";";

                //Enable Tracking
                cnn.TrackDataChanges = true;
                //Set buffer size, default is to track last 1000 changes.
                // for this example we will change it to 10
                cnn.TrackedChangesMaxCount = 10;

                cnn.Open();

                using (SqlDatabaseCommand cmd = new SqlDatabaseCommand())
                {
                    cmd.Connection = cnn;
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS UsersTestTable (Username TEXT PRIMARY KEY, FirstName TEXT, LastName TEXT);";
                    cmd.ExecuteNonQuery();

                    // INSERT
                    cmd.CommandText = "INSERT INTO UsersTestTable VALUES ('johndoe', 'John' , 'DOE');";
                    cmd.CommandText += "INSERT INTO UsersTestTable VALUES ('janedoe', 'Jane' , 'DOE');";
                    cmd.ExecuteNonQuery();

                    // UPDATE
                    cmd.CommandText = "UPDATE UsersTestTable SET LastName = 'Doe' WHERE Username = 'johndoe'; ";
                    cmd.ExecuteNonQuery();

                    // DELETE - The actual row is not recoverable after the delete
                    // Only RowId is stored without deleted data.
                    cmd.CommandText = "DELETE FROM UsersTestTable WHERE Username = 'johndoe'; ";
                    cmd.ExecuteNonQuery();

                    // INSERT again to show new RowId has been generated
                    cmd.CommandText = "INSERT INTO UsersTestTable VALUES ('johndoe', 'John' , 'DOE');";
                    cmd.ExecuteNonQuery();

                    // To view changes we will call GetTrackedDataChanges() function which will yeild
                    foreach (SqlDatabaseDataChanges tdc in cnn.GetTrackedDataChanges())
                    {
                        string DMLType = string.Empty;
                        switch (tdc.ChangeType)
                        {
                            case 1:
                                DMLType = "INSERT";
                                break;
                            case 2:
                                DMLType = "UPDATE";
                                break;
                            case 3:
                                DMLType = "DELETE";
                                break;
                            default:
                                DMLType = "UNKNOWN";
                                break;
                        }
                        Console.WriteLine("ChangeType {0} \t SchemaName: {1} \t TableName: {2} \t RowId: {3} \t DateTime: {4}"
                            , DMLType
                            , tdc.SchemaName
                            , tdc.TableName
                            , tdc.RowId
                            , new DateTime(tdc.NowTicks));
                    }

                    // To view changed row simply query using SELECT with where clause
                    // By default each row is given unique RowId
                    // Using Linq with changeType = 2 for updated rows only.
                    Int64 RowId = cnn.GetTrackedDataChanges().Where(item => item.ChangeType == 2).FirstOrDefault().RowId;
                    cnn.MultipleActiveResultSets = true;
                    cmd.CommandText = "SELECT * FROM UsersTestTable WHERE RowId = " + RowId;
                    SQLDatabaseResultSet[] rs = cmd.ExecuteReader(false);

                    if (rs != null)
                    {
                        if (!string.IsNullOrWhiteSpace(rs[0].ErrorMessage))
                        {
                            Console.WriteLine(rs[0].ErrorMessage);
                            return;
                        }

                        foreach (object[] row in rs[0].Rows)
                            foreach (object column in row)
                                Console.WriteLine(column);
                    }
                }
            }
        }
    }
}
