# Examples for .Net Standard in c# dotnet core 2.0 as console application

Embedded database Example for dot net core 2.0 using .Net standard 2.0 library, the examples inlcude importing and exporting CSV, key value store and orm client similar to eff. The example project uses library from http://wwww.sqldatabase.net and require Visual studio 2017 however examples work with .Net 4.6 and above as long as .Net standard is supported the example code can be copied from these examples to use in your own project. Some may require little modification and some are utility classes and only require changes to namespace etc.

## In your code
To use that in your code and projects, simply copy the class code, remove the namespaces and add the files you need.

### ORM 
To use ORM Client, simply add [ORMClient.cs] (https://github.com/sqldatabase/embedded-dotnetcore20/blob/master/SQLDatabase.Net.Core.Examples/SQLDatabase.Net.Core.Examples/ORMClient.cs) and see example code under [ORMClientExamples.cs] (https://github.com/sqldatabase/embedded-dotnetcore20/blob/master/SQLDatabase.Net.Core.Examples/SQLDatabase.Net.Core.Examples/ORMClientExamples.cs) 

### KeyValueStore 
To use KeyValueStore like behavior for storing Keys and Values under some collection add the [KeyValueStore.cs] (https://github.com/sqldatabase/embedded-dotnetcore20/blob/master/SQLDatabase.Net.Core.Examples/SQLDatabase.Net.Core.Examples/KeyValueStore.cs) in your project and see example code with function name KeyValueStoreLikeBehavior(), how to store and retrive values. Also remove the example settings class after adding in your own project.

### Export to CSV file
CSV file export and import can be performed on any table and view by adding [CSVFile.cs] (https://github.com/sqldatabase/embedded-dotnetcore20/blob/master/SQLDatabase.Net.Core.Examples/SQLDatabase.Net.Core.Examples/CSVFile.cs).
Simply add the file in your project and see example CSV import and export code in Program.cs under function name ImportExportCSV.

### Adding Images
Adding images or files should be only in BLOB column see [Data Types] (http://www.sqldatabase.net/docs/datatypes.aspx) and are stored as byte array. You can compress it when storing and decompress after reading.. No compression is performed by the library it self and byte[] is stored as provided. See example code under AddImage() function with stores files and images. You can store Base64 in Text column, however BLOB is preferred storage for bytes.
