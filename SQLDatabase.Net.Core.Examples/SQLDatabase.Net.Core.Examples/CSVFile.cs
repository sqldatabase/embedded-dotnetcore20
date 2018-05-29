using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using SQLDatabase.Net.SQLDatabaseClient;

namespace CSVFile
{

    /// <summary>
    /// Determines how an entire blank or empty line should be handled.
    /// </summary>
    public enum BlankLine
    {       
        /// <summary>
        /// Return a line with a single empty column.
        /// </summary>
        EmptySingleColumn,
        /// <summary>
        /// Blank and empty lines are skipped.
        /// </summary>
        SkipEntireLine,
        /// <summary>
        /// Consider end of file
        /// </summary>
        EndOfFile
    }

    public class CsvImportExport
    {
        public SqlDatabaseConnection SQLDatabaseConnection { get; set; }
        public string TableName { get; set; }
        public string SchemaName { get; set; }
        public SqlDatabaseTransaction SQLDatabaseTransaction { get; set; } = null;

        public CsvFileReader CsvReader { get; set; }
        public CsvFileWriter CsvWriter { get; set; }

        public CsvImportExport(SqlDatabaseConnection sqlDatabaseConnection, string TableName, string SchemaName)
        {
            if (string.IsNullOrWhiteSpace(TableName))
                throw new Exception("TableName parameter is required.");

            if (sqlDatabaseConnection.State == System.Data.ConnectionState.Closed)
                sqlDatabaseConnection.Open();
                        
            if (string.IsNullOrWhiteSpace(SchemaName))
                SchemaName = "sdbn";

            SQLDatabaseConnection = sqlDatabaseConnection;
            this.TableName = TableName;
            this.SchemaName = SchemaName;
        }

        public CsvImportExport(string DatabaseFile, string TableName, string SchemaName)
        {
            if (string.IsNullOrWhiteSpace(TableName))
                throw new Exception("TableName parameter is required.");

            if (string.IsNullOrWhiteSpace(SchemaName))
                SchemaName = "sdbn";

            string constr = string.Format("SchemaName={0};uri=file://{1}", SchemaName, DatabaseFile);
            SQLDatabaseConnection = new SqlDatabaseConnection(constr);
            SQLDatabaseConnection.DatabaseFileMode = DatabaseFileMode.OpenIfExists;
                        
            this.TableName = TableName;
            this.SchemaName = SchemaName;
        }

        public int ExportTable(string FilePathAndName, bool AppendToFile = false)
        {
            int _row_count = 0;
            
            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(SQLDatabaseConnection))
            {
                if (SQLDatabaseTransaction != null)
                    cmd.Transaction = SQLDatabaseTransaction;

                cmd.CommandText = string.Format("SELECT * FROM [{0}].[{1}]", SchemaName, TableName);
                using(CsvWriter = new CsvFileWriter(FilePathAndName, AppendToFile, Encoding.UTF8))
                {
                    SqlDatabaseDataReader dataReader = cmd.ExecuteReader();
                    List<string> ColumnNames = new List<string>();
                    // Write header i.e. column names
                    for (int i = 0; i < dataReader.VisibleFieldCount; i++)
                    {
                        if (dataReader.GetFieldType(i) != Type.GetType("byte[]")) // BLOB will not be written
                        {
                            ColumnNames.Add(dataReader.GetName(i)); //maintain columns in the same order as the header line.
                            CsvWriter.AddField(dataReader.GetName(i));
                        }
                            
                    }
                    CsvWriter.SaveAndCommitLine();                    
                    // Write data i.e. rows.                    
                    while (dataReader.Read())
                    {
                        foreach (string ColumnName in ColumnNames)
                        {
                            CsvWriter.AddField(dataReader.GetString(dataReader.GetOrdinal(ColumnName))); //dataReader.GetOrdinal(ColumnName) provides the position.
                        }
                        CsvWriter.SaveAndCommitLine();
                        _row_count++; //Increase row count to track number of rows written.
                    }
                    
                }
            }

            return _row_count;
        }

        public int ImportTable(string FilePathAndName, bool IsFirstLineHeader)
        {

            int _row_count = 0;
            List<string> _HeaderColumns = new List<string>();

            using (CsvReader = new CsvFileReader(FilePathAndName, Encoding.UTF8))
            {
                CsvReader.OnEmptyLine = BlankLine.SkipEntireLine;
                CsvReader.MaximumLines = 1; //Just read one line to get the header info and/or number of columns.
                while (CsvReader.ReadLine())
                {
                    int ColumnCount = 0;
                    foreach(string Field in CsvReader.Fields)
                    {
                        ColumnCount++;
                        if (IsFirstLineHeader)
                            _HeaderColumns.Add(Field);
                        else
                            _HeaderColumns.Add("CsvColumn" + ColumnCount);
                    }
                    break;
                }
            }

            if (_HeaderColumns.Count == 0)
                throw new Exception("Columns are required, check the function parameters.");
            
            if (SQLDatabaseConnection.State != ConnectionState.Open)
                throw new Exception("A valid and open connection is required.");

            using (SqlDatabaseCommand cmd = new SqlDatabaseCommand(SQLDatabaseConnection))
            {
                if (SQLDatabaseTransaction != null)
                    cmd.Transaction = SQLDatabaseTransaction;

                //cmd.CommandText = string.Format("DROP TABLE IF EXISTS [{0}].[{1}]", SchemaName, TableName);
                //cmd.ExecuteNonQuery();

                System.Data.DataTable dt =SQLDatabaseConnection.GetSchema("Columns", new string[] { string.Format("[{0}].[{1}]",SchemaName,TableName) });

                if (dt.Rows.Count != 6) //Table does not exists other wise if 6 rows then table have definition
                {
                    cmd.CommandText = string.Format("CREATE TABLE IF NOT EXISTS [{0}].[{1}] (", SchemaName, TableName);
                    foreach(string ColumnName in _HeaderColumns)
                    {
                        cmd.CommandText += ColumnName + " None,"; //The DataType none is used since we do not know if all rows have same datatype                        
                    }
                    cmd.CommandText = cmd.CommandText.Substring(0, cmd.CommandText.Length - 1); //Remove the last comma
                    cmd.CommandText += ");";
                    cmd.ExecuteNonQuery(); // Create table

                    dt = SQLDatabaseConnection.GetSchema("Columns", new string[] { string.Format("[{0}].[{1}]", SchemaName, TableName) });
                    if (dt.Rows.Count != 6)
                        throw new Exception("Unable to create or find table.");
                }


                // Sanity check if number of columns in CSV and table are equal
                if (dt.Rows.Count != _HeaderColumns.Count)
                    throw new Exception("Number of columns in CSV should be same as number of columns in the table");


                // Start of code block to generate INSERT statement.
                cmd.CommandText = string.Format("INSERT INTO {0}.[{1}] VALUES (", SchemaName, TableName);
                int ParamCount = 0;
                foreach (string ColumnName in _HeaderColumns)
                {
                    ParamCount++;
                    cmd.CommandText += string.Format("@param{0},", ParamCount); //The DataType none is used since we do not know if all rows have same datatype                        
                }
                cmd.CommandText = cmd.CommandText.Substring(0, cmd.CommandText.Length - 1); //Remove the last comma
                cmd.CommandText += ");";

                // Add parameters
                ParamCount = 0;
                foreach (string ColumnName in _HeaderColumns)
                {
                    ParamCount++;
                    cmd.Parameters.Add(string.Format("@param{0}", ParamCount)); //The DataType none is used since we do not know if all rows have same datatype                        
                }

                // End of code block to generate INSERT statement.


                //Read CSV once insert statement has been created.
                using (CsvReader = new CsvFileReader(FilePathAndName, Encoding.UTF8))
                {
                    CsvReader.OnEmptyLine = BlankLine.SkipEntireLine;

                    //Skip the header line.
                    if (IsFirstLineHeader)
                        CsvReader.SkipLines = 1;

                    while (CsvReader.ReadLine())
                    {
                        int CsvColumnCount = 0;
                        foreach (string FieldValue in CsvReader.Fields)
                        {
                            CsvColumnCount++;
                            cmd.Parameters["@param" + CsvColumnCount].Value = FieldValue; //Assign File Column to parameter
                        }
                        cmd.ExecuteNonQuery();
                        _row_count++; // Count inserted rows.
                    }
                }
            }

            return _row_count;
        }
    }

    /// <summary>
    /// Class for reading from comma separated values (CSV) file
    /// </summary>
    public class CsvFileReader : IDisposable
    {
        // Private members        
        StreamReader _Reader;
        string _CurrentLineText;
        int _CurrentPosition;
        int _LineCount = 0;
        BlankLine _OnEmptyLine = BlankLine.SkipEntireLine;
        List<string> _Columns = new List<string>();
        List<string> _Comments = new List<string>();
        bool _IsHeaderSkipped = false;
        HashSet<int> _ColumnIndexes = new HashSet<int>();
        long _TotalLength = 0;
        List<long> _LineOffSets = new List<long>();
        bool _IsLineEmtpy = false;

        public string[] Fields
        {
            get { return _Columns.ToArray(); }
        }
        public string[] CommentLines
        {
            get { return _Comments.ToArray(); }
        }
        public char Delimiter { get; set; } = Convert.ToChar(",");
        public char QuoteChar { get; set; } = Convert.ToChar('"');
        public string CommentLineStartsWith { get; set; } = "";        
        public HashSet<int> ColumnIndexes {
            get { return _ColumnIndexes; }
        }
        public int SkipLines { get; set; } = 0;
        public int MaximumLines { get; set; } = -1;
        public bool StoreLineOffSets { get; set; } = false;
        public long StartAtOffSet { get; set; } = 0;
        public List<long> LineOffSets
        {
            get { return _LineOffSets; }
        }
        public int LineCount
        {
            get { return _LineCount; }
        }
        public long CurrentOffSet
        {
            get { return _TotalLength; }
        }
        public BlankLine OnEmptyLine {
            get { return _OnEmptyLine; }
            set { _OnEmptyLine = value; }
        }
        public bool IsLineEmpty {
            get { return _IsLineEmtpy; }
        }
        
        private void InitCsvReader()
        {
            _LineOffSets.Clear();
            StartAtOffSet = 0;
            _LineCount = 0;
        }

        /// <summary>
        /// Initializes a new instance of the CsvFileReader class for the
        /// specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are handled</param>
        public CsvFileReader(Stream stream)
        {
            InitCsvReader();
            _Reader = new StreamReader(stream, true);
        }

        /// <summary>
        /// Initializes a new instance of the CsvFileReader class for the
        /// specified file path.
        /// </summary>
        /// <param name="path">The name of the CSV file to read from</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are handled</param>
        public CsvFileReader(string path)
        {
            InitCsvReader();
            _Reader = new StreamReader(path,true);
        }

        /// <summary>
        /// Initializes a new instance of the CsvFileReader class for the
        /// specified file path.
        /// </summary>
        /// <param name="path">The name of the CSV file to read from</param>
        public CsvFileReader(string path, Encoding encoding)
        {
            InitCsvReader();
            _Reader = new StreamReader(path, encoding);
        }

        public void RestrictToColumns(params int[] ColumnIndexes)
        {
            foreach(int i in ColumnIndexes)
            {
                _ColumnIndexes.Add(i);
            }
        }

        /// <summary>
        /// Reads a row of columns from the current CSV file. Returns false if no
        /// more data could be read because the end of the file was reached.
        /// </summary>        
        public bool ReadLine()
        {
            if (MaximumLines > 0)
            {
                if (_LineCount >= MaximumLines)
                    return false;
            }

            if ((!_IsHeaderSkipped) && (SkipLines > 0))
            {
                int rows_skipped = 0;
                while (!_IsHeaderSkipped)
                {
                    //_Reader.ReadLine();
                    ReadNextLine();
                    rows_skipped++;
                    if (rows_skipped >= SkipLines)
                    {
                        _IsHeaderSkipped = true;
                        break;
                    }
                }
            }

            // Read next line from the file
            if ((_CurrentLineText = ReadNextLine()) == null)
                return false;
            

            _IsLineEmtpy = false;

            _CurrentPosition = 0;
           
            // Test for empty line
            if (_CurrentLineText.Length == 0)
            {
                _IsLineEmtpy = true;
                switch (_OnEmptyLine)
                {
                    case BlankLine.EmptySingleColumn:
                        _Columns.Clear();
                        return true;
                    case BlankLine.SkipEntireLine:
                        return ReadLine();
                    case BlankLine.EndOfFile:
                        return false;
                }
            }
            else
            {
                _Columns.Clear();
            }

            // Parse line            
            int _ColumnCount = 0;
            while (true)
            {
                string column;

                // Read next column
                if (_CurrentPosition < _CurrentLineText.Length && _CurrentLineText[_CurrentPosition] == QuoteChar)
                    column = ReadQuotedColumn();
                else
                    column = ReadUnquotedColumn();

                if (_ColumnIndexes.Count > 0)
                {
                    if (_ColumnIndexes.Contains(_ColumnCount))
                    {
                        _Columns.Add(column);
                        if (_Columns.Count == _ColumnIndexes.Count)
                            break;
                    }
                        
                } else
                {
                    _Columns.Add(column);
                }
                               
                _ColumnCount++;

                // Break if we reached the end of the line
                if (_CurrentLineText == null || _CurrentPosition == _CurrentLineText.Length)
                    break;
                // Otherwise skip delimiter
                if(_CurrentLineText[_CurrentPosition].Equals(Delimiter))
                    _CurrentPosition++;
            }
           
            // Indicate success
            _LineCount++;            
            return true;
        }

        
        private string ReadNextLine(bool IsContinued = false)
        {
            string _LineText = string.Empty;
            if (!_Reader.EndOfStream)
            {
                if ((!IsContinued) && (StoreLineOffSets) && (!_LineOffSets.Contains(_TotalLength)))
                {
                    _LineOffSets.Add(_TotalLength);
                }
                
                if (StartAtOffSet > 0)
                {
                    _Reader.DiscardBufferedData();
                    _Reader.BaseStream.Seek(StartAtOffSet, SeekOrigin.Begin);
                    StartAtOffSet = -1;
                }

                if (CommentLineStartsWith.Trim().Length > 0) {
                    _LineText = _Reader.ReadLine();
                    while (_LineText != null && _LineText.Trim().StartsWith(CommentLineStartsWith))
                    {
                        _TotalLength += _LineText.Length + Environment.NewLine.Length;
                        _Comments.Add(_LineText);
                        _LineText = _Reader.ReadLine();
                    }
                } else {
                    _LineText = _Reader.ReadLine();
                }
                
                if (_LineText != null)
                    _TotalLength += _LineText.Length + Environment.NewLine.Length;

            } else
            {
                return null;
            }
            return _LineText;
        }
        /// <summary>
        /// Reads a quoted column by reading from the current line until a
        /// closing quote is found or the end of the file is reached. 
        /// </summary>
        private string ReadQuotedColumn()
        {
            // Skip opening quote character
            if(_CurrentPosition < _CurrentLineText.Length && _CurrentLineText[_CurrentPosition] == QuoteChar)
                _CurrentPosition++;

            // Parse column
            StringBuilder builder = new StringBuilder();
            while (true)
            {
                while (_CurrentPosition == _CurrentLineText.Length)
                {
                    // End of line so attempt to read the next line
                    _CurrentLineText = ReadNextLine(true); //_Reader.ReadLine();
                    
                    _CurrentPosition = 0;
                    // Done if we reached the end of the file
                    if (_CurrentLineText == null)
                        return builder.ToString();
                    // Otherwise, treat as a multi-line field
                    builder.Append(Environment.NewLine);
                }

                // check for quote character
                if (_CurrentLineText[_CurrentPosition] == QuoteChar)
                {
                    // If two quotes, skip first and treat second as literal
                    int nextPos = (_CurrentPosition + 1);
                    if (nextPos < _CurrentLineText.Length && _CurrentLineText[nextPos] == QuoteChar)
                        _CurrentPosition++;
                    else
                        break;  // Single quote ends quoted sequence
                }
                // Add current character to the column
                builder.Append(_CurrentLineText[_CurrentPosition++]);
            }

            if (_CurrentPosition < _CurrentLineText.Length)
            {
                // Consume closing quote
                if(_CurrentLineText[_CurrentPosition] == QuoteChar)
                    _CurrentPosition++;

                // Append any additional characters appearing before next delimiter
                builder.Append(ReadUnquotedColumn());
            }
            // Return column value
            return builder.ToString();
        }

        /// <summary>
        /// Reads an unquoted column by reading from the current line until a
        /// delimiter is found or the end of the line is reached. 
        /// </summary>
        private string ReadUnquotedColumn()
        {
            int startPos = _CurrentPosition;
            if ( (_CurrentPosition = _CurrentLineText.IndexOf(Delimiter, _CurrentPosition)) == -1)
                _CurrentPosition = _CurrentLineText.Length;

            if (_CurrentPosition > startPos)
                return _CurrentLineText.Substring(startPos, _CurrentPosition - startPos);
            return String.Empty;
        }

        
        public void Dispose()
        {
            _Reader.Dispose();
        }
    }

    //
    /// <summary>
    /// Class for writing CSV file
    /// </summary>
    public class CsvFileWriter : IDisposable
    {
        StreamWriter _Writer;
        List<string> _Fields = new List<string>();

        public char Delimiter { get; set; } = ',';
        public char QuoteChar { get; set; } = '"';
        public string CommentLineStartsWith { get; set; } = "#";
        public int NumberOfFields { get; set; } = 0;
        public CsvFileWriter(string path)
        {
            _Writer = new StreamWriter(path, true, Encoding.UTF8);
            if (!_Writer.BaseStream.CanWrite)
                throw new Exception("Stream does not support writing.");
        }
        public CsvFileWriter(string path, Encoding encoding)
        {
            _Writer = new StreamWriter(path, true, encoding);
            if (!_Writer.BaseStream.CanWrite)
                throw new Exception("Stream does not support writing.");
        }

        public CsvFileWriter(string path, bool append, Encoding encoding)
        {
            _Writer = new StreamWriter(path, append, encoding);
            if (!_Writer.BaseStream.CanWrite)
                throw new Exception("Stream does not support writing.");
        }

        public void AddField(string ColumnValue)
        {
            string value = string.Empty;
            if (ColumnValue == null)
                value = "null";
            else
                value = QuoteValue(ColumnValue);

            _Fields.Add(value);
        }

        public void AddField(params string[] ColumnValues)
        {
            foreach (string ColumnValue in ColumnValues)
                AddField(ColumnValue);
        }
        public void AddComments(string Comments)
        {
            if (string.IsNullOrWhiteSpace(CommentLineStartsWith))
                throw new Exception("Property CommentLineStartsWith must be set before comments can be added");

            string[] NewLineChars = { "\n", "\r", "\r\n", "\n\r" };
            foreach(string Line in Comments.Split(NewLineChars, StringSplitOptions.None))
            {
                _Writer.WriteLine(CommentLineStartsWith + Line);
            }
        }

        public void SaveAndCommitLine()
        {
            _Writer.AutoFlush = true;
            
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < _Fields.Count; i++)
            {
                sb.Append(_Fields[i]);

                if ( (NumberOfFields > 0) && (i >= NumberOfFields) )
                    break;

                if (i + 1 < _Fields.Count)
                    sb.Append(Delimiter);
                
            } 
            
            if ((NumberOfFields > 0) && (NumberOfFields > _Fields.Count) )
            {
                for (int i = _Fields.Count; i < NumberOfFields; i++)
                {                    
                    if (i + 1 < NumberOfFields)
                        sb.Append(Delimiter);
                }
            }
            _Writer.WriteLine(sb.ToString());
            _Writer.Flush();
            _Writer.BaseStream.Flush();
            System.Threading.Thread.Sleep(1);
            _Fields.Clear();
        }

        private string QuoteValue(string Value)
        {
            if (Value.IndexOf(QuoteChar) > -1)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(QuoteChar);
                sb.Append(QuoteChar);
                Value = Value.ToString().Replace(QuoteChar.ToString(), sb.ToString());                
            }                

            return QuoteChar + Value + QuoteChar;
        }

        public void Dispose()
        {            
            _Writer.Dispose();
        }
    }
}
