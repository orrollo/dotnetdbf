/*
 DBFWriter
 Class for defining a DBF structure and addin data to that structure and
 finally writing it to an OutputStream.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 license: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetDBF
{
    public class DBFWriter : DBFBase, IDisposable
    {
	    protected const string MsgSpecifiedFileIsNotFound = "Specified file is not found. ";
	    protected const string MsgWhileReadingHeader = " while reading header";
	    protected const string MsgLanguagedriverHasAlreadyBeenSet = "LanguageDriver has already been set";
	    protected const string MsgFieldsHasAlreadyBeenSet = "Fields has already been set";
	    protected const string MsgShouldHaveAtLeastOneField = "Should have at least one field";
	    protected const string MsgFieldIsNull = "Field {0} is null";
	    protected const string MsgErrorAccesingFile = "Error accesing file";
	    protected const string MsgNotInitializedWithFileForWriterecordUseUseAddrecordInstead = "Not initialized with file for WriteRecord use, use AddRecord instead";
	    protected const string MsgAppendingToAFileRequiresUsingWriterecordInstead = "Appending to a file, requires using Writerecord instead";
	    protected const string MsgFieldsShouldBeSetBeforeAddingRecords = "Fields should be set before adding records";
	    protected const string MsgNullCannotBeAddedAsRow = "Null cannot be added as row";
	    protected const string MsgInvalidRecordInvalidNumberOfFieldsInRow = "Invalid record. Invalid number of fields in row";
	    protected const string MsgInvalidValueForField = "Invalid value for field {0}";
	    protected const string MsgErrorOccuredWhileWritingRecord = "Error occured while writing record. ";
	    protected const string MsgErrorWriting = "Error Writing";
	    protected const string MsgUnknownFieldType = "Unknown field type {0}";

		private DBFHeader header;
        private Stream defaultStream;
        private int recordCount;
        //private ArrayList v_records = new ArrayList();
		private readonly List<object[]> storedRecords = new List<object[]>();
        private string _dataMemoLoc;

        /// Creates an empty Object.
        public DBFWriter()
        {
            header = new DBFHeader();
        }

        /// Creates a DBFWriter which can append to records to an existing DBF file.
        /// @param dbfFile. The file passed in shouls be a valid DBF file.
        /// @exception Throws DBFException if the passed in file does exist but not a valid DBF file, or if an IO error occurs.
        public DBFWriter(String dbfFile)
        {
            try
            {
                defaultStream = File.Open(dbfFile, FileMode.OpenOrCreate, FileAccess.ReadWrite);
                _dataMemoLoc = Path.ChangeExtension(dbfFile, "dbt");
	            InitDBFWriter();
            }
            catch (FileNotFoundException e)
            {
                throw new DBFException(MsgSpecifiedFileIsNotFound, e);
            }
            catch (IOException e)
            {
                throw new DBFException(MsgWhileReadingHeader, e);
            }
            recordCount = header.NumberOfRecords;
        }

        public DBFWriter(Stream dbfFile)
        {
            defaultStream = dbfFile;
			if (InitDBFWriter()) return;
	        recordCount = header.NumberOfRecords;
        }

	    protected bool InitDBFWriter()
	    {
			/* before proceeding check whether the passed in File object
			 is an empty/non-existent file or not.
			 */
			header = new DBFHeader();
		    var isNewFile = defaultStream.Length == 0;
		    if (!isNewFile) ProcessHeader();
		    return isNewFile;
	    }

	    private void ProcessHeader()
	    {
		    header.Read(new BinaryReader(defaultStream));
		    /* position file pointer at the end of the defaultStream */
		    defaultStream.Seek(-1, SeekOrigin.End);
		    /* to ignore the END_OF_DATA byte at EoF */
	    }

	    public byte Signature
	    {
		    get { return header.Signature; }
		    set { header.Signature = value; }
	    }

	    public string DataMemoLoc
        {
            get { return _dataMemoLoc; }
            set { _dataMemoLoc = value; }
        }

	    public byte LanguageDriver
	    {
		    get { return header.LanguageDriver; }
		    set { SetLanguageDriver(value); }
	    }

	    public DBFField[] Fields
        {
		    get { return header.FieldArray; }
		    set { SetFields(value); }
        }

	    protected virtual void SetLanguageDriver(byte value)
	    {
		    if (header.LanguageDriver != 0x00)
		    {
			    throw new DBFException(MsgLanguagedriverHasAlreadyBeenSet);
		    }
		    header.LanguageDriver = value;
	    }

	    protected virtual void SetFields(DBFField[] value)
	    {
		    if (header.FieldArray != null)
			    throw new DBFException(MsgFieldsHasAlreadyBeenSet);

		    if (value == null || value.Length == 0)
			    throw new DBFException(MsgShouldHaveAtLeastOneField);

		    for (int i = 0; i < value.Length; i++)
		    {
			    if (value[i] == null)
				    throw new DBFException(string.Format(MsgFieldIsNull, i + 1));
		    }

		    header.FieldArray = value;

		    try
		    {
			    if (defaultStream != null && defaultStream.Length == 0)
			    {
				    /*
						 this is a new/non-existent file. So write header before proceeding
						 */
				    header.Write(new BinaryWriter(defaultStream));
			    }
		    }
		    catch (IOException e)
		    {
			    throw new DBFException(MsgErrorAccesingFile, e);
		    }
	    }

	    #region IDisposable Members

        /// <summary>Performs application-defined tasks associated with freeing, releasing,
        /// or resetting unmanaged resources.</summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Close();
        }

        #endregion

        /**
		 Add a record.
		 */

        public void WriteRecord(params Object[] values)
        {
	        if (defaultStream == null)
		        throw new DBFException(
			        MsgNotInitializedWithFileForWriterecordUseUseAddrecordInstead);
	        AddRecord(values, true);
        }

        public void AddRecord(params Object[] values)
        {
	        if (defaultStream != null)
		        throw new DBFException(MsgAppendingToAFileRequiresUsingWriterecordInstead);
	        AddRecord(values, false);
        }

        private void AddRecord(Object[] values, bool writeImediately)
        {
	        if (header.FieldArray == null)
		        throw new DBFException(MsgFieldsShouldBeSetBeforeAddingRecords);
	        if (values == null)
		        throw new DBFException(MsgNullCannotBeAddedAsRow);
	        if (values.Length != header.FieldArray.Length)
		        throw new DBFException(MsgInvalidRecordInvalidNumberOfFieldsInRow);

	        CheckValues(values);

            if (!writeImediately)
            {
                storedRecords.Add(values);
            }
            else
            {
                try
                {
                    WriteRecord(new BinaryWriter(defaultStream), values);
                    recordCount++;
                }
                catch (IOException e)
                {
                    throw new DBFException(MsgErrorOccuredWhileWritingRecord, e);
                }
            }
        }

	    private void CheckValues(IList<object> values)
	    {
		    for (int i = 0; i < header.FieldArray.Length; i++)
		    {
			    var currentValue = values[i];
			    if ((currentValue == null) || (currentValue is DBNull)) continue;

			    var isBadValue = false; 
				switch (header.FieldArray[i].DataType)
			    {
				    case NativeDbType.Char:
					    isBadValue = !(currentValue is String);
					    break;

				    case NativeDbType.Logical:
					    isBadValue = !(currentValue is Boolean);
					    break;

				    case NativeDbType.Date:
					    isBadValue = !(currentValue is DateTime);
					    break;

				    case NativeDbType.Numeric:
				    case NativeDbType.Float:
					    isBadValue = !(currentValue is IConvertible);
					    break;

				    case NativeDbType.Memo:
					    isBadValue = !(currentValue is MemoValue);
					    break;
			    }
			    if (isBadValue) throw new DBFException(string.Format(MsgInvalidValueForField, i));
		    }
	    }

	    ///Writes the set data to the OutputStream.
        public void Write(Stream tOut)
        {
            try
            {
                var outStream = new BinaryWriter(tOut);

                header.NumberOfRecords = storedRecords.Count;
                header.Write(outStream);

                /* Now write all the records */
	            storedRecords.ForEach(record => WriteRecord(outStream, record));

                outStream.Write(DBFFieldType.EndOfData);
                outStream.Flush();
            }
            catch (IOException e)
            {
                throw new DBFException(MsgErrorWriting, e);
            }
        }

        public void Close()
        {
            /* everything is written already. just update the header for record count and the END_OF_DATA mark */
            header.NumberOfRecords = recordCount;
	        if (defaultStream == null) return;
	        defaultStream.Seek(0, SeekOrigin.Begin);
	        header.Write(new BinaryWriter(defaultStream));
	        defaultStream.Seek(0, SeekOrigin.End);
	        defaultStream.WriteByte(DBFFieldType.EndOfData);
	        defaultStream.Close();
        }

        private void WriteRecord(BinaryWriter dataOutput, Object[] objectArray)
        {
            dataOutput.Write((byte) ' ');
            for (int fieldIndex = 0; fieldIndex < header.FieldArray.Length; fieldIndex++)
            {
	            /* iterate throught fields */
				var value = objectArray[fieldIndex];
	            var field = header.FieldArray[fieldIndex];
	            switch (field.DataType)
                {
                    case NativeDbType.Char:
		                dataOutput.Write(MakeCharField(value, field));
		                break;

                    case NativeDbType.Date:
						dataOutput.Write(MakeDateField(value, field));
		                break;

                    case NativeDbType.Float:
						dataOutput.Write(MakeFloatPointField(value, field, true));
		                break;

                    case NativeDbType.Numeric:
						dataOutput.Write(MakeFloatPointField(value, field, false));
                        break;

                    case NativeDbType.Logical:
		                dataOutput.Write(MakeLogicalField(value));
                        break;

                    case NativeDbType.Memo:
		                dataOutput.Write(MakeMemoField(value));
		                break;

                    default:
		                throw new DBFException(string.Format(MsgUnknownFieldType, field.DataType));
                }
            } /* iterating through the fields */
        }

	    private byte[] MakeMemoField(object value)
	    {
		    if (IsNullValue(value)) return Utils.textPadding("", CharEncoding, 10);
		    var tMemoValue = ((MemoValue) value);
		    tMemoValue.Write(this);
		    return Utils.NumericFormating(tMemoValue.Block, CharEncoding, 10, 0);
	    }

	    private byte MakeLogicalField(object value)
	    {
			if (IsNullValue(value)) return DBFFieldType.UnknownByte;
			return (bool)value ? DBFFieldType.True : DBFFieldType.False;
	    }

	    private byte[] MakeFloatPointField(object value, DBFField field, bool isFloat)
	    {
		    if (IsNullValue(value))
			    return Utils.textPadding(DBFFieldType.Unknown, CharEncoding, field.FieldLength, Utils.ALIGN_RIGHT);
		    var num = isFloat ? (IFormattable) Convert.ToDouble(value) : Convert.ToDecimal(value);
		    return Utils.NumericFormating(num, CharEncoding, field.FieldLength, field.DecimalCount);
	    }

	    private byte[] MakeDateField(object value, DBFField field)
	    {
		    if (IsNullValue(value)) return Utils.FillArray(new byte[8], DBFFieldType.Space);
		    var dateTime = (DateTime) value;
		    return CharEncoding.GetBytes(dateTime.ToString("yyyyMMdd"));
	    }

	    private byte[] MakeCharField(object value, DBFField field)
	    {
		    var strValue = IsNullValue(value) ? "" : value.ToString();
			return Utils.textPadding(strValue, CharEncoding, field.FieldLength);
	    }

	    private static bool IsNullValue(object value)
	    {
		    return value == null || value == DBNull.Value;
	    }
    }
}
