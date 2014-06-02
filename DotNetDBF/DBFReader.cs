/*
 DBFReader
 Class for reading the records assuming that the given
 InputStream comtains DBF data.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 License: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Linq;

namespace DotNetDBF
{
    public class DBFReader : DBFBase, IDisposable
    {
	    protected const string msgSourceIsNotOpen = "Source is not open";
	    protected const string msgFailedToReadDbf = "Failed To Read DBF";
		protected const string msgProblemReadingFile = "Problem Reading File";
	    protected const string msgMemoLocationNotSet = "Memo Location Not Set";
	    protected const string msgFailedToParseNumber = "Failed to parse Number";
	    protected const string msgFailedToParseFloat = "Failed to parse Float";
	    
		private BinaryReader _dataInputStream;
        private DBFHeader _header;
        private string _dataMemoLoc;

        private int[] _selectFields = new int[]{};
        private int[] _orderedSelectFields = new int[] { };
        /* Class specific variables */
        private bool isClosed = true;

        /**
		 Initializes a DBFReader object.
		 
		 When this constructor returns the object
		 will have completed reading the hader (meta date) and
		 header information can be quried there on. And it will
		 be ready to return the first row.
		 
		 @param InputStream where the data is read from.
		 */

        public void SetSelectFields(params string[] aParams)
        {
            _selectFields =
                aParams.Select(
                    it =>
                    Array.FindIndex(_header.FieldArray,
                                    jt => jt.Name.Equals(it, StringComparison.InvariantCultureIgnoreCase))).ToArray();
            _orderedSelectFields = _selectFields.OrderBy(it => it).ToArray();
        }

        public DBFField[] GetSelectFields()
        {
            return _selectFields.Any()
                ? _selectFields.Select(it => _header.FieldArray[it]).ToArray()
                : _header.FieldArray;
        }

        public DBFReader(string anIn)
        {
            try
            {
                _dataInputStream = new BinaryReader(
                    File.Open(anIn,
                              FileMode.Open,
                              FileAccess.Read,
                              FileShare.Read)
                    );

                var dbtPath = Path.ChangeExtension(anIn, "dbt");
                if (File.Exists(dbtPath))
                {
                    _dataMemoLoc = dbtPath;
                }

                isClosed = false;
                _header = new DBFHeader();
                _header.Read(_dataInputStream);

                /* it might be required to leap to the start of records at times */
                int t_dataStartIndex = _header.HeaderLength
                                       - (32 + (32 * _header.FieldArray.Length))
                                       - 1;
                if (t_dataStartIndex > 0)
                {
                    _dataInputStream.ReadBytes((t_dataStartIndex));
                }
            }
            catch (IOException ex)
            {
                throw new DBFException(msgFailedToReadDbf, ex);
            }
        }

        public DBFReader(Stream anIn)
        {
            try
            {
                _dataInputStream = new BinaryReader(anIn);
                isClosed = false;
                _header = new DBFHeader();
                _header.Read(_dataInputStream);

                /* it might be required to leap to the start of records at times */
                var t_dataStartIndex = _header.HeaderLength
                                       - (32 + (32 * _header.FieldArray.Length))
                                       - 1;
                if (t_dataStartIndex > 0)
                {
                    _dataInputStream.ReadBytes((t_dataStartIndex));
                }
            }
            catch (IOException e)
            {
                throw new DBFException(msgFailedToReadDbf, e);
            }
        }

        /**
		 Returns the number of records in the DBF.
		 */

        public int RecordCount
        {
            get { return _header.NumberOfRecords; }
        }

        /**
		 Returns the asked Field. In case of an invalid index,
		 it returns a ArrayIndexOutofboundsException.
		 
		 @param index. Index of the field. Index of the first field is zero.
		 */

        public DBFField[] Fields
        {
            get { return _header.FieldArray; }
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


	    public string DataMemoLoc
	    {
		    get { return _dataMemoLoc; }
		    set { _dataMemoLoc = value; }
	    }

	    public override String ToString()
        {
			var sb = new StringBuilder();
		    sb.AppendFormat("{0}/{1}/{2}\n", _header.Year, _header.Month, _header.Day);
		    sb.AppendFormat("Total records: {0}\n", _header.NumberOfRecords);
		    sb.AppendFormat("Header length: {0}\n", _header.HeaderLength);
            foreach (DBFField field in _header.FieldArray) sb.AppendFormat("{0}\n", field.Name);
            return sb.ToString();
        }

        public void Close()
        {
            _dataInputStream.Close();
            isClosed = true;
        }

        /**
		 Reads the returns the next row in the DBF stream.
		 @returns The next row as an Object array. Types of the elements
		 these arrays follow the convention mentioned in the class description.
		 */
        public Object[] NextRecord()
        {
            return NextRecord(_selectFields, _orderedSelectFields);
        }


        internal Object[] NextRecord(IEnumerable<int> selectIndexes, IList<int> sortedIndexes)
        {
	        if (isClosed) throw new DBFException(msgSourceIsNotOpen);
	        
			var tOrderedSelectIndexes = sortedIndexes;
            var recordObjects = new Object[_header.FieldArray.Length];

            try
            {
	            if (!SkipDeletedRecords()) return null;

                int j = 0;
                int k = -1;
				int count = tOrderedSelectIndexes.Count;
				
				for (int i = 0; i < _header.FieldArray.Length; i++)
                {
	                var fieldLength = _header.FieldArray[i].FieldLength;

	                bool noNeedInSelect = count == j && j != 0;
	                bool skipField = noNeedInSelect || (count > j && tOrderedSelectIndexes[j] > i && tOrderedSelectIndexes[j] != k);
	                if (skipField)
                    {
                        _dataInputStream.BaseStream.Seek(fieldLength, SeekOrigin.Current);
                        continue;
                    }
                    if (count > j) k = tOrderedSelectIndexes[j];
                    j++;
                  
                    switch (_header.FieldArray[i].DataType)
                    {
                        case NativeDbType.Char:
                            var b_array = _dataInputStream.ReadBytes(fieldLength);
		                    recordObjects[i] = CharEncoding.GetString(b_array).TrimEnd();
                            break;
                        case NativeDbType.Date:
		                    recordObjects[i] = ReadDateValue();
                            break;
                        case NativeDbType.Float:
		                    recordObjects[i] = ReadFloatValue(fieldLength);
                            break;
                        case NativeDbType.Numeric:
		                    recordObjects[i] = ReadNumericValue(fieldLength);
                            break;
                        case NativeDbType.Logical:
		                    recordObjects[i] = ReadLogicValue();
                            break;
                        case NativeDbType.Memo:
		                    recordObjects[i] = ReadMemoValue(fieldLength);
                            break;
                        default:
                            _dataInputStream.ReadBytes(fieldLength);
                            recordObjects[i] = DBNull.Value;
                            break;
                    }

                 
                }
            }
            catch (EndOfStreamException)
            {
                return null;
            }
            catch (IOException e)
            {
                throw new DBFException(msgProblemReadingFile, e);
            }

            return selectIndexes.Any() ? selectIndexes.Select(it => recordObjects[it]).ToArray() : recordObjects;
        }

	    protected bool SkipDeletedRecords()
	    {
		    var isDeleted = false;
		    do
		    {
			    if (isDeleted) _dataInputStream.ReadBytes(_header.RecordLength - 1);
			    var t_byte = _dataInputStream.ReadByte();
			    if (t_byte == DBFFieldType.EndOfData) return false;
				isDeleted = t_byte == '*';
		    } while (isDeleted);
		    return true;
	    }

	    private object ReadMemoValue(int fieldLength)
	    {
		    if (string.IsNullOrEmpty(_dataMemoLoc))
			    throw new Exception(msgMemoLocationNotSet);
		    var tRawMemoPointer = _dataInputStream.ReadBytes(fieldLength);
		    var tMemoPoiner = CharEncoding.GetString(tRawMemoPointer);
		    //Because Memo files can vary and are often the least importat data, 
		    //we will return null when it doesn't match our format.
		    if (!string.IsNullOrEmpty(tMemoPoiner))
		    {
			    long tBlock;
			    if (long.TryParse(tMemoPoiner, out tBlock))
				    return new MemoValue(tBlock, this, _dataMemoLoc);
		    }
		    return DBNull.Value;
	    }

	    private object ReadLogicValue()
	    {
		    var data = _dataInputStream.ReadByte();
		    //todo find out whats really valid
		    if (data == DBFFieldType.UnknownByte) return DBNull.Value;
		    return data == 'Y' || data == 'y' || data == 'T' || data == 't';
	    }

	    private object ReadNumericValue(int fieldLength)
	    {
		    try
		    {
			    var tParsed = GetBytesAsString(fieldLength);
			    if (IsGoodString(tParsed))
			    {
				    return Decimal.Parse(tParsed.Replace(',', '.'),
				                          NumberStyles.Float | NumberStyles.AllowLeadingWhite,
				                          CultureInfo.InvariantCulture);
			    }
		    }
		    catch (FormatException e)
		    {
			    throw new DBFException(msgFailedToParseNumber, e);
		    }
		    return null;
	    }

	    private static bool IsGoodString(string tParsed)
	    {
		    var tLast = tParsed.Substring(tParsed.Length - 1);
		    bool isGoodString = tParsed.Length > 0 && tLast != " " && tLast != DBFFieldType.Unknown;
		    return isGoodString;
	    }

	    private string GetBytesAsString(int fieldLength)
	    {
		    var data = _dataInputStream.ReadBytes(fieldLength);
		    var tParsed = CharEncoding.GetString(data);
		    return tParsed;
	    }

	    private object ReadFloatValue(int fieldLength)
	    {
		    try
		    {
				var tParsed = GetBytesAsString(fieldLength);
				if (IsGoodString(tParsed))
				{
					return Double.Parse(tParsed.Replace(',', '.'),
					                     NumberStyles.Float | NumberStyles.AllowLeadingWhite,
					                     CultureInfo.InvariantCulture);
				}
		    }
		    catch (FormatException e)
		    {
			    throw new DBFException(msgFailedToParseFloat, e);
		    }
		    return null;
	    }

	    private object ReadDateValue()
	    {
		    var bytesYear = _dataInputStream.ReadBytes(4);
		    var bytesMonth = _dataInputStream.ReadBytes(2);
		    var bytesDay = _dataInputStream.ReadBytes(2);

		    try
		    {
			    var tYear = CharEncoding.GetString(bytesYear);
			    var tMonth = CharEncoding.GetString(bytesMonth);
			    var tDay = CharEncoding.GetString(bytesDay);

			    int tIntYear, tIntMonth, tIntDay;
			    if (Int32.TryParse(tYear, out tIntYear) &&
			        Int32.TryParse(tMonth, out tIntMonth) &&
			        Int32.TryParse(tDay, out tIntDay))
			    {
				    return new DateTime(tIntYear, tIntMonth, tIntDay);
			    }
		    }
		    catch (ArgumentOutOfRangeException)
		    {
			    /* this field may be empty or may have improper value set */
		    }
		    return null;
	    }
    }
}