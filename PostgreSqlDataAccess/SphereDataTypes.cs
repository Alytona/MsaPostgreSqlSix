using System;
using System.Collections.Generic;
using System.Text;

using PostgreSqlDataAccess;

namespace Sphere {
  //const UInt32 StatusOk = 0x80000000;
  //const UInt32 StatusEmu = 0x40000000;
  //const UInt32 StatusReq = 0x20000000;

//#define StatusOk   0x80000000
//#define StatusEmu  0x40000000
//#define StatusReq  0x20000000
  public enum DataStatus :Int32 {  /// ������ ������ � ������
    StatusOk = 0x08000000,
    StatusEmu= 0x40000000,
    StatusReq= 0x20000000,
    NotInit  = 0x10000000
  }


  public enum SphereDataTypes_t :UInt32 {  /// ��� ������ � ������
    Scalar      = 0,
    SampleArray = 1,
    Logic = 2,
    UserAction  = 3
  }



  public enum MBDataTypes_t :UInt32 { /// ��� ������ ��� ������ � Modbus
      Bit_t = 0,
      Float_t = 1,
      int16_t = 2,
      uint16_t = 3,
      int32_t = 4,
      uint32_t = 5
  }

  public enum CombineFormat :UInt32 { /// ������ ��������� 32-������ ��������
      ABCD = 0,
      CDAB = 1,
      DCBA = 2,
      BADC = 3
  }

  [Serializable]
  public class SphereDataTypesScalarSW_t : IGroupInsertableRecord
  {
    // ��������� ����������� ����� ������ ������������ ��� ���������� ������� ��� ���������� ���������� �������

    /// <summary>
    /// ���������� �������
    /// </summary>
    internal static readonly uint ColumnsQuantity = 6;
    /// <summary>
    /// ������ ������� ��� ���������� �������� ����� �� ����������� �������
    /// </summary>
    internal static readonly string ValuesPartFormat = "(@p{0}, @p{1}, @p{2}, @p{3}, @p{4}, @p{5})";
    
    /// ��������� �������� SW
    // ����� 32 �����
    public Int32 node_id;
    public Int32 var_id;          // int ParameterId ? (������������ ��� ���������� ����� �������)
    public Double val;            // Double -> db float Value
    public DateTime timestamp;    // db DateTime Time
    public Int32 counter;
    DataStatus status;            // uint -> db int Status         
                                  // int EventId
    
    
    public SphereDataTypesScalarSW_t(Int32 _node_id, Int32 _var_id, Double _val, DateTime _timestamp, Int32 _counter, DataStatus _status)
    {
      node_id   = _node_id;
      var_id    = _var_id;
      val       = _val;
      timestamp = _timestamp;
      counter   = _counter;
      status    = _status;
    }

    public SphereDataTypesScalarSW_t(ArraySegment<Byte> sc_bytes) {
      node_id   = BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 0);
      var_id    = BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 4);
      val       = BitConverter.ToDouble(sc_bytes.Array, sc_bytes.Offset + 8);// ��� ������������ ������ ���������� ����� � ���������� ����!!!
      timestamp = new DateTime(BitConverter.ToInt64(sc_bytes.Array, sc_bytes.Offset + 16)); 
      counter   = BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 24);
      status    = (DataStatus)BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 28);
    }

    /// <summary>
    /// ����� ���������� ������� �� ���������� ����� ������.
    /// ����������� ������� � ���������� �������. 
    /// ������ ������������ ��� �������� �������� ����� ����������� ������� � ����� ���������� �������. 
    /// </summary>
    /// <param name="fieldValues">������, ���� ����� �������� ��������</param>
    /// <param name="valuesIndex">������ � �������, ������� � �������� ����������� ��������</param>
    public void FillValues (object[] fieldValues, uint valuesIndex)
    {
      fieldValues[valuesIndex + 1] = node_id;
      fieldValues[valuesIndex + 2] = val;
      fieldValues[valuesIndex + 3] = timestamp;
      fieldValues[valuesIndex + 4] = counter;
      fieldValues[valuesIndex + 5] = status;
    }
    
    public Byte[] GetBytes()
    {
      Byte[] data = new byte[32];

      Byte[] nodeBytes = BitConverter.GetBytes(node_id);
      Byte[] varBytes = BitConverter.GetBytes(var_id);
      Byte[] valBytes = BitConverter.GetBytes(val);
      Byte[] timestampBytes = BitConverter.GetBytes(timestamp.Ticks);
      Byte[] counterBytes = BitConverter.GetBytes(counter);
      Byte[] statusBytes = BitConverter.GetBytes((Int32)status);

      Buffer.BlockCopy(nodeBytes, 0, data,0,4 );
      Buffer.BlockCopy(varBytes, 0, data,4,4 );
      Buffer.BlockCopy(valBytes, 0, data,8,8 );
      Buffer.BlockCopy(timestampBytes, 0, data,16,8 );
      Buffer.BlockCopy(counterBytes, 0, data,24,4 );
      Buffer.BlockCopy(statusBytes, 0, data,28,4 );

      return data;
    }
  }

  /// <summary>
    /// ���������� ����������� ���������� ��������� insert ��� ������� ParameterEvents
    /// </summary>
    class EventsGroupInsertMaker : AGroupInsertMaker
    {
        private readonly Dictionary<int, string> MonthsDictionary = new Dictionary<int, string>();

        public int ParameterId
        {
            get; set;
        }

        /// <summary>
        /// �����������
        /// </summary>
        /// <param name="insertSize">������������ ���������� �������, ����������� ����� ���������� insert</param>
        public EventsGroupInsertMaker (uint insertSize) : base( SphereDataTypesScalarSW_t.ColumnsQuantity, insertSize )
        {
        }

        /// <summary>
        /// ����� �������� ���� ������� � ��������� ����� �����������
        /// </summary>
        /// <param name="eventsToStore">��������� �������, �������������� ��� ����������</param>
        protected override void checkCollectionType (IList<IGroupInsertableRecord> eventsToStore)
        {
            if (eventsToStore is IList<SphereDataTypesScalarSW_t>)
                return;

            throw new ArrayTypeMismatchException( "�������� ��������� � �������� ����� �������" );
        }

        protected override StringBuilder makeQueryBuilder ()
        {
            StringBuilder queryBuilder = new StringBuilder( "insert into \"var_" + ParameterId + "\" (year_month, node_id, event_value, event_time, event_counter, event_status) values " );
            return queryBuilder;
        }

        protected override void FillValuesForRecord (IGroupInsertableRecord record, object[] fieldValues, uint valuesIndex)
        {
            record.FillValues( fieldValues, valuesIndex );

            if (record is SphereDataTypesScalarSW_t parEventRecord) 
            {
                int year = parEventRecord.timestamp.Year - 2000;
                int month = parEventRecord.timestamp.Month;
                int monthKey = (year << 4) + month;

                if (!MonthsDictionary.TryGetValue( monthKey, out string monthText ))
                {
                    monthText = $"{year}_{month:D2}";
                    MonthsDictionary.Add( monthKey, monthText );
                }
                fieldValues[valuesIndex] = monthText;
            }
        }
    }

  /// <summary>
  /// ���������� �������, ���������������� ����� ���������� �������
  /// </summary>
  class EventsGroupRecordsWriter : AGroupRecordsWriter
  {
      /// <summary>
      /// �����������
      /// </summary>
      /// <param name="connectionString">������ � ����������� ���������� � �������� ��</param>
      /// <param name="insertSize">������������ ���������� �������, ����������� ����� ���������� insert</param>
      /// <param name="transactionSize">������������ ���������� ���������� insert � ����� ����������</param>
      public EventsGroupRecordsWriter (string connectionString, uint insertSize, uint transactionSize) : base( connectionString, new EventsGroupInsertMaker( insertSize ), transactionSize )
      {
      }

      public void setParameterId (int parameterId)
      {
          if (InsertMaker is EventsGroupInsertMaker insertMaker) {
              insertMaker.ParameterId = parameterId;
          }
      }
  }
}
