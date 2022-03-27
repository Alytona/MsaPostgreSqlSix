using System;
using System.Collections.Generic;

namespace Sphere {
  //const UInt32 StatusOk = 0x80000000;
  //const UInt32 StatusEmu = 0x40000000;
  //const UInt32 StatusReq = 0x20000000;

//#define StatusOk   0x80000000
//#define StatusEmu  0x40000000
//#define StatusReq  0x20000000
  public enum DataStatus :UInt32 {  /// Статус данных в пакете
    StatusOk = 0x80000000,
    StatusEmu= 0x40000000,
    StatusReq= 0x20000000,
    NotInit  = 0x10000000
  }


  public enum SphereDataTypes_t :UInt32 {  /// Тип данных в пакете
    Scalar      = 0,
    SampleArray = 1,
    Logic = 2,
    UserAction  = 3
  }



  public enum MBDataTypes_t :UInt32 { /// Тип данных для обмена с Modbus
      Bit_t = 0,
      Float_t = 1,
      int16_t = 2,
      uint16_t = 3,
      int32_t = 4,
      uint32_t = 5
  }


  public enum CombineFormat :UInt32 { /// Формат кодировки 32-битных значений
      ABCD = 0,
      CDAB = 1,
      DCBA = 2,
      BADC = 3
  }

  [Serializable]
  public class SphereDataTypesScalarSW_t
  {
    /// Скалярное значение SW
    // всего 32 байта
    public Int32 node_id;
    public Int32 var_id;
    public Double val;
    public DateTime timestamp;
    public Int32 counter;
    DataStatus status;
    
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
      val       = BitConverter.ToDouble(sc_bytes.Array, sc_bytes.Offset + 8);// при некорректных байтах исключение будет в вызывающем коде!!!
      timestamp = new DateTime(BitConverter.ToInt64(sc_bytes.Array, sc_bytes.Offset + 16)); 
      counter   = BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 24);
      status    = (DataStatus)BitConverter.ToInt32(sc_bytes.Array, sc_bytes.Offset + 28);
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
}
