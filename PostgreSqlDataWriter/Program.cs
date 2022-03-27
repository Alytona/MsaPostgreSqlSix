using System;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using PostgreSqlDataAccess;

namespace PostgreSqlDataWriter
{
    /// <summary>
    /// Ссылочный класс, который содержит порцию событий для добавления
    /// </summary>
    class EventsBulk
    {
        /// <summary>
        /// Коллекция событий
        /// </summary>
        private List<ParameterEvent> Events;
    
        /// <summary>
        /// Конструктор порции с заданным количеством событий
        /// <param name="quantity">Количество событий</param>
        /// </summary>
    /*
        EventsBulk (int quantity) {
    
            int portionQuantity = quantity / 6;
    
            DateTime currentTime = DateTime.Now;
            Events = new List< ParameterEvent >(quantity);
            int i = 0;
            for (; i < portionQuantity * 2 + 500; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = 1; // i % 10;
                parameterEvent.Time = DateTime.Now; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
            }
            for (; i < portionQuantity * 4 + 700; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = 2; // i % 10;
                parameterEvent.Time = DateTime.Now; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
            }
            for (; i < portionQuantity * 5; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = 3; // i % 10;
                parameterEvent.Time = DateTime.Now; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
            }
            for (; i < portionQuantity * 5 + 1000; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = 6; // i % 10;
                parameterEvent.Time = DateTime.Now; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
            }
            for (; i < quantity; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = 7; // i % 10;
                parameterEvent.Time = DateTime.Now; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
            }
        }
    */
        public EventsBulk(int quantity) {
    
            DateTime currentTime = DateTime.Now;
            Events = new List< ParameterEvent >(quantity);
            for (int i = 0; i < quantity; i++)
            {
                ParameterEvent parameterEvent = new ParameterEvent();
                parameterEvent.ParameterId = i % 1000;
                parameterEvent.Time = DateTime.UtcNow; // currentTime;
                parameterEvent.Value = 0.011F;
                parameterEvent.Status = 11;
                Events.Add(parameterEvent);
                // currentTime = currentTime.AddMinutes(30);
            }
        }
    
        /// <summary>
        /// Метод для получения коллекции событий, хранящейся в экземпляре класса
        /// </summary>
        public List< ParameterEvent > getEvents() {
            return Events;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Количество потоков добавления записей
            const int WRITERS_QUANTITY = 3;
            // Количество записей, добавляемых одним оператором INSERT
            const int INSERT_SIZE = 200;
            // Количество операций в транзакции
            const int TRANSACTION_SIZE = 10;
        
            // Строка подключения к БД
            // Можно формировать из конфигурационного файла, пароль запрашивать при запуске приложения
            String connectionString = "host=localhost;port=5432;database=Monitoring_V2;user id=postgres;password=!Q2w3e4r;";
        
            // Ссылка на экземпляр класса модели БД
            MonitoringDb context = null;
        
            // Объект, выполняющий отслеживание длины очереди событий
            WritingQueueLengthLogger logger = null;
        
            // Объект, выполняющий добавление событий в БД
            EventsWriteAdapter writeAdapter = null;
        
            try
            {
                Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff "));
        
                // Подключаемся к БД
                context = new MonitoringDb(connectionString);
        
                // Установка таймаута в секундах
                // context.Database.CommandTimeout = 30;
        
                // EF6 создаёт БД, если её не существует
                if (context.Database.EnsureCreated())
                    Console.WriteLine( "Database created" );
        
                // Заполнение таблицы параметров
                //for (int i = 1; i <= 1000; i++) 
                //{
                //    Parameter parameter = new Parameter();
                //    parameter.Name = "parameter " + i;
                //    parameter.Comment = "Комментарий к параметру";
                //    parameter.Units = "inches, дюймы по-нашему";
                //    parameter.Source = "Откуда ни возьмись";
                //    context.Parameters.Add(parameter);
                //}
                //context.SaveChanges();
        
                //int startEventId = context.LastParameterEventId.HasValue ? context.LastParameterEventId.Value : -1;
                //Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                //Console.WriteLine("Last id : " + startEventId);
        
                // Читаем стартовое количество записей в таблице событий
                int counter1 = context.ParameterEventsCount;
                Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                Console.WriteLine("We have " + counter1 + " event(s).");
        
                // Счетчик количества порций 
                int counter2 = 0;
        
                // Размер порции событий
                int quantity = 1000; // 10000
        
                DateTime startTime;
                try {
                    // Создаём объект, который будет выполнять добавление событий в БД
                    writeAdapter = new EventsWriteAdapter( connectionString, WRITERS_QUANTITY, INSERT_SIZE, TRANSACTION_SIZE);
                    // Добавляем обработчик события окончания добавления порции записей
                    writeAdapter.OnStored += new GroupRecordsWriteAdapter.StoredEventHandler( logErrors );
        
                    // Создаём объект, который будет выполнять отслеживание длины очереди событий
                    logger = new WritingQueueLengthLogger(writeAdapter);
                    // Добавляем обработчик события, которое вызывается при получении очередного значения длины очереди 
                    logger.OnLogged += new WritingQueueLengthLogger.LogQueueLenEventHandler( logQueueLen );
        
                    // Создаём порцию событий.
                    // Порция используется на все операции одна, чтобы не исключить из замеров время на её создание
                    EventsBulk bulk = new EventsBulk( quantity );
        
                    // Начинаем замер времени
                    startTime = DateTime.Now;
                    Console.WriteLine("Start time : " + startTime.ToString("HH:mm:ss.fff "));
                    do
                    {
                        // Добавляем порцию в БД
                        writeAdapter.StoreEvents(bulk.getEvents());
        
                        Thread.Sleep(50);
                        counter2++;
        
                        // break;
        
                    } while ((DateTime.Now - startTime).TotalSeconds < 60);
                    // До тех пор, пока не пройдет 60 секунд
        
                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    Console.WriteLine("Writing to adapter was stopped.");
                }
                finally
                {
                    // Освобождаем объект, выполнявший добавление событий в БД
                    // Это может занять довольно много времени, ведь нужно дождаться, когда события, накопившиеся в очереди, будут добавлены в БД
                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    Console.WriteLine("Disposing adapter ...");
                    if (writeAdapter != null)
                        writeAdapter.Dispose();
        
                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    Console.WriteLine("done.");
        
                    // Объект, отслеживавший состояние очереди тоже освобождаем, ведь поток отслеживания нужно корректно остановить
                    if (logger != null)
                        logger.Dispose();
                }
        
                // Замеряем время окончания добавления
                DateTime endTime = DateTime.Now;
                Console.WriteLine("Writing duration is " + (endTime - startTime).TotalMilliseconds + " milliseconds.");
/*        
                // Считаем время, потраченное на запись 10000 событий
                // Чтобы определить время на запись 50000, надо просто умножить на 5.
                double timePerTenThousand = (double)(endTime - startTime).TotalMilliseconds / (counter2 * quantity / 10000);
                Console.WriteLine(timePerTenThousand.ToString("N3") + " milliseconds per 10000 events.");
        
                // Считаем, сколько записей теперь в таблице.
                // Тоже может занять значительное время, так как записей может быть очень много.
                // Из-за этой операции пришлось устанавливать таймаут соединения побольше стандартного
                Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff "));
                int parametersQuantity = context.ParameterEventsCount;
        
                //int endEventId = context.LastParameterEventId;
                //Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                //Console.WriteLine("Last id : " + endEventId);
        
                // Всего записей в таблице
                Console.WriteLine("We have " + parametersQuantity + " event(s).");
                // Считаем разницу между было и стало для определения фактического количества добавленных записей
                Console.WriteLine("We have written " + (parametersQuantity - counter1) + " event(s).");
                // Console.WriteLine("We have written " + (endEventId - startEventId) + " event(s).");
                // Выводим количество записей, которое должно было добавиться
                Console.WriteLine("We have written " + counter2 * quantity + " event(s).");
*/                
            }
            catch (System.Exception error)
            {
                reportException(error);
            }
            finally
            {
                // Делаем отметки времени в консоль и закрываем соединение с БД
                Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                Console.WriteLine("Deleting of the DB connection.");
                if (context != null)
                    context.Dispose();
                Console.WriteLine("The DB connection was deleted.");
            }
        
            // Ждём нажатия любой клавиши
            Console.WriteLine("Press a key");
            Console.ReadKey(true);
        }
        
        /// <summary>
        /// Метод для вывода в консоль длины очереди
        /// <param name="queueLen">Количество событий в очереди</param>
        /// </summary>
        private static void logQueueLen (uint preparedQueueLen, uint storingQueueLen, uint errorsQuantity )
        {
            Console.Write( DateTime.Now.ToString( "HH:mm:ss.fff " ) );
            // Console.WriteLine( "Queue length is " + (preparedQueueLen + storingQueueLen - errorsQuantity));
            Console.WriteLine( "Prepared events : " + preparedQueueLen + ", storing events : " + storingQueueLen + ", errors : " + errorsQuantity );
        }
        
        /// <summary>
        /// Метод для вывода в консоль количества добавленных записей и коллекции ошибок 
        /// <param name="storedCount">Количество добавленных записей</param>
        /// <param name="errors">Коллекция ошибок</param>
        /// </summary>
        private static void logErrors (uint storedCount, List <Exception> errors)
        {
            // Если коллекция ошибок не пуста, выводим их в консоль
            if (errors != null && errors.Count > 0) {
                Console.Write( DateTime.Now.ToString("HH:mm:ss.fff ") );
                Console.WriteLine( "There are errors!" );
                var enumerator = errors.GetEnumerator();
                do {
                    if (enumerator.Current != null)
                    {
                        Console.WriteLine( "Error: " + enumerator.Current.Message );
                    }
                } while (enumerator.MoveNext());
            }
        }

         /// <summary>
         /// Вывод исключения в консоль
         /// Выводит переданное исключение и всю цепочку InnerException
         /// </summary>
         public static void reportException(Exception error)
         {
             // Отступ, для наглядности
             StringBuilder indentBuilder = new StringBuilder( "" );
         
             Console.WriteLine(error.Message);
             // Console.WriteLine(error.ToString());
         
             // Перебираем и выводим цепочку InnerException
             Exception innerException = error.InnerException;
             while (innerException != null) 
             {
                 // Увеличиваем отступ
                 indentBuilder.Append("  ");
         
                 Console.WriteLine( indentBuilder.ToString() + innerException.Message );
                 // Console.WriteLine( indentBuilder.ToString() + innerException.ToString() );
                 innerException = innerException.InnerException;
             }
         }
   }
}