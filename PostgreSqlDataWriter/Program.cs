using System;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;

using PostgreSqlDataAccess;
using Sphere;

namespace PostgreSqlDataWriter
{
    class Program
    {
        static void Main (string[] args)
        {
            // Количество потоков добавления записей
            const int WRITERS_QUANTITY = 5;
            // Количество записей, добавляемых одним оператором INSERT
            const int INSERT_SIZE = 100;
            // Количество операций в транзакции
            const int TRANSACTION_SIZE = 50;
        
            // Строка подключения к БД
            // Можно формировать из конфигурационного файла, пароль запрашивать при запуске приложения
            string userName = "postgres";
            string password = "!Q2w3e4r";
            string connectionString =
                $"host=localhost;port=5432;database=Monitoring_V3;user id={userName};password={password};client encoding=utf-8;";
        
            // Ссылка на экземпляр класса модели БД
            MonitoringDb context = null;
        
            // Объект, выполняющий отслеживание длины очереди событий
            WritingQueueLengthLogger logger = null;
        
            // Объект, выполняющий добавление событий в БД
            EventsWriteAdapter writeAdapter = null; 

            try
            {
                Encoding.RegisterProvider( CodePagesEncodingProvider.Instance );
                
                Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff "));

                // Подключаемся к БД
                context = new MonitoringDb( connectionString );

                // Установка таймаута в секундах
                // context.Database.CommandTimeout = 30;

                // EF6 создаёт БД, если её не существует
                if (context.Database.EnsureCreated())
                    Console.WriteLine("Database created");

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

                int parametersQuantity; 
                    
                // Читаем стартовое количество записей в таблице событий
                int eventsCounter = context.ParameterEventsCount;
                Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                Console.WriteLine("We have " + eventsCounter + " event(s).");

                // Счетчик количества порций 
                int controlEventsCounter = 0;

                DateTime startTime;
                try
                {
                    // Создаём объект, который будет выполнять добавление событий в БД
                    writeAdapter = new EventsWriteAdapter(connectionString, WRITERS_QUANTITY, INSERT_SIZE,
                        TRANSACTION_SIZE);
                    // Добавляем обработчик события окончания добавления порции записей
                    writeAdapter.OnStored += logErrors;

                    // Создаём объект, который будет выполнять отслеживание длины очереди событий
                    logger = new WritingQueueLengthLogger( writeAdapter );
                    // Добавляем обработчик события, которое вызывается при получении очередного значения длины очереди 
                    logger.OnLogged += logQueueLen;

                    // Начинаем замер времени
                    startTime = DateTime.Now;
                    Console.WriteLine("Start time : " + startTime.ToString("HH:mm:ss.fff "));

                    int cyclesCount = 100; // количество циклов генерации данных
                    int node_idMax = 100; // node_id=[0..Max] - номер узла
                    int var_idMax = 100; // var_id=[0..Max] - номер переменной в узле
                    
                    Random rand = new Random();
                    DataStatus status = DataStatus.StatusOk; // статус, для записи в базу не важен  
                    Int32 counter = 0; // счетчик события генерации переменной, для записи в базу не важен          

                    for (int cycle = 0; cycle < cyclesCount; cycle++)
                    {
                        // Console.WriteLine("Cycle " + cycle.ToString());

                        DateTime timestamp = DateTime.UtcNow; // метка времени генерации переменной 
                        for (int node_id = 1; node_id <= node_idMax; node_id++)
                        {
                            for (int var = 1; var <= var_idMax; var++)
                            {
                                double val = 1000.0 * rand.NextDouble(); // случайное число [0..1000]

                                SphereDataTypesScalarSW_t scalar =
                                    new SphereDataTypesScalarSW_t(node_id, var, val, timestamp, counter, status);
                               
                                // Запись объекта scalar в очередь на сохранение в DB
                                writeAdapter.StoreEvent( scalar );
                                
                                controlEventsCounter++;
                            }
                        }

                        // пауза между циклами генерации данных
                        Task.Delay( 10 ).Wait();
                    }

                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    Console.WriteLine("Writing to adapter was stopped.");
                }
                finally
                {
                    // Освобождаем объект, выполнявший добавление событий в БД
                    // Это может занять довольно много времени, ведь нужно дождаться, когда события, накопившиеся в очереди, будут добавлены в БД
                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    
                    writeAdapter?.WaitForStoring();

                    // Считаем, сколько записей теперь в таблице.
                    // Тоже может занять значительное время, так как записей может быть очень много.
                    // Из-за этой операции пришлось устанавливать таймаут соединения побольше стандартного
                    parametersQuantity = context.ParameterEventsCount;
                    
                    Console.WriteLine("Disposing adapter ...");
                    writeAdapter?.Dispose();

                    Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                    Console.WriteLine("done.");

                    // Объект, отслеживавший состояние очереди тоже освобождаем, ведь поток отслеживания нужно корректно остановить
                    logger?.Dispose();
                }

                // Замеряем время окончания добавления
                DateTime endTime = DateTime.Now;
                Console.WriteLine("Writing duration is " + (endTime - startTime).TotalMilliseconds + " milliseconds.");

                // Всего записей в таблице
                // Считаем разницу между было и стало для определения фактического количества добавленных записей
                Console.WriteLine("We have written " + (parametersQuantity - eventsCounter) + " event(s).");

                double timePerTenThousand =
                    (endTime - startTime).TotalMilliseconds / (parametersQuantity - eventsCounter) * 10000;
                Console.WriteLine(timePerTenThousand.ToString("N3") + " milliseconds per 10000 events.");
                
                // Выводим количество записей, которое должно было добавиться
                Console.WriteLine("We have to write " + controlEventsCounter + " event(s).");
            }
            catch (Npgsql.PostgresException pgException)
            {
                if (pgException.SqlState == "28P01")
                {
                    Console.WriteLine( $"Пользователь \"{userName}\" не прошёл проверку подлинности (по паролю)" );
                }
                else
                {
                    reportException( pgException );
                }
            }
            catch (Exception error)
            {
                reportException( error );
            }
            finally
            {
                // Делаем отметки времени в консоль и закрываем соединение с БД
                Console.Write(DateTime.Now.ToString("HH:mm:ss.fff "));
                Console.WriteLine("Deleting of the DB connection.");
                context?.Dispose();
                Console.WriteLine("The DB connection was deleted.");
            }

            // Ждём нажатия любой клавиши
            Console.WriteLine("Press a key");
            Console.ReadKey(true);
        }
        
        /// <summary>
        /// Метод для вывода в консоль длины очереди
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
            if (errors == null || errors.Count == 0)
                return;
            
            Console.Write( DateTime.Now.ToString("HH:mm:ss.fff ") );
            Console.WriteLine( "There are errors!" );

            using var enumerator = errors.GetEnumerator();
            do {
                if (enumerator.Current != null)
                {
                    Console.WriteLine( "Error: " + enumerator.Current.Message );
                }
            } while (enumerator.MoveNext());
        }

         /// <summary>
         /// Вывод исключения в консоль
         /// Выводит переданное исключение и всю цепочку InnerException
         /// </summary>
         private static void reportException(Exception error)
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
         
                 Console.WriteLine( indentBuilder + innerException.Message );
                 // Console.WriteLine( indentBuilder.ToString() + innerException.ToString() );
                 innerException = innerException.InnerException;
             }
         }
   }
}