using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Базовый класс объектов, инкапсулирующих потоки добавления записей
    /// </summary>
    public abstract class AGroupRecordsWriter : IDisposable
    {
        /// <summary>
        /// Объект модели БД
        /// </summary>
        MonitoringDb DbContext;

        /// <summary>
        /// Признак того, что подключение к БД установлено
        /// </summary>
        bool DbInited;

        /// <summary>
        /// Количество операторов добавления в транзакции
        /// </summary>
        readonly uint TransactionSize;

        /// <summary>
        /// Строка подключения к БД
        /// </summary>
        public string ConnectionString
        {
            get; private set;
        }

        /// <summary>
        /// Объект, выполняющий создание запроса
        /// </summary>
        protected readonly AGroupInsertMaker InsertMaker;

        /// <summary>
        /// Обработчик события, вызываемого после добавления очередной порции записей
        /// </summary>
        /// <param name="storedQuantity">Количество добавленных записей</param>
        public delegate void StoredEventHandler (uint storedQuantity);
        /// <summary>
        /// Событие, вызываемое после добавления очередной порции записей
        /// </summary>
        public event StoredEventHandler OnStored;

        /// <summary>
        /// Обработчик события, вызываемого при возникновении ошибок добавления
        /// </summary>
        /// <param name="errorRecordsQuantity">Количество записей, при добавлении которых возникли ошибки</param>
        public delegate void ErrorsEventHandler (uint errorRecordsQuantity);
        /// <summary>
        /// Событие, вызываемое при возникновении ошибок добавления
        /// </summary>
        public event ErrorsEventHandler OnError;

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка подключения к БД</param>
        /// <param name="insertMaker">Объект, который будет создавать запросы</param>
        /// <param name="transactionSize">Количество операторов добавления в транзакции</param>
        protected AGroupRecordsWriter (string connectionString, AGroupInsertMaker insertMaker, uint transactionSize)
        {
            ConnectionString = connectionString;
            TransactionSize = transactionSize;

            InsertMaker = insertMaker;
        }

        /// <summary>
        /// Метод, выполняющий добавление записей в БД
        /// </summary>
        /// <param name="recordsToStore">Коллекция записей для добавления</param>
        /// <param name="startIndex">Индекс записи, с которой нужно начать добавление</param>
        /// <param name="quantity">Количество записей, которые нужно добавить</param>
        /// <returns>Общее количество добавленных записей</returns>
        public uint storeEvents (IList<IGroupInsertableRecord> recordsToStore, uint startIndex, uint quantity)
        {
            uint insertResultCounter = 0;

            // Подключаемся к БД, если ещё не подключились
            if (!DbInited)
                dbInit();

            // Назначаем коллекцию построителю запросов
            InsertMaker.setCollection( recordsToStore, startIndex, quantity );

            uint insertsCounter = 0;
            uint storedCounter = 0;

            uint queryRowsCounter;

            // Берём у построителя очередной запрос
            string query = InsertMaker.nextQuery(out queryRowsCounter);
            // И пока он не вернёт нам null
            while (query != null)
            {
                // Выполняем запрос к БД, получаем количество добавленных записей
                int queryResult = DbContext.Database.ExecuteSqlRaw( query, InsertMaker.FieldValues );
                //int queryResult = (int)queryRowsCounter;

                if (queryResult > 0) { 
                    insertResultCounter += (uint)queryResult;
                    storedCounter += (uint)queryResult;
                }
                else
                {
                    OnError?.Invoke( queryRowsCounter );
                }

                // Считаем количество выполненных запросов, если достигнут размер транзакции - закрываем её
                if (++insertsCounter == TransactionSize)
                {
                    // Закрываем транзакцию
                    DbContext.SaveChanges();
                    // Сообщаем о том, что сколько-то записей добавлено
                    OnStored?.Invoke( storedCounter );

                    // Чистим счётчики записей и добавлений
                    storedCounter = 0;
                    insertsCounter = 0;
                }
                // Получаем следующий запрос
                query = InsertMaker.nextQuery( out queryRowsCounter );
            }

            // Если счетчик добавлений не обнулён, значит последняя транзакция была короткой и её тоже надо закрыть
            if (insertsCounter > 0)
                DbContext.SaveChanges();

            // Сообщаем о том, что сколько-то записей добавлено
            OnStored?.Invoke( storedCounter );

            // Возвращаем общее количество добавленных записей
            return insertResultCounter;
        }

        /// <summary>
        /// Подключение к БД
        /// </summary>
        void dbInit ()
        {
            DbContext = new MonitoringDb( ConnectionString );

            // Устанавливаем признак того, что подключение выполнено
            DbInited = true;
        }

        #region Поддержка интерфейса IDisposable, освобождение неуправляемых ресурсов

        private bool disposedValue; // Для определения излишних вызовов, чтобы выполнять Dispose только один раз

        /// <summary>
        /// Метод, выполняющий освобождение неуправляемых ресурсов
        /// </summary>
        /// <param name="disposing">Признак того, что вызов метода выполнен не из финализатора</param>
        protected virtual void Dispose (bool disposing)
        {
            // Если Dispose ещё не вызывался
            if (!disposedValue)
            {
                // Если вызов выполнен не из финализатора
                if (disposing)
                {
                    // Если выполнено подключение к БД
                    if (DbInited)
                    {
                        // Закрываем подключение
                        DbContext.Dispose();
                    }
                }

                disposedValue = true;
            }
        }
        public void Dispose ()
        {
            Dispose( true );
        }
        #endregion
    }
}
