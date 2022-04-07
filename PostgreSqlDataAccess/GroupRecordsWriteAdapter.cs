using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Потокобезопасный счетчик остатка для буфера записи
    /// </summary>
    public class ThreadSafeCounter
    {
        /// <summary>
        /// Блокировка для потокобезопасного доступа к счетчику
        /// </summary>
        readonly object ValueLock = new object();

        /// <summary>
        /// Значение счётчика
        /// </summary>
        public uint _value;

        /// <summary>
        /// Свойство, обеспечивающее потокобезопасный доступ к счетчику
        /// </summary>
        public uint Value
        {
            get {
                lock (ValueLock) { return _value; }
            }
            set {
                lock (ValueLock) { _value = value; }
            }
        }

        /// <summary>
        /// Вычесть заданное количество из счётчика
        /// </summary>
        /// <param name="quantity">Количество, которое следует вычесть из счётчика</param>
        public void subtract (uint quantity)
        {
            lock (ValueLock) { 
                _value -= quantity; 
            }
        }
        /// <summary>
        /// Добавить заданное количество к счётчику
        /// </summary>
        /// <param name="quantity">Количество, которое следует добавить к счётчику</param>
        public void add (uint quantity)
        {
            lock (ValueLock)
            {
                _value += quantity;
            }
        }
    }

    /// <summary>
    /// Потокобезопасный буфер, в котором события накапливаются перед записью
    /// </summary>
    class EventsPrepareBuffer<TEventType>
    {
        /// <summary>
        /// Коллекция, в которой хранятся записи буфера
        /// </summary>
        private List<TEventType> _events = new();
        /// <summary>
        /// Блокировка для потокобезопасного доступа к буферу
        /// </summary>
        readonly object _eventsLock = new();

        /// <summary>
        /// Количество записей в буфере
        /// </summary>
        public uint Length
        {
            get
            {
                lock (_eventsLock)
                    return (uint)_events.Count;
            }
        }

        /// <summary>
        /// Метод создаёт новый буфер событий и возвращает ссылку на старый
        /// Используется для переключения процессов записи в БД на накопленный буфер
        /// Если старый буфер пуст, то просто возвращается null
        /// </summary>
        /// <returns>Ссылка на старый буфер</returns>
        public List<TEventType> replaceBufferIfNotEmpty ()
        {
            lock (_eventsLock)
            {
                if (_events.Count == 0)
                    return null;

                List<TEventType> eventsToStore = _events;
                _events = new List<TEventType>();
                return eventsToStore;
            }
        }
        /// <summary>
        /// Добавить события в буфер
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей, которые нужно добавить</param>
        public void addEvents (List<TEventType> eventsToStore)
        {
            lock (_eventsLock)
            {
                _events.AddRange( eventsToStore );
            }
        }

        /// <summary>
        /// Добавить событие в буфер
        /// </summary>
        /// <param name="eventToStore">Запись, которую нужно добавить</param>
        public void addEvent (TEventType eventToStore)
        {
            lock (_eventsLock)
            {
                _events.Add( eventToStore );
            }
        }
    }

    public abstract class GroupRecordsWriteAdapter : IDisposable
    {
        /// <summary>
        /// Массив потоков добавления записей в БД
        /// </summary>
        protected readonly AGroupRecordsWriter[] Writers;
        /// <summary>
        /// Количество потоков добавления записей в БД
        /// </summary>
        protected readonly uint WritersQuantity;

        /// <summary>
        /// Обработчик окончания добавления внутреннего буфера в БД
        /// </summary>
        /// <param name="storedCount">Количество добавленных в БД записей</param>
        /// <param name="errors">Список ошибок, возникших при добавлении</param>
        public delegate void StoredEventHandler (uint storedCount, List<Exception> errors);

        /// <summary>
        /// Событие, которое вызывается после записи всего внутреннего буфера для передачи количества записей, 
        /// добавленных в БД и списка ошибок.
        /// </summary>
        public event StoredEventHandler OnStored;

        protected void invokeOnStored (uint storedCount, List<Exception> errors)
        {
            OnStored?.Invoke( storedCount, errors );
        }

        /// <summary>
        /// Признак того, что выполняется запись буфера в БД.
        /// Используется при ожидании завершения записи перед освобождением ресурсов
        /// </summary>
        bool _storing = true;
        readonly object StoringLock = new object();
        protected bool Storing
        {
            get {
                lock (StoringLock) { return _storing; }
            }
            set {
                lock (StoringLock) { _storing = value; }
            }
        }

        /// <summary>
        /// Счетчик ошибок при записи в БД
        /// </summary>
        protected readonly ThreadSafeCounter ErrorsCounter = new ();
        /// <summary>
        /// Счетчик объектов, ожидающих записи в БД в буфере сохранения
        /// </summary>
        protected readonly ThreadSafeCounter BufferRemainderCounter = new ();

        /// <summary>
        /// Задание основного потока
        /// </summary>
        private readonly Task _storingTask;

        /// <summary>
        /// Объект для остановки основного потока
        /// </summary>
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        
        /// <summary>
        /// Логика итерации основного потока
        /// </summary>
        protected abstract void storingIteration ();

        /// <summary>
        /// Констуктор
        /// </summary>
        /// <param name="connectionString">Строка подключения к БД</param>
        /// <param name="writersQuantity">Количество потоков добавления записей</param>
        /// <param name="insertSize">Количество записей, добавляемых одним оператором INSERT</param>
        /// <param name="transactionSize">Количество операций в транзакции</param>
        protected GroupRecordsWriteAdapter (string connectionString, uint writersQuantity, uint insertSize, uint transactionSize)
        {
            // Создаём объекты, которые будут выполнять запись объектов в БД
            WritersQuantity = writersQuantity;
            Writers = new AGroupRecordsWriter[WritersQuantity];
            initWriters( connectionString, insertSize, transactionSize );

            // Создаём и запускаем поток накопления и сохранения
            _storingTask = Task.Run(() =>
            {
                do
                {
                    // Выполняем логику итерации потока
                    storingIteration();
                }
                while (!_cancellationTokenSource.IsCancellationRequested);
            }
            );
        }
        private void initWriters (string connectionString, uint insertSize, uint transactionSize)
        {
            for (int i = 0; i < WritersQuantity; i++)
            {
                Writers[i] = createWriter( connectionString, insertSize, transactionSize );
                // Когда писатель запишет порцию в БД, счетчик оставшихся записей будет уменьшен на количество записанных
                Writers[i].OnStored += BufferRemainderCounter.subtract;
                // В случае ошибок будет увеличен счетчик ошибок
                Writers[i].OnError += ErrorsCounter.add;
            }
        }

        /// <summary>
        /// Метод создания потока сохранения объектов в БД
        /// </summary>
        /// <param name="connectionString">Строка подключения к БД</param>
        /// <param name="insertSize">Количество записей, добавляемых одним оператором INSERT</param>
        /// <param name="transactionSize">Количество операций в транзакции</param>
        /// <returns></returns>
        protected abstract AGroupRecordsWriter createWriter (string connectionString, uint insertSize, uint transactionSize);

        /// <summary>
        /// Ожидание завершения записи буфера сохранения
        /// </summary>
        public void WaitForStoring ()
        {
            while (Storing) {
                Thread.Sleep( 50 );
            }
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
                    // Ждем окончания записи буфера сохранения
                    WaitForStoring();

                    // Сообщаем основному потоку, что надо заканчиваться
                    _cancellationTokenSource.Cancel();
                        
                    // Останавливаем писателей
                    for (int i = 0; i < WritersQuantity; i++)
                    {
                        Writers[i]?.Dispose();
                    }

                    // Ждём завершения основного потока
                    _storingTask.Wait();
                }
                // Больше не выполнять
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
