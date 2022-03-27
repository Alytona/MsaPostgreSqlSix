using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Класс, выполняющий отслеживание текущей длины очереди на запись
    /// </summary>
    public sealed class WritingQueueLengthLogger : IDisposable
    {
        /// <summary>
        /// Ссылка на объект, очередь которого будет отслеживаться
        /// </summary>
        readonly EventsWriteAdapter EventsWriteAdapterInstance;

        /// <summary>
        /// Возможные состояния потока
        /// </summary>
        enum ThreadStates
        {
            Working,        // Работаем
            Terminating,    // Пора заканчивать
            Terminated      // Закончили
        }
        /// <summary>
        /// Текущее состояние потока
        /// </summary>
        ThreadStates ThreadState;
        /// <summary>
        /// Блокировка для исключения конфликтов чтения-записи состояния потока из разных потоков
        /// </summary>
        readonly object ThreadStateLock = new object();

        /// <summary>
        /// Рабочий поток, выполняющий отслеживание длины очереди
        /// </summary>
        readonly Thread LoggerThread;

        /// <summary>
        /// Обработчик события, которое вызывается при получении очередного значения длины очереди
        /// </summary>
        /// <param name="queueLen"></param>
        public delegate void LogQueueLenEventHandler (uint prepared, uint storing, uint errors);
        /// <summary>
        /// Событие, которое вызывается при получении очередного значения длины очереди
        /// </summary>
        public event LogQueueLenEventHandler OnLogged;

        /// <summary>
        /// Конструктор, выполняет запуск потока отслеживания длины очереди для переданного EventsWriteAdapter
        /// </summary>
        /// <param name="eventsWriteAdapter">Ссылка на объект, очередь которого будет отслеживаться</param>
        public WritingQueueLengthLogger (EventsWriteAdapter eventsWriteAdapter)
        {
            EventsWriteAdapterInstance = eventsWriteAdapter;

            ThreadState = ThreadStates.Working;
            LoggerThread = new Thread( new ThreadStart( logging ) );
            LoggerThread.Start();
        }

        /// <summary>
        /// Метод потока журналирования
        /// </summary>
        void logging ()
        {
            ThreadStates threadState;
            do
            {
                // Вызываем событие с текущей длиной очереди
                OnLogged?.Invoke( EventsWriteAdapterInstance.PreparedLen, EventsWriteAdapterInstance.StoringQueueLen, EventsWriteAdapterInstance.ErrorsQuantity );
                Thread.Sleep( 300 );

                // Осторожно читаем состояние потока, чтобы проверить, не пора ли заканчивать
                lock (ThreadStateLock)
                {
                    threadState = ThreadState;
                }
            }
            while (threadState == ThreadStates.Working);

            // Состояние потока переключаем на "Закончили"
            lock (ThreadStateLock)
            {
                ThreadState = ThreadStates.Terminated;
            }
        }

        #region Поддержка интерфейса IDisposable, освобождение неуправляемых ресурсов

        private bool disposedValue = false; // Для определения излишних вызовов, чтобы выполнять Dispose только один раз

        /// <summary>
        /// Метод, выполняющий освобождение неуправляемых ресурсов
        /// </summary>
        /// <param name="disposing">Признак того, что вызов метода выполнен не из финализатора</param>
        void Dispose (bool disposing)
        {
            // Если Dispose ещё не вызывался
            if (!disposedValue)
            {
                // Если вызов выполнен не из финализатора
                if (disposing)
                {
                    // Переключаем состояние потока на "Заканчиваем" 
                    lock (ThreadStateLock)
                    {
                        ThreadState = ThreadStates.Terminating;
                    }
                    // Ждём секунду, что поток завершится сам
                    if (!LoggerThread.Join( 1000 ))
                    {
                        // Если не завершился - останавливаем принудительно
                        LoggerThread.Abort();
                    }
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
