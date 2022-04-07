using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Sphere;
    
namespace PostgreSqlDataAccess
{
    class WriterSlot
    {
        public readonly ParameterValues Collection;
        public readonly uint StartIndex;
        public readonly uint Quantity;

        public WriterSlot (ParameterValues collection, uint startIndex, uint quantity)
        {
            Collection = collection;
            StartIndex = startIndex;
            Quantity = quantity;
        }
    }

    /// <summary>
    /// Реализация класса группового добавления записей для таблицы ParameterEvents
    /// </summary>
    public class EventsWriteAdapter : GroupRecordsWriteAdapter
    {
        readonly ParameterSectionsController SectionsController;

        /// <summary>
        /// Накопительный буфер объектов, ожидающих записи в БД
        /// </summary>
        readonly EventsPrepareBuffer<SphereDataTypesScalarSW_t> PrepareBuffer = new();

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        /// <param name="writersQuantity">Количество потоков добавления записей</param>
        /// <param name="insertSize">Максимальное количество записей, добавляемых одним оператором insert</param>
        /// <param name="transactionSize">Максимальное количество операторов insert в одной транзакции</param>
        public EventsWriteAdapter (string connectionString, uint writersQuantity, uint insertSize, uint transactionSize) : base( connectionString, writersQuantity, insertSize, transactionSize )
        {
            SectionsController = new ParameterSectionsController( connectionString );
        }

        /// <summary>
        /// Метод создания потока добавления записей
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        /// <param name="insertSize">Максимальное количество записей, добавляемых одним оператором insert</param>
        /// <param name="transactionSize">Максимальное количество операторов insert в одной транзакции</param>
        /// <returns>Объект, инкапсулирующий поток добавления записей</returns>
        protected override AGroupRecordsWriter createWriter (string connectionString, uint insertSize, uint transactionSize)
        {
            return new EventsGroupRecordsWriter( connectionString, insertSize, transactionSize );
        }

        /// <summary>
        /// Логика итерации основного потока
        /// </summary>
        protected override void storingIteration ()
        {
            // Переключаем буферы - в накопительном буфере создаём новую коллекцию, а для того, что накопилось вызываем сохранение
            List<SphereDataTypesScalarSW_t> eventsToStore = PrepareBuffer.replaceBufferIfNotEmpty();
            uint totalEventsQuantity = (uint)(eventsToStore?.Count ?? 0);
            if (totalEventsQuantity == 0)
            {
                Storing = false;

                // Если буфер был пустым, надо выполнить задержку
                Task.Delay( 50 ).Wait();
            }
            else 
            {
                Storing = true;

                // Инициализируем количество несохраненных записей в буфере сохранения
                BufferRemainderCounter.Value = totalEventsQuantity; 
                List<Exception> errors = new List<Exception>();

                SectionsController.fillParameterSections( eventsToStore );
                SectionsController.createSections();

                uint baseQuantityPerWriter = (totalEventsQuantity - 1) / WritersQuantity;
                uint quantityRemainder = (totalEventsQuantity - 1) - baseQuantityPerWriter * WritersQuantity;

                // Заполнение слотов для каждого потока записи

                List<WriterSlot>[] WritersSlots = new List<WriterSlot>[WritersQuantity];
                for (int i = 0; i < WritersQuantity; i++)
                    WritersSlots[i] = new List<WriterSlot>();

                uint slottedSize = 0;
                uint currentWriter = 0;
                uint quantityPerWriter = baseQuantityPerWriter;
                if (0 <= quantityRemainder)
                    quantityPerWriter++;

                Console.WriteLine( $"collection size: {SectionsController.FilledParametersSet.Values.First().Events.Count}" );

                foreach (ParameterValues eventsCollection in SectionsController.FilledParametersSet.Values)
                {
                    uint startIndex = 0;
                    while (startIndex < eventsCollection.Events.Count)
                    {
                        // Переключение на следующий слот
                        if (slottedSize == quantityPerWriter)
                        {
                            slottedSize = 0;
                            currentWriter++;
                            quantityPerWriter = baseQuantityPerWriter;
                            if (0 <= quantityRemainder)
                                quantityPerWriter++;
                        }

                        uint slottedPortionSize = 0;
                        if (slottedSize + eventsCollection.Events.Count - startIndex <= quantityPerWriter) //  + 100)
                        {
                            // Добавляем остаток коллекции
                            slottedPortionSize = (uint)eventsCollection.Events.Count - startIndex;
                        }
                        else
                        {
                            // Если остаток коллекции не влезает
                            // Добавляем  кусок коллекции, сколько влезет
                            slottedPortionSize = quantityPerWriter - slottedSize;
                        }
                        if (slottedPortionSize > quantityPerWriter)
                            slottedPortionSize = quantityPerWriter;

                        WritersSlots[currentWriter].Add( new WriterSlot( eventsCollection, startIndex, slottedPortionSize ) );
                        startIndex += slottedPortionSize;
                        slottedSize += slottedPortionSize;
                    }
                }

                // Выполняем сохранение в синхронном режиме
                uint insertedCount = storeEventsTask( WritersSlots, errors );

                // Сообщаем о результатах сохранения
                invokeOnStored( insertedCount, errors );

                foreach (ParameterValues eventsCollection in SectionsController.FilledParametersSet.Values)
                {
                    eventsCollection.clearEvents();
                }

                // Если по каким-то причинам что-то осталось в буфере сохранения, сообщаем об этом
                if (BufferRemainderCounter.Value != 0)
                    Console.WriteLine( "BufferRemainderCounter.Remainder: " + BufferRemainderCounter.Value );
            }
        }

        /// <summary>
        /// Вычисляет количество объектов, ожидающих записи в БД
        /// </summary>
        /// <returns>Количество объектов, ожидающих записи в БД</returns>
        public uint GetQueueLength ()
        {
            // Считается как количество записей в накопительном буфере и в сохраняемом буфере, минус количество ошибок записи
            return PrepareBuffer.Length + BufferRemainderCounter.Value - ErrorsCounter.Value;
        }

        public uint PreparedLen => PrepareBuffer.Length;
        public uint StoringQueueLen => BufferRemainderCounter.Value;
        public uint ErrorsQuantity => ErrorsCounter.Value;

        /// <summary>
        /// Добавляет записи в накопительный буфер
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        public void StoreEvents (List<SphereDataTypesScalarSW_t> eventsToStore)
        {
            PrepareBuffer.addEvents( eventsToStore );
            Storing = true;
        }

        /// <summary>
        /// Добавляет записи в накопительный буфер
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        public void StoreEvent (SphereDataTypesScalarSW_t eventToStore)
        {
            PrepareBuffer.addEvent( eventToStore );
            Storing = true;
        }
            
        /// <summary>
        /// Запись буфера сохранения в БД
        /// </summary>
        /// <param name="eventsToStore">Буфер сохранения</param>
        /// <param name="errors">Коллекция ошибок, возникших при сохранении</param>
        /// <returns>Количество записей, добавленных в БД</returns>
        private uint storeEventsTask (List<WriterSlot>[] WritersSlots, List<Exception> errors)
        {
            // Количество добавленных записей
            uint insertedCount = 0;

            // Коллекция заданий
            List<Task<uint>> tasks = new List<Task<uint>>();
            try
            {
                for (int i = 0; i < WritersQuantity; i++)
                {
                    // Создаём задание для писателя
                    tasks.Add( runWriter( WritersSlots[i], (EventsGroupRecordsWriter)Writers[i] ) );
                }

                // Ожидаем завершения заданий и собираем количество сохраненных записей
                foreach (Task<uint> task in tasks)
                {
                    task.Wait();
                    insertedCount += task.Result;
                }
            }
            catch (Exception error)
            {
                // Если произошла ошибка, добавляем её в коллекцию ошибок
                errors.Add( error );

                // Если ошибка произошла в каком-то задании, тоже добавляем её в коллекцию ошибок
                foreach (Task<uint> task in tasks)
                {
                    if (task.Exception != null)
                        errors.Add( task.Exception );
                }
            }
            return insertedCount;
        }
        /// <summary>
        /// Запись буфера сохранения в БД
        /// </summary>
        /// <param name="eventsToStore">Буфер сохранения</param>
        /// <param name="errors">Коллекция ошибок, возникших при сохранении</param>
        /// <returns>Количество записей, добавленных в БД</returns>
        private uint storeEventsTask (List<ParameterValues> eventsCollections, uint writerIndex, List<Exception> errors)
        {

            // Количество добавленных записей
            uint insertedCount = 0;

            foreach (ParameterValues eventsCollection in eventsCollections) 
            {
                uint totalQuantity = (uint)eventsCollection.Events.Count;
                Task<uint> task = null;
                try
                {
                    (Writers[writerIndex] as EventsGroupRecordsWriter).setParameterId( eventsCollection.ParameterSection.ParameterId );

                    // Создаём задание для писателя
                    task = runWriter( eventsCollection.Events, Writers[writerIndex], 0, totalQuantity );

                    // Ожидаем завершения заданий и собираем количество сохраненных записей
                    task.Wait();
                    insertedCount += task.Result;
                }
                catch (Exception error)
                {
                    // Если произошла ошибка, добавляем её в коллекцию ошибок
                    errors.Add( error );

                    // Если ошибка произошла в каком-то задании, тоже добавляем её в коллекцию ошибок
                    if (task != null && task.Exception != null)
                        errors.Add( task.Exception );
                }
            }

            return insertedCount;
        }

        /// <summary>
        /// Создание задания для писателя (объекта, выполняющего сохранение записей в БД)
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        /// <param name="writer">Ссылка на писателя</param>
        /// <param name="startIndex">Индекс записи, с которой нужно начать добавление</param>
        /// <param name="quantity">Количество записей, которые нужно добавить</param>
        /// <returns>Созданное задание</returns>
        Task<uint> runWriter (List<SphereDataTypesScalarSW_t> eventsToStore, AGroupRecordsWriter writer, uint startIndex, uint quantity)
        {
            // Приведение типа коллекции
            IList<IGroupInsertableRecord> eventsToStoreA = eventsToStore.Cast<IGroupInsertableRecord>().ToList();

            Task<uint> task = Task.Run( () => {
                try
                {
                    // Устанавливаем приоритет потока ниже обычного
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;

                    // Вызываем метод сохранения писателя
                    return writer.storeEvents( eventsToStoreA, startIndex, quantity );
                }
                finally
                {
                    // Восстанавливаем приоритет потока
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
                }
            } );
            return task;
        }

        /// <summary>
        /// Создание задания для писателя (объекта, выполняющего сохранение записей в БД)
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        /// <param name="writer">Ссылка на писателя</param>
        /// <param name="startIndex">Индекс записи, с которой нужно начать добавление</param>
        /// <param name="quantity">Количество записей, которые нужно добавить</param>
        /// <returns>Созданное задание</returns>
        Task<uint> runWriter (List<WriterSlot> WritersSlots, EventsGroupRecordsWriter writer)
        {
            Task<uint> task = Task.Run( () => {
                try
                {
                    // Устанавливаем приоритет потока ниже обычного
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;

                    uint insertedCount = 0;
                    foreach (WriterSlot slot in WritersSlots) {
                        // Приведение типа коллекции
                        IList<IGroupInsertableRecord> eventsToStoreA = slot.Collection.Events.Cast<IGroupInsertableRecord>().ToList();

                        writer.setParameterId( slot.Collection.ParameterSection.ParameterId );

                        // Вызываем метод сохранения писателя
                        insertedCount += writer.storeEvents( eventsToStoreA, slot.StartIndex, slot.Quantity );
                    }
                    return insertedCount;
                }
                finally
                {
                    // Восстанавливаем приоритет потока
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
                }
            } );
            return task;
        }
    }
}
