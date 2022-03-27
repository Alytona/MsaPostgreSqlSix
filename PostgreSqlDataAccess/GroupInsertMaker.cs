using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Класс, создающий серию групповых операторов INSERT для заданной коллекции записей
    /// </summary>
    public abstract class AGroupInsertMaker
    {
        /// <summary>
        /// Количество полей записи
        /// </summary>
        readonly uint ColumnsQuantity;
        /// <summary>
        /// Начальная часть оператора (со списком полей)
        /// </summary>
        // readonly string InsertQuery;
        /// <summary>
        /// Количество записей в одном операторе
        /// </summary>
        public readonly uint InsertSize;

        /// <summary>
        /// Часть оператора с его параметрами
        /// </summary>
        readonly string[] ValuesParts;
        /// <summary>
        /// Текущие значения параметров оператора добавления
        /// </summary>
        public readonly object[] FieldValues;

        /// <summary>
        /// Колекция записей для добавления 
        /// </summary>
        IList<IGroupInsertableRecord> RecordsToStore;
        /// <summary>
        /// Индекс в коллекции, с которого нужно начать добавление
        /// </summary>
        uint StartIndex;
        /// <summary>
        /// Индекс записи в коллекции, добавление которой будет выполнено следующим
        /// </summary>
        uint RowIndex;
        /// <summary>
        /// Общее количество записей, которое должно быть добавлено
        /// </summary>
        uint RecordsQuantity;

        /// <summary>
        /// Конструктор, выполняет инициализацию и создаёт массивы нужного размера
        /// </summary>
        /// <param name="columnsQuantity">Количество полей записи</param>
        /// <param name="insertQuery">Начальная часть оператора (со списком полей)</param>
        /// <param name="insertSize">Количество записей в одном операторе</param>
        //protected AGroupInsertMaker (uint columnsQuantity, string insertQuery, uint insertSize)
        protected AGroupInsertMaker (uint columnsQuantity, uint insertSize)
        {
            // InsertQuery = insertQuery;
            ColumnsQuantity = columnsQuantity;
            InsertSize = insertSize;

            ValuesParts = new string[InsertSize];
            string[] formatParts = new string[ColumnsQuantity];
            for (int i = 0; i < InsertSize; i++)
            {
                object[] values = new object[ColumnsQuantity];
                for (int j = 0; j < ColumnsQuantity; j++)
                {
                    uint cellIndex = (uint)(i * ColumnsQuantity + j);
                    values[j] = cellIndex;
                    formatParts[j] = "@p" + cellIndex;
                }
                ValuesParts[i] = "(" + string.Join( ", ", formatParts ) + ")";
            }
            FieldValues = new object[InsertSize * ColumnsQuantity];
        }

        /// <summary>
        /// Метод, выполняющий назначение коллекции записей для добавления
        /// </summary>
        /// <param name="recordsToStore">Колекция записей для добавления</param>
        /// <param name="startIndex">Индекс в коллекции, с которого нужно начать добавление</param>
        /// <param name="quantity">Общее количество записей, которое должно быть добавлено</param>
        public void setCollection (IList<IGroupInsertableRecord> recordsToStore, uint startIndex, uint quantity)
        {
            // Контроль типа коллекции
            // Закомментировал пока, так как не знаю, нужно ли это
            //checkCollectionType( eventsToStore );

            RecordsToStore = recordsToStore;
            StartIndex = startIndex;
            RecordsQuantity = quantity;

            // Также инициализируется индекс записи в коллекции, добавление которой будет выполнено следующим
            RowIndex = StartIndex;
        }

        /// <summary>
        /// Метод, выполняющий создание очередного запроса
        /// Сначала следует выполнить setCollection, а затем выполнять nextQuery до тех пор, пока он не вернёт null
        /// </summary>
        /// <returns>Запрос к БД в виде строки</returns>
        public string nextQuery (out uint insertRows)
        {
            insertRows = 0;

            // Если нужное количество записей уже добавлено, возвращаем пустой оператор
            if (RowIndex >= StartIndex + RecordsQuantity)
                return null;

            // Здесь будем собирать запрос
            StringBuilder queryBuilder = makeQueryBuilder(); //  new StringBuilder( InsertQuery );

            uint insertRowIndex = 0;

            // Индекс в FieldValues
            uint valuesIndex = 0;

            while (RowIndex < StartIndex + RecordsQuantity)
            {
                queryBuilder.Append( ValuesParts[insertRowIndex] );

                // Заполняем массив FieldValues значениями полей текущей записи
                FillValuesForRecord( RecordsToStore[(int)RowIndex], FieldValues, valuesIndex );
                valuesIndex += ColumnsQuantity;

                insertRowIndex++;
                RowIndex++;

                // Добавляем разделители
                if (insertRowIndex == InsertSize || RowIndex == StartIndex + RecordsQuantity)
                {
                    // В конце запроса - точку с запятой
                    queryBuilder.Append( ";" );
                    break;
                }
                else
                {
                    // После каждой записи - запятую
                    queryBuilder.Append( ", " );
                }
            }
            insertRows = insertRowIndex;

            // Отдаём готовый запрос
            return queryBuilder.ToString();
        }

        protected abstract void FillValuesForRecord (IGroupInsertableRecord record, object[] fieldValues, uint valuesIndex );

        protected abstract StringBuilder makeQueryBuilder ();

        /// <summary>
        /// Метод, выполняющий проверку типа коллекции
        /// </summary>
        /// <param name="eventsToStore"></param>
        protected abstract void checkCollectionType (IList<IGroupInsertableRecord> eventsToStore);
    }
}
