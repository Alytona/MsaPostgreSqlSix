using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore;

using Sphere;

namespace PostgreSqlDataAccess
{

    class ParameterSection
    {
        public readonly int ParameterId;

        readonly SortedSet<int> Months;
        readonly SortedSet<int> NewMonths;

        public bool IsNewParameter
        {
            get; private set;
        } = true;

        public ParameterSection (int _parameterId)
        {
            ParameterId = _parameterId;

            Months = new SortedSet<int>();
            NewMonths = new SortedSet<int>();
        }

        public int addMonth (DateTime eventDataTime)
        {
            int month = ((eventDataTime.Year - 2000) << 4) + eventDataTime.Month;
            if (!Months.Contains( month ))
            {
                Months.Add( month );
                NewMonths.Add( month );
                return month;
            }
            return 0;
        }

        public int[] getNewMonths ()
        {
            return NewMonths.ToArray();
        }

        public void clearNewMonths ()
        {
            IsNewParameter = false;
            NewMonths.Clear();
        }
    }
    class ParameterValues
    {
        public readonly ParameterSection ParameterSection;

        public readonly List<SphereDataTypesScalarSW_t> Events;

        public ParameterValues (ParameterSection parSection)
        {
            ParameterSection = parSection;

            Events = new List<SphereDataTypesScalarSW_t>();
        }

        public int addEvent (SphereDataTypesScalarSW_t eventData)
        {
            Events.Add( eventData );
            return 0;
        }

        public void clearEvents ()
        {
            Events.Clear();
            ParameterSection.clearNewMonths();
        }
    }

    class ParameterSectionsController : IDisposable
    {
        /// <summary>
        /// Объект модели БД
        /// </summary>
        private readonly MonitoringDb DbContext;

        public readonly Dictionary<int, ParameterValues> FilledParametersSet = new ();

        readonly Dictionary<int, ParameterSection> ParametersSet = new ();

        public ParameterSectionsController (string connectionString)
        {
            DbContext = new MonitoringDb( connectionString );
        }

        public void fillParameterSections (List<SphereDataTypesScalarSW_t> Events)
        {
            FilledParametersSet.Clear();
            foreach (SphereDataTypesScalarSW_t eventData in Events)
            {
                if (!ParametersSet.TryGetValue( eventData.var_id, out ParameterSection parameterSection ))
                {
                    parameterSection = new ParameterSection( eventData.var_id );
                    ParametersSet.Add( eventData.var_id, parameterSection );
                }
                parameterSection.addMonth( eventData.timestamp );

                if (!FilledParametersSet.TryGetValue( eventData.var_id, out ParameterValues parameterData ))
                {
                    parameterData = new ParameterValues( parameterSection );
                    FilledParametersSet.Add( eventData.var_id, parameterData );
                }
                parameterData.addEvent( eventData );
            }
        }

        public void createSections ()
        {
            StringBuilder commandBuilder = new StringBuilder();
            foreach (ParameterValues parameterData in FilledParametersSet.Values)
            {
                string tablename = "var_" + parameterData.ParameterSection.ParameterId;
                if (parameterData.ParameterSection.IsNewParameter)
                {
                    // создать таблицу для нового параметра, предварительно проверив, нет ли такой таблицы
                    commandBuilder.Clear();
                    commandBuilder.AppendLine( "create table if not exists public." + tablename + " (" );
                    commandBuilder.AppendLine( "year_month character( 5 ) COLLATE pg_catalog.\"default\" NOT NULL," );
                    commandBuilder.AppendLine( "event_id SERIAL," );
                    commandBuilder.AppendLine( "node_id integer," );
                    commandBuilder.AppendLine( "event_value double precision," );
                    commandBuilder.AppendLine( "event_time timestamp without time zone," );
                    commandBuilder.AppendLine( "event_counter integer," );
                    commandBuilder.AppendLine( "event_status integer" );
//                    commandBuilder.AppendLine( "constraint " + tablename + "_pkey PRIMARY KEY( year_month, event_id )" );
                    commandBuilder.AppendLine( ") PARTITION BY RANGE( year_month );" );
                    DbContext.Database.ExecuteSqlRaw( commandBuilder.ToString() );
                }

                int[] newMonths = parameterData.ParameterSection.getNewMonths();
                foreach (int newMonth in newMonths)
                {
                    int year = newMonth >> 4;
                    int month = newMonth & 0x0F;
                    string monthString = $"{ year }_{ month:D2}";
                    string nextMonthString = $"{ year }_{ month + 1:D2}";

                    // создать секцию для нового месяца, но сначала проверить, может такая уже есть
                    commandBuilder.Clear();
                    commandBuilder.AppendLine( "create table if not exists " + tablename + "_" + monthString );
                    commandBuilder.AppendLine( "PARTITION OF " + tablename + " FOR VALUES FROM ('" + monthString + "') TO ('" + nextMonthString + "');" );
                    DbContext.Database.ExecuteSqlRaw( commandBuilder.ToString() );
                }
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
                    DbContext.Dispose();
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
