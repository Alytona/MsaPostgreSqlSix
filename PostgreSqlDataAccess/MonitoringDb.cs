using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Data;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Модель БД мониторинга
    /// </summary>
    public class MonitoringDb : DbContext
    {
        public DbSet<Project> Projects
        {
            get; set;
        }
        public DbSet<Parameter> Parameters
        {
            get; set;
        }
        public DbSet<TableName> TableNames
        {
            get; set;
        }

        public int ParameterEventsCount
        {
            get
            {
                var connection = this.Database.GetDbConnection();
                if (connection.State != ConnectionState.Open)
                    connection.Open();

                string query =
                    "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') AND table_name similar to 'var_\\d*'";
            
                var tableNames 
                    = TableNames.FromSqlRaw( query ).Select( i =>  i.Value ).ToList();

                int count = 0;
                foreach (string tableName in tableNames)
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"select max(event_id) - min(event_id) + 1 from {tableName};";
                        command.CommandType = CommandType.Text;

                        int? quantity = (int?)command.ExecuteScalar();
                        if (quantity.HasValue)
                            count += quantity.Value;
                    }
                }
                return count;
            }
        }

        void LogToConsole (string message)
        {
            Console.WriteLine( message );
        }

        private readonly string _connectionString;
        
        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        public MonitoringDb (string connectionString, uint timeout = 0)
        {
            _connectionString = connectionString;
            
            // this.Database.Log += LogToConsole;
            // ChangeTracker
                
            // Выключаем автоматический запуск DetectChanges()
            ChangeTracker.AutoDetectChangesEnabled = false;
//            Configuration.AutoDetectChangesEnabled = false;
            // Выключаем автоматическую валидацию при вызове SaveChanges()
            // ConfigureOptions..ValidateOnSaveEnabled = false;
            // Выключаем создание прокси-экземпляров сущностей
            // ChangeTracker.ProxyCreationEnabled = false;

            if (timeout != 0)
                Database.SetCommandTimeout( (int)timeout );
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql( _connectionString );
        }        
        /// <summary>
        /// Обработчик события создания модели БД
        /// </summary>
        /// <param name="modelBuilder"></param>
        protected override void OnModelCreating (ModelBuilder modelBuilder)
        {
            // Устанавливаем имя схемы
            modelBuilder.HasDefaultSchema( "public" );

            // modelBuilder.Entity<ParameterEvent>().ToTable( "ParameterEvents", "edition_2" );

            // Задаём ключевые поля
            // modelBuilder.Entity<ParameterEvent>().HasKey( b => b.EventId );

            modelBuilder.Entity<Project>().Property( b => b.Id );
            modelBuilder.Entity<Parameter>().Property( b => b.Id );

            base.OnModelCreating( modelBuilder );
        }
    }
}
