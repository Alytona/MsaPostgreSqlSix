using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace PostgreSqlDataAccess
{
    [Table("information_schema.tables")]
    public class TableName
    {
        [Key]
        [Column( "table_name" ), Editable(false)]
        public string Value
        {
            get; set;
        }
    }
}