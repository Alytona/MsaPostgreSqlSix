using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace PostgreSqlDataAccess
{
    [Table( "variables" )]
    public class Parameter
    {
        [Key]
        [Column( "par_id" )]
        public int Id
        {
            get; set;
        }
        [Column( "par_name" )]
        public string Name
        {
            get; set;
        }
        [Column( "par_comment" )]
        public string Comment
        {
            get; set;
        }
        [Column( "par_units" )]
        public string Units
        {
            get; set;
        }
        [Column( "par_source" )]
        public string Source
        {
            get; set;
        }
    }
}
