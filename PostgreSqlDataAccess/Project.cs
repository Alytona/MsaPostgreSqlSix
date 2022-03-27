using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace PostgreSqlDataAccess
{
    [Table( "projects" )]
    public class Project
    {
        [Key]
        [Column( "project_id" )]
        public int Id
        {
            get; set;
        }
        [Column( "project_description" )]
        public string Description
        {
            get; set;
        }
    }
}
