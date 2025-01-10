using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInserion;

public class User
{
    public string first_name { get; set; }
    public string last_name  { get; set; }
    public string email { get; set; }
    public string address { get; set; }
    public string gender { get; set; }
    public int age { get; set; }
    public string occupation { get; set; }
}
