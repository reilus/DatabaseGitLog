using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Dapper;
using Quartz;

namespace testingnet
{
    class Program
    {

        static void Main(string[] args)
        {
            //last id = 145
            //Work.ApplyAllSources();
            //Work.Apply(0);

            //Change 145 here into actual lastID
            /*Application.Start(145).GetAwaiter().GetResult();
            Console.ReadKey();*/
            DatabaseConnectionManager.ReadAllLogs(145);
            //DatabaseConnectionManager.ReadLog(145);
        }
    }
}