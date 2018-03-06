using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Dapper;
using LibGit2Sharp;
using Quartz;
using Quartz.Impl;
using Quartz.Xml.JobSchedulingData20;

/*
 * TODO list:
 * 1. quartz
 * 2. windows service
 */

namespace testingnet
{
    public class UnlistedSource
    {
        public string Name { get; set; }
        public string Text { get; set; }
        public string Type { get; set; }
    }

    public class HistoryItem
    {
        public int Id { get; set; }
        public string OsUser { get; set; }
        public string Name { get; set; }
        public string Text { get; set; }
        public string ChangeDate { get; set; }
        public string Type { get; set; }
        public string Scheme { get; set; }
    }

    public class DatabaseConnectionManager
    {
        // Reads one log from database with given id
        public static IEnumerable<HistoryItem> ReadLog(int id)
        {
            using (var connection = new OracleConnection(ConfigurationManager.AppSettings["dbAlmzConnectionString"]))
            {
                connection.Open();
                var query =
                    @"SELECT id,os_user as osUser,name,text,change_date as changeDate,type,scheme FROM SOURCE_HIST WHERE Id = :lastId";
                var result = connection.Query<HistoryItem>(query, new {lastId = id});
                return result;
            }
        }

        // Reads all logs from database with id more than given
        public static IEnumerable<HistoryItem> ReadAllLogs(int id = 0)
        {
            using (var connection = new OracleConnection(ConfigurationManager.AppSettings["dbAlmzConnectionString"]))
            {
                connection.Open();
                var query =
                    @"SELECT id,os_user as osUser,name,text,change_date as changeDate,type,scheme FROM SOURCE_HIST WHERE Id > :lastId ORDER BY id";
                var result = connection.Query<HistoryItem>(query, new { lastId = id });
                return result;
            }
        }

        //Get all source code from almz@database
        public static IEnumerable<UnlistedSource> GetAlmzSourceNames()
        {
            using (var connection = new OracleConnection(ConfigurationManager.AppSettings["dbAlmzConnectionString"]))
            {
                connection.Open();
                const string query =
                    @"select name, type, dbms_metadata.get_ddl(type,name) as text from user_source us WHERE type not like '%BODY' AND type not like 'TYPE' AND type not like 'TRIGGER' AND NOT EXISTS (SELECT 1 FROM almz.source_hist sh WHERE sh.name = us.name AND sh.type = us.type AND sh.scheme = 'almz') GROUP BY name,type";
                var result = connection.Query<UnlistedSource>(query);
                return result;
            }
        }

        //Get all source code from bpmonline_p@database 
        public static IEnumerable<UnlistedSource> GetBpmonline_pSourceNames()
        {
            using (var connection =
                new OracleConnection(ConfigurationManager.AppSettings["dbBpmonline_pConnectionString"]))
            {
                connection.Open();
                const string query =
                    @"select name, type, dbms_metadata.get_ddl(type,name) as text from user_source us WHERE type not like '%BODY' AND type not like 'TYPE' AND type not like 'TRIGGER' AND NOT EXISTS (SELECT 1 FROM almz.source_hist sh WHERE sh.name = us.name AND sh.type = us.type AND sh.scheme = 'bpmonline_p') GROUP BY name,type";
                var result = connection.Query<UnlistedSource>(query);
                return result;
            }
        }

        public static int GetLastId()
        {
            using (var connection = new OracleConnection(ConfigurationManager.AppSettings["dbAlmzConnectionString"]))
            {
                connection.Open();
                const string query =
                    @"select max(ID) from source_hist";
                var result = connection.Query<int>(query);
                return result.First();
            }
        }
    }

    public class GitInterface
    {
        //Repository path
        public string RepoPath { get; set; }

        //Initialize with given repository path
        public GitInterface(string path)
        {
            RepoPath = path;
        }

        //Initialize with default repository path, which should be set in appSettings.config
        public GitInterface()
        {
            RepoPath = ConfigurationManager.AppSettings["repoPath"];
        }

        //Write item into file, stage it and then commit
        public void Commit(HistoryItem item)
        {
            bool isDirectory;
            Directory.CreateDirectory(RepoPath + "\\" + item.Scheme);
            //if item is source code of package then create directory for it and write it there
            if (item.Type.Contains("PACKAGE"))
            {
                isDirectory = true;
                Directory.CreateDirectory(RepoPath + "\\" + item.Scheme + "\\" + item.Name);
                using (StreamWriter writer =
                    File.CreateText(RepoPath + "\\" + item.Scheme + "\\" + item.Name + "\\" + "PACKAGEBODY.sql"))
                {
                    writer.Write(item.Text);
                }
            }
            //else just write it in current directory
            else
            {
                isDirectory = false;
                using (StreamWriter writer = File.CreateText(RepoPath + "\\" + item.Scheme + "\\" + item.Name + ".sql"))
                {
                    writer.Write(item.Text);
                }
            }

            // Stage and commit last log file
            using (var repo = new Repository(RepoPath))
            {
                string path;
                if (isDirectory) path = RepoPath + "\\" + item.Scheme + "\\" + item.Name + "/";
                else path = RepoPath + "\\" + item.Scheme + "\\" + item.Name + ".sql";
                Commands.Stage(repo, path);
                var author = new Signature(item.OsUser, item.OsUser + "@aebit.local",
                    DateTimeOffset.Parse(item.ChangeDate));
                try
                {
                    var commit = repo.Commit("No comments", author, author);
                    Console.WriteLine("File " + item.Name + " commited.");
                }
                catch (
                    LibGit2SharpException e
                )
                {
                    Console.WriteLine("Commit skipped " + item.Name + "\nError message " + e.Message);
                }
            }
        }

        //push all commited files
        public void Push()
        {
            using (var repo = new Repository(RepoPath))
            {
                var options = new PushOptions();
                options.CredentialsProvider = (url, usernameFromUrl, types) =>
                    new UsernamePasswordCredentials()
                    {
                        // login and password are stored in appSettings.config
                        Username = ConfigurationManager.AppSettings["gitLogin"],
                        Password = ConfigurationManager.AppSettings["gitPassword"]
                    };
                repo.Network.Push(repo.Branches["master"], options);
            }
        }

        //Commit and push items
        public void Apply(IEnumerable<HistoryItem> items)
        {
            foreach (var item in items)
            {
                Commit(item);
            }

            Push();
        }

        //Commit and push all source files which are not in the log table
        public void InitSources()
        {
            var almzSources = DatabaseConnectionManager.GetAlmzSourceNames();
            var bpmonline_pSources = DatabaseConnectionManager.GetBpmonline_pSourceNames();
            bool isDirectory;
            // Commit all sources from Almz scheme
            foreach (var source in almzSources)
            {
                // If type eq package or package body, create a folder then add log there
                if (source.Type.Contains("PACKAGE"))
                {
                    isDirectory = true;
                    Directory.CreateDirectory(RepoPath + "\\" + "almz" + "\\" + source.Name);
                    using (StreamWriter writer =
                        File.CreateText(RepoPath + "\\" + "almz" + "\\" + source.Name + "\\" + "PACKAGEBODY.sql"))
                    {
                        writer.Write(source.Text);
                    }
                }
                //else just write it in current directory
                else
                {
                    isDirectory = false;
                    using (StreamWriter writer =
                        File.CreateText(RepoPath + "\\" + "almz" + "\\" + source.Name + ".sql"))
                    {
                        writer.Write(source.Text);
                    }
                }

                // Stage and commit last file
                using (var repo = new Repository(RepoPath))
                {
                    const string user = "gprokopyev";
                    string path;
                    if (isDirectory) path = RepoPath + "\\" + "almz" + "\\" + source.Name + "/";
                    else
                        path = RepoPath + "\\" + "almz" + "\\" + source.Name + ".sql";
                    Commands.Stage(repo, path);
                    var author = new Signature(user, user + "@aebit.local",
                        DateTimeOffset.Now);
                    try
                    {
                        var commit = repo.Commit("No comments", author, author);
                        Console.WriteLine("File " + source.Name + " commited.");
                    }
                    catch (
                        LibGit2SharpException e
                    )
                    {
                        Console.WriteLine("Commit skipped " + source.Name + "\nError message " + e.Message);
                    }
                }
            }

            // Commit all sources from bpmonline_p scheme
            foreach (var source in bpmonline_pSources)
            {
                // If type eq package or package body, create a folder then add log there
                if (source.Type.Contains("PACKAGE"))
                {
                    isDirectory = true;
                    Directory.CreateDirectory(RepoPath + "\\" + "bpmonline_p" + "\\" + source.Name);
                    using (StreamWriter writer =
                        File.CreateText(RepoPath + "\\" + "bpmonline_p" + "\\" + source.Name + "\\" +
                                        "PACKAGEBODY.sql"))
                    {
                        writer.Write(source.Text);
                    }
                }
                //else just write it in current directory
                else
                {
                    isDirectory = false;
                    using (StreamWriter writer =
                        File.CreateText(RepoPath + "\\" + "bpmonline_p" + "\\" + source.Name + ".sql"))
                    {
                        writer.Write(source.Text);
                    }
                }

                // Stage and commit last file
                using (var repo = new Repository(RepoPath))
                {
                    const string user = "gprokopyev";
                    string path;
                    if (isDirectory) path = RepoPath + "\\" + "bpmonline_p" + "\\" + source.Name + "/";
                    else path = RepoPath + "\\" + "bpmonline_p" + "\\" + source.Name + ".sql";
                    Commands.Stage(repo, path);
                    var author = new Signature(user, user + "@aebit.local",
                        DateTimeOffset.Now);
                    try
                    {
                        var commit = repo.Commit("No comments", author, author);
                        Console.WriteLine("File " + source.Name + " commited.");
                    }
                    catch (
                        LibGit2SharpException e
                    )
                    {
                        Console.WriteLine("Commit skipped " + source.Name + "\nError message " + e.Message);
                    }
                }
            }
        }
    }


    public class Work : IJob
    {
        public static void Apply(int lastID)
        {
            var iGit = new GitInterface();
            iGit.Apply(DatabaseConnectionManager.ReadAllLogs(lastID));
        }

        public static void ApplyAllSources()
        {
            var iGit = new GitInterface();
            iGit.InitSources();
            iGit.Push();
        }
        public Task Execute(IJobExecutionContext context)
        {
            JobKey key = context.JobDetail.Key;
            var dataMap = context.MergedJobDataMap;
            var state = (ArrayList)dataMap["stateData"];
            Apply((int)state[state.Count - 1]);
            state.Add(DatabaseConnectionManager.GetLastId());
            var log = "Instance " + key + " done";
            return Console.Out.WriteAsync(log);
        }
    }

    public class Application
    {
        public static async Task Start(int lastID)
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            IScheduler sched = factory.GetScheduler().Result;
            sched.Start().Wait();

            JobKey jobkey = JobKey.Create("myJob");
            IJobDetail Job = JobBuilder.Create<Work>()
                .WithIdentity(jobkey)
                .Build();
            var ScheduleIntervalInHours = 24; // job will run once every day //feel free to change to whatever you want
            Job.JobDataMap["stateData"] = new ArrayList();
            var state = (ArrayList)Job.JobDataMap["stateData"];
            // Initial state of last ID commited
            // change this every time you start this job
            state.Add(lastID);

            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("myTrigger")
                .StartNow()
                .WithSimpleSchedule(x => x
                    .WithIntervalInHours(ScheduleIntervalInHours)
                    .RepeatForever())
                .Build();

            await sched.ScheduleJob(Job, trigger);
        }

    }
}
