using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace QuickFix
{
    public class SqlLog : ILog, System.IDisposable
    {
        private readonly object messageLogTableLock = new object();
        private readonly object eventLogTableLock = new object();
        private string connectionString;

        private string messageLogTableName;
        private string eventLogTableName;

        private string prefix;

        private DataTable messageLogTable;
        private DataTable eventLogTable;

        public string ConnectionString
        {
            get { return this.connectionString; }
        }

        private static DataTable CreateLogTable()
        {
            DataTable logTable = new DataTable();
            logTable.Columns.Add("timestamp", typeof(DateTime));
            logTable.Columns.Add("message", typeof(string));
            return logTable;
        }

        public SqlLog(string connectionString,SessionID sessionID)
        {
            this.Init(connectionString, Prefix(sessionID));
        }

        private void Init(string connectionString,string prefix)
        {
            this.connectionString = connectionString;

            this.prefix = prefix;
            //TODO still use this prefix somewhere - the file log used it in the filename... Use in table names?
            this.messageLogTableName = "messageslog";
            this.eventLogTableName = "eventslog";

            lock(messageLogTableLock)
                this.messageLogTable = CreateLogTable();
            lock(eventLogTableLock)
                this.eventLogTable = CreateLogTable();
        }

        public static string Prefix(SessionID sessionID)
        {
            System.Text.StringBuilder prefix = new System.Text.StringBuilder(sessionID.BeginString)
                .Append('-').Append(sessionID.SenderCompID);
            if (SessionID.IsSet(sessionID.SenderSubID))
                prefix.Append('_').Append(sessionID.SenderSubID);
            if (SessionID.IsSet(sessionID.SenderLocationID))
                prefix.Append('_').Append(sessionID.SenderLocationID);
            prefix.Append('-').Append(sessionID.TargetCompID);
            if (SessionID.IsSet(sessionID.TargetSubID))
                prefix.Append('_').Append(sessionID.TargetSubID);
            if (SessionID.IsSet(sessionID.TargetLocationID))
                prefix.Append('_').Append(sessionID.TargetLocationID);

            if (sessionID.SessionQualifier.Length != 0)
                prefix.Append('-').Append(sessionID.SessionQualifier);

            return prefix.ToString();
        }

        public void ClearMessageLog()
        {
            lock(messageLogTableLock)
            {
                this.messageLogTable.Clear();
            }
        }

        public void ClearEventLog()
        {
            lock (eventLogTableLock)
            {
                this.eventLogTable.Clear();
            }
        }

        public void Clear()
        {
            this.ClearEventLog();
            this.ClearMessageLog();
        }
        public void OnIncoming(string msg)
        {

            lock (messageLogTableLock)
            {
                DataRow newRow = this.messageLogTable.NewRow();
                newRow["timestamp"] = DateTime.UtcNow;
                newRow["message"] = msg;
                this.messageLogTable.Rows.Add(newRow);
                this.messageLogTable.AcceptChanges();
            }

            if (IsMessageLogBatchReady())
            {
                lock (messageLogTableLock)
                {                                        
                    //SqlConnection dbconn = new SqlConnection(this.connectionString)
                    BulkCopyToDatabase(this.messageLogTable, this.messageLogTableName);
                }
            }
        }


        private virtual bool IsMessageLogBatchReady()
        {
            return true;
        }

        public virtual bool IsEventLogBatchReady()
        {
            return true;
        }

        public void OnOutgoing(string msg)
        {
            lock (messageLogTableLock)
            {
                DataRow newRow = this.messageLogTable.NewRow();
                newRow["timestamp"] = DateTime.UtcNow;
                newRow["message"] = msg;
                this.messageLogTable.Rows.Add(newRow);
                this.messageLogTable.AcceptChanges();
            }

            if (IsMessageLogBatchReady())
            {
                lock (messageLogTableLock)
                {
                    BulkCopyToDatabase(this.messageLogTable,this.messageLogTableName);
                }
            }
        }

        public virtual void BulkCopyToDatabase(DataTable sourceTable, string targetTableName)
        {
            using (SqlConnection dbconn = new SqlConnection(this.connectionString))
            {
                SqlCommand cmd = new SqlCommand(
                "INSERT INTO " + targetTableName + "(timestamp, message) SELECT timestamp, message FROM @MemoryLogTable",
                dbconn);

                cmd.Parameters.Add(
                    new SqlParameter()
                    {
                        ParameterName = "@MemoryLogTable",
                        SqlDbType = SqlDbType.Structured,
                        TypeName = targetTableName,
                        Value = sourceTable,
                    });

                cmd.ExecuteNonQuery();
            }            
            sourceTable.Clear();
        }

        public void OnEvent(string s)
        {
            lock (eventLogTableLock)
            {
                DataRow newRow = this.eventLogTable.NewRow();
                newRow["timestamp"] = DateTime.UtcNow;
                newRow["message"] = s;
                this.eventLogTable.Rows.Add(newRow);
                this.eventLogTable.AcceptChanges();
            }

            if (IsEventLogBatchReady())
            {
                lock (eventLogTableLock)
                {
                    BulkCopyToDatabase(this.eventLogTable, this.eventLogTableName);
                }
            }
        }

        public virtual void Dispose()
        {
            //throw new NotImplementedException();
            //if (this.dbconnection != null)
            //{
            //    //attempt to close this cleanly. .Dispose *probably* does so already, but until
            //    //we research that, close explicitly anyways
            //    if (this.dbconnection.State == System.Data.ConnectionState.Open)
            //        this.dbconnection.Close();

            //    this.dbconnection.Dispose();
            //    this.dbconnection = null;
            //}

        }
    }
}
