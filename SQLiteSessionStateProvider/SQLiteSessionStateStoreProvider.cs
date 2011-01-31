using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Configuration;
using System.Configuration;
using System.Collections.Specialized;
using System.Web.SessionState;
using System.Configuration.Provider;
using System.Web;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Diagnostics;
using System.Timers;

namespace Littlefish.SQLiteSessionStateProvider
{
    public class SQLiteSessionStateStoreProvider : SessionStateStoreProviderBase
    {
        private SessionStateSection _config = null;
        private string _connectionString;
        private ConnectionStringSettings _connectionStringSettings;
        private string _eventSource = "SQLiteSessionStateStore";
        private string _eventLog = "Application";
        private string _exceptionMessage = "An exception occurred. Please contact your administrator.";
        private Timer _cleanupTimer;

        /// <summary>
        /// If false, exceptions are thrown to the caller. If true,
        /// exceptions are written to the event log.
        /// </summary>
        public bool WriteExceptionsToEventLog { get; set; }

        /// <summary>
        /// The ApplicationName property is used to differentiate sessions
        /// in the data source by application.
        /// </summary>
        public string ApplicationName { get; private set; }


        /// <summary>
        /// Initialize values from web.config.
        /// </summary>
        public override void Initialize(string name, NameValueCollection config)
        {
            if (config == null)
                throw new ArgumentNullException("config");

            if (name == null || name.Length == 0)
                name = "SQLiteSessionStateStore";

            if (String.IsNullOrEmpty(config["description"]))
            {
                config.Remove("description");
                config.Add("description", "SQLite Session State Store provider");
            }

            // Initialize the abstract base class.
            base.Initialize(name, config);

            // Initialize the ApplicationName property.
            ApplicationName = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath;

            // Get <sessionState> configuration element.
            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            _config = (SessionStateSection)cfg.GetSection("system.web/sessionState");

            // Initialize connection string.
            var databaseFile = config["databaseFile"];
            if (databaseFile == null || string.IsNullOrWhiteSpace(databaseFile))
                throw new ProviderException("Configuration 'databaseFile' must be specified for SqliteSessionStateStoreProvider.");

            //Try and map the database to the location on the server. 
            //This will allow databse files to be specified as ~/Folder
            var currentContext = HttpContext.Current;
            if (currentContext != null)
                databaseFile = currentContext.Server.MapPath(databaseFile);
            
            _connectionString = "Data Source =" + databaseFile;

            var schemagenerator = new SchemaGenerator(databaseFile);
            schemagenerator.Create();

            //Setup cleanup timer to remove old session data
            _cleanupTimer = new Timer(_config.Timeout.Milliseconds);
            _cleanupTimer.Elapsed += (sender,e) => CleanUpExpiredData();

            // Initialize WriteExceptionsToEventLog
            var writeExceptionsToEventLog = config["writeExceptionsToEventLog"];
            if (writeExceptionsToEventLog != null && writeExceptionsToEventLog.Equals("true", StringComparison.OrdinalIgnoreCase))
                WriteExceptionsToEventLog = true;
        }


        // SessionStateStoreProviderBase members
        public override void Dispose() { }

        // SessionStateProviderBase.SetItemExpireCallback
        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        // SessionStateProviderBase.SetAndReleaseItemExclusive
        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            string sessItems = Serialize((SessionStateItemCollection)item.Items);

            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd;
            IDbCommand deleteCmd = null;

            if (newItem)
            {
                // SQLiteCommand to clear an existing expired session if it exists.
                deleteCmd = conn.CreateCommand();
                deleteCmd.CommandText = "DELETE FROM Sessions WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName AND Expires < @Expires";
                deleteCmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                deleteCmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, 255, ApplicationName));
                deleteCmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now));

                // SQLiteCommand to insert the new session item.
                cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO Sessions (SessionId, ApplicationName, Created, Expires, LockDate, LockId, Timeout, Locked, SessionItems, Flags) Values(@SessionId, @ApplicationName, @Created, @Expires, @LockDate, @LockId, @Timeout, @Locked, @SessionItems, @Flags)";
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, 255, ApplicationName));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Created", DbType.DateTime, DateTime.Now));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now.AddMinutes((Double)item.Timeout)));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockDate", DbType.DateTime, DateTime.Now));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, 0));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Timeout", DbType.Int32, item.Timeout));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Locked", DbType.Boolean, false));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionItems", DbType.String, sessItems.Length, sessItems));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Flags", DbType.Int32, 0));
            }
            else
            {
                // SQLiteCommand to update the existing session item.
                cmd = conn.CreateCommand();
                cmd.CommandText = "UPDATE Sessions SET Expires = @Expires, SessionItems = @SessionItems, Locked = @Locked WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName AND LockId = @LockId";
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now.AddMinutes((Double)item.Timeout)));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionItems", DbType.String, sessItems.Length, sessItems));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Locked", DbType.Boolean, false));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, lockId));
            }

            try
            {
                conn.Open();

                if (deleteCmd != null)
                    deleteCmd.ExecuteNonQuery();

                cmd.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "SetAndReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Close();
            }
        }


        /// <summary>
        /// SessionStateProviderBase.GetItem
        /// </summary>
        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(false, context, id, out locked, out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        /// SessionStateProviderBase.GetItemExclusive
        /// </summary>
        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(true, context, id, out locked, out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        /// GetSessionStoreItem is called by both the GetItem and 
        /// GetItemExclusive methods. GetSessionStoreItem retrieves the </summary>
        /// session data from the data source. If the lockRecord parameter<param name="lockRecord"></param>
        /// is true (in the case of GetItemExclusive), then GetSessionStoreItem<param name="context"></param>
        /// locks the record and sets a new LockId and LockDate.<param name="id"></param>
        private SessionStateStoreData GetSessionStoreItem(bool lockRecord, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actionFlags)
        {
            // Initial values for return value and out parameters.
            SessionStateStoreData item = null;
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actionFlags = 0;

            // SQLite database connection.
            IDbConnection conn = new SQLiteConnection(_connectionString);
            // SQLiteCommand for database commands.
            IDbCommand cmd = null;
            // DataReader to read database record.
            IDataReader reader = null;
            // DateTime to check if current session item is expired.
            DateTime expires;
            // String to hold serialized SessionStateItemCollection.
            string serializedItems = "";
            // True if a record is found in the database.
            bool foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            bool deleteData = false;
            // Timeout value from the data store.
            int timeout = 0;

            try
            {
                conn.Open();

                // lockRecord is true when called from GetItemExclusive and
                // false when called from GetItem.
                // Obtain a lock if possible. Ignore the record if it is expired.
                if (lockRecord)
                {
                    cmd = conn.CreateCommand();
                    cmd.CommandText = "UPDATE Sessions SET Locked = @Locked, LockDate = @LockDate WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName AND Locked = @Locked AND Expires > @Expires";
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Locked", DbType.Boolean, true));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockDate", DbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Locked", DbType.Int32, false));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now));

                    if (cmd.ExecuteNonQuery() == 0)
                        // No record was updated because the record was locked or not found.
                        locked = true;
                    else
                        // The record was updated.

                        locked = false;
                }

                // Retrieve the current session item information.
                cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT Expires, SessionItems, LockId, LockDate, Flags, Timeout FROM Sessions WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName";
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));

                // Retrieve session item data from the data source.
                reader = cmd.ExecuteReader(CommandBehavior.SingleRow);
                while (reader.Read())
                {
                    expires = reader.GetDateTime(0);

                    if (expires < DateTime.Now)
                    {
                        // The record was expired. Mark it as not locked.
                        locked = false;
                        // The session was expired. Mark the data for deletion.
                        deleteData = true;
                    }
                    else
                        foundRecord = true;

                    serializedItems = reader.GetString(1);
                    lockId = reader.GetInt32(2);
                    lockAge = DateTime.Now.Subtract(reader.GetDateTime(3));
                    actionFlags = (SessionStateActions)reader.GetInt32(4);
                    timeout = reader.GetInt32(5);
                }
                reader.Close();


                // If the returned session item is expired, 
                // delete the record from the data source.
                if (deleteData)
                {
                    cmd = conn.CreateCommand();
                    cmd.CommandText = "DELETE FROM Sessions WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName";
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));

                    cmd.ExecuteNonQuery();
                }

                // The record was not found. Ensure that locked is false.
                if (!foundRecord)
                    locked = false;

                // If the record was found and you obtained a lock, then set 
                // the lockId, clear the actionFlags,
                // and create the SessionStateStoreItem to return.
                if (foundRecord && !locked)
                {
                    lockId = (int)lockId + 1;
                    cmd = conn.CreateCommand();
                    cmd.CommandText = "UPDATE Sessions SET LockId = @LockId, Flags = 0 WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName";
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, lockId));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
                    cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, 255, ApplicationName));

                    cmd.ExecuteNonQuery();

                    // If the actionFlags parameter is not InitializeItem, 
                    // deserialize the stored SessionStateItemCollection.
                    if (actionFlags == SessionStateActions.InitializeItem)
                        item = CreateNewStoreData(context, (int)_config.Timeout.TotalMinutes);
                    else
                        item = Deserialize(context, serializedItems, timeout);
                }
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "GetSessionStoreItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                if (reader != null) { reader.Close(); }
                conn.Close();
            }

            return item;
        }

        /// <summary>
        /// Serialize is called by the SetAndReleaseItemExclusive method to 
        /// convert the SessionStateItemCollection into a Base64 string
        private string Serialize(SessionStateItemCollection items)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            if (items != null)
                items.Serialize(writer);

            writer.Close();

            return Convert.ToBase64String(ms.ToArray());
        }

        /// <summary>
        /// DeSerialize is called by the GetSessionStoreItem method to 
        /// convert the Base64 string
        /// </summary>
        private SessionStateStoreData Deserialize(HttpContext context, string serializedItems, int timeout)
        {
            MemoryStream ms = new MemoryStream(Convert.FromBase64String(serializedItems));

            SessionStateItemCollection sessionItems =
              new SessionStateItemCollection();

            if (ms.Length > 0)
            {
                BinaryReader reader = new BinaryReader(ms);
                sessionItems = SessionStateItemCollection.Deserialize(reader);
            }

            return new SessionStateStoreData(sessionItems,
              SessionStateUtility.GetSessionStaticObjects(context),
              timeout);
        }

        /// <summary>
        /// SessionStateProviderBase.ReleaseItemExclusive
        /// </summary>
        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE Sessions SET Locked = 0, Expires = @Expires WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName AND LockId = @LockId";
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes)));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, 255, ApplicationName));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, lockId));

            try
            {
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// SessionStateProviderBase.RemoveItem
        /// </summary>
        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Sessions WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName AND LockId = @LockId";
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, 255, ApplicationName));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, lockId));

            try
            {
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "RemoveItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// SessionStateProviderBase.CreateUninitializedItem
        /// </summary>
        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO Sessions (SessionId, ApplicationName, Created, Expires, LockDate, LockId, Timeout, Locked, SessionItems, Flags) Values(@SessionId, @ApplicationName, @Created, @Expires, @LockDate, @LockId, @Timeout, @Locked, @SessionItems, @Flags)";
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Created", DbType.DateTime, DateTime.Now));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now.AddMinutes((Double)timeout)));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockDate", DbType.DateTime, DateTime.Now));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@LockId", DbType.Int32, 0));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Timeout", DbType.Int32, timeout));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Locked", DbType.Boolean, false));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionItems", DbType.String, 0, ""));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Flags", DbType.Int32, 1));

            try
            {
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "CreateUninitializedItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// SessionStateProviderBase.CreateNewStoreData
        /// </summary>
        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(), SessionStateUtility.GetSessionStaticObjects(context), timeout);
        }

        /// <summary>
        /// SessionStateProviderBase.ResetItemTimeout
        /// </summary>
        public override void ResetItemTimeout(HttpContext context, string id)
        {
            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE Sessions SET Expires = @Expires WHERE SessionId = @SessionId AND ApplicationName = @ApplicationName";
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes)));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@SessionId", DbType.String, 80, id));
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@ApplicationName", DbType.String, ApplicationName));

            try
            {
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ResetItemTimeout");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Close();
            }
        }


        /// <summary>
        /// SessionStateProviderBase.InitializeRequest
        /// </summary>
        public override void InitializeRequest(HttpContext context) { }

        /// <summary>
        /// SessionStateProviderBase.EndRequest 
        /// </summary>
        public override void EndRequest(HttpContext context) { }

        /// <summary>
        /// WriteToEventLog
        /// This is a helper function that writes exception detail to the 
        /// event log. Exceptions are written to the event log as a security
        /// measure to ensure private database details are not returned to 
        /// browser. If a method does not return a status or Boolean
        /// indicating the action succeeded or failed, the caller also 
        /// throws a generic exception.
        /// </summary>
        private void WriteToEventLog(Exception e, string action)
        {
            EventLog log = new EventLog();
            log.Source = _eventSource;
            log.Log = _eventLog;

            string message = "An exception occurred communicating with the data source.\n\n";
            message += "Action: " + action + "\n\n";
            message += "Exception: " + e.ToString();

            log.WriteEntry(message);
        }

        /// <summary>
        /// Remove expired session data.
        /// </summary>
        private void CleanUpExpiredData()
        {
            IDbConnection conn = new SQLiteConnection(_connectionString);
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Sessions WHERE Expires < @Expires";
            cmd.Parameters.Add(SQLiteHelper.CreateParameter("@Expires", DbType.DateTime, DateTime.Now));
            conn.Open();
            cmd.ExecuteNonQuery();
            conn.Close();
        }
    }
}
