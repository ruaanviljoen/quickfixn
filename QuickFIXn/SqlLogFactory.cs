using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QuickFix
{
    /// <summary>
    /// Creates a event and messages store that stores messages and events in a sql database
    /// </summary>
    public class SqlLogFactory : ILogFactory
    {
        SessionSettings settings_;

        #region LogFactory Members

        public SqlLogFactory(SessionSettings settings)
        {
            settings_ = settings;
        }

        /// <summary>
        /// Creates a sql-based message and event store
        /// </summary>
        /// <param name="sessionID">session ID for the message store</param>
        /// <returns></returns>
        public ILog Create(SessionID sessionID)
        {

            return new SqlLog(settings_.Get(sessionID).GetString(SessionSettings.SQL_LOG_CONNECTION_STRING), sessionID);
        }

        #endregion
    }
}
