namespace NServiceBus.NHibernate.SharedSession
{
    using System;
    using System.Data;
    using Outbox;
    using Pipeline;

    class DbConnectionProvider : IDbConnectionProvider
    {
        public PipelineExecutor PipelineExecutor { get; set; }

        public string DefaultConnectionString { get; set; }

        public IDbConnection Connection
        {
            get
            {
                IDbConnection existingConnection;
                Lazy<IDbConnection> lazyExistingConnection;

                if (PipelineExecutor.CurrentContext.TryGet(string.Format("SqlConnection-{0}", DefaultConnectionString), out existingConnection))
                {
                    return existingConnection;
                }

                if (!PipelineExecutor.CurrentContext.TryGet(string.Format("LazySqlConnection-{0}", DefaultConnectionString), out lazyExistingConnection))
                {
                    throw new Exception("No active sql connection found");
                }

                return lazyExistingConnection.Value;
            }
        }

        public bool TryGetConnection(out IDbConnection connection, string connectionString = null)
        {
            if (connectionString == null)
            {
                connectionString = DefaultConnectionString;
            }

            var result = PipelineExecutor.CurrentContext.TryGet(string.Format("SqlConnection-{0}", connectionString), out connection);

            if (result == false)
            {
                Lazy<IDbConnection> lazyConnection;

                result = PipelineExecutor.CurrentContext.TryGet(string.Format("LazySqlConnection-{0}", connectionString), out lazyConnection);
                
                if (result)
                {
                    connection = lazyConnection.Value;
                }
            }

            return result;
        }
    }
}