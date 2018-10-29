using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace NEventStore.Persistence.Sql.SqlDialects
{
    public class OracleDbStatement : CommonDbStatement
    {
        private readonly ISqlDialect _dialect;

        public OracleDbStatement(ISqlDialect dialect, TransactionScope scope, IDbConnection connection, IDbTransaction transaction)
            : base(dialect, scope, connection, transaction)
        {
            _dialect = dialect;
        }

        public override void AddParameter(string name, object value, DbType? dbType = null)
        {
            name = name.Replace('@', ':');

            if (value is Guid)
            {
                base.AddParameter(name, ((Guid) value).ToByteArray(), null);
            }
            else
            {
                base.AddParameter(name, value, dbType);
            }
        }

        public override Task<int> ExecuteNonQuery(string commandText, CancellationToken cancellationToken)
        {
            try
            {
                using (var command = BuildCommand(commandText))
                {
                    return Task.FromResult(command.ExecuteNonQuery());
                }
            }
            catch (Exception e)
            {
                if (Dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }

                throw;
            }
        }

        protected override IDbCommand BuildCommand(string statement)
        {
            var command = base.BuildCommand(statement);
            var pi = command.GetType().GetProperty("BindByName");
            if (pi != null)
            {
                pi.SetValue(command, true, null);
            }
            return command;
        }

        protected override void BuildParameter(IDbCommand command, string name, object value, DbType? dbType)
        {
            //HACK
            if (name == _dialect.Payload && value is DbParameter)
            {
                command.Parameters.Add(value);
                return;
            }
            base.BuildParameter(command, name, value, dbType);
        }
    }
}