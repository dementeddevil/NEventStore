namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using NEventStore.Logging;
    using NEventStore.Persistence.Sql;

    public class CommonDbStatement : IDbStatement
    {
        private const int InfinitePageSize = 0;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (CommonDbStatement));
        private readonly IDbConnection _connection;
        private readonly ISqlDialect _dialect;
        private readonly TransactionScope _scope;
        private readonly IDbTransaction _transaction;

        public CommonDbStatement(
            ISqlDialect dialect,
            TransactionScope scope,
            IDbConnection connection,
            IDbTransaction transaction)
        {
            Parameters = new Dictionary<string, Tuple<object, DbType?>>();

            _dialect = dialect;
            _scope = scope;
            _connection = connection;
            _transaction = transaction;
        }

        protected IDictionary<string, Tuple<object, DbType?>> Parameters { get; private set; }

        protected ISqlDialect Dialect => _dialect;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual int PageSize { get; set; }

        public virtual void AddParameter(string name, object value, DbType? parameterType = null)
        {
            Logger.Debug(Messages.AddingParameter, name);
            Parameters[name] = Tuple.Create(_dialect.CoalesceParameterValue(value), parameterType);
        }

        public virtual Task<int> ExecuteWithoutExceptions(string commandText, CancellationToken cancellationToken)
        {
            try
            {
                return ExecuteNonQuery(commandText, cancellationToken);
            }
            catch (Exception)
            {
                Logger.Debug(Messages.ExceptionSuppressed);
                return Task.FromResult(0);
            }
        }

        public virtual Task<int> ExecuteNonQuery(string commandText, CancellationToken cancellationToken)
        {
            try
            {
                using (var command = BuildCommand(commandText))
                {
                    if (command is DbCommand asyncCommand)
                    {
                        return asyncCommand.ExecuteNonQueryAsync(cancellationToken);
                    }

                    return Task.FromResult(command.ExecuteNonQuery());
                }
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }

                throw;
            }
        }

        public virtual Task<object> ExecuteScalar(string commandText, CancellationToken cancellationToken)
        {
            try
            {
                using (var command = BuildCommand(commandText))
                {
                    if (command is DbCommand asyncCommand)
                    {
                        return asyncCommand.ExecuteScalarAsync(cancellationToken);
                    }

                    return Task.FromResult(command.ExecuteScalar());
                }
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }
                throw;
            }
        }

        public virtual Task<IEnumerable<IDataRecord>> ExecuteWithQuery(string queryText, CancellationToken cancellationToken)
        {
            return ExecuteQuery(queryText, (query, latest) => { }, InfinitePageSize, cancellationToken);
        }

        public virtual Task<IEnumerable<IDataRecord>> ExecutePagedQuery(string queryText, NextPageDelegate nextPage, CancellationToken cancellationToken)
        {
            var pageSize = _dialect.CanPage ? PageSize : InfinitePageSize;
            if (pageSize > 0)
            {
                Logger.Verbose(Messages.MaxPageSize, pageSize);
                Parameters.Add(_dialect.Limit, Tuple.Create((object) pageSize, (DbType?) null));
            }

            return ExecuteQuery(queryText, nextPage, pageSize, cancellationToken);
        }

        protected virtual void Dispose(bool disposing)
        {
            Logger.Verbose(Messages.DisposingStatement);

            _transaction?.Dispose();

            _connection?.Dispose();

            _scope?.Dispose();
        }

        protected virtual Task<IEnumerable<IDataRecord>> ExecuteQuery(string queryText, NextPageDelegate nextpage, int pageSize, CancellationToken cancellationToken)
        {
            Parameters.Add(_dialect.Skip, Tuple.Create((object) 0, (DbType?) null));
            var command = BuildCommand(queryText);

            try
            {
                return Task.FromResult<IEnumerable<IDataRecord>>(new PagedEnumerationCollection(_scope, _dialect, command, nextpage, pageSize, cancellationToken, this));
            }
            catch (Exception)
            {
                command.Dispose();
                throw;
            }
        }

        protected virtual IDbCommand BuildCommand(string statement)
        {
            Logger.Verbose(Messages.CreatingCommand);
            var command = _connection.CreateCommand();
            command.Transaction = _transaction;
            command.CommandText = statement;

            Logger.Verbose(Messages.ClientControlledTransaction, _transaction != null);
            Logger.Verbose(Messages.CommandTextToExecute, statement);

            BuildParameters(command);

            return command;
        }

        protected virtual void BuildParameters(IDbCommand command)
        {
            foreach (var item in Parameters)
            {
                BuildParameter(command, item.Key, item.Value.Item1, item.Value.Item2);
            }
        }

        protected virtual void BuildParameter(IDbCommand command, string name, object value, DbType? dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            SetParameterValue(parameter, value, dbType);

            Logger.Verbose(Messages.BindingParameter, name, parameter.Value);
            command.Parameters.Add(parameter);
        }

        protected virtual void SetParameterValue(IDataParameter param, object value, DbType? type)
        {
            param.Value = value ?? DBNull.Value;
            param.DbType = type ?? (value == null ? DbType.Binary : param.DbType);
        }
    }
}