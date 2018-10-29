namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using NEventStore.Persistence.Sql.SqlDialects;

    public interface IDbStatement : IDisposable
    {
        int PageSize { get; set; }

        void AddParameter(string name, object value, DbType? parameterType = null);

        Task<int> ExecuteNonQuery(string commandText, CancellationToken cancellationToken);

        Task<int> ExecuteWithoutExceptions(string commandText, CancellationToken cancellationToken);

        Task<object> ExecuteScalar(string commandText, CancellationToken cancellationToken);

        Task<IEnumerable<IDataRecord>> ExecuteWithQuery(string queryText, CancellationToken cancellationToken);

        Task<IEnumerable<IDataRecord>> ExecutePagedQuery(string queryText, NextPageDelegate nextPage, CancellationToken cancellationToken);
    }
}