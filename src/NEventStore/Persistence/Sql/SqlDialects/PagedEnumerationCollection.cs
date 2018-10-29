using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Transactions;
using NEventStore.Logging;

namespace NEventStore.Persistence.Sql.SqlDialects
{
    public class PagedEnumerationCollection : IEnumerable<IDataRecord>, IEnumerator<IDataRecord>
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (PagedEnumerationCollection));
        private readonly IDbCommand _command;
        private readonly ISqlDialect _dialect;
        private readonly IEnumerable<IDisposable> _disposable = new IDisposable[] {};
        private readonly NextPageDelegate _nextPage;
        private readonly int _pageSize;
        private readonly CancellationToken _cancellationToken;
        private readonly TransactionScope _scope;

        private IDataRecord _current;
        private bool _disposed;
        private int _position;
        private IDataReader _reader;

        public PagedEnumerationCollection(
            TransactionScope scope,
            ISqlDialect dialect,
            IDbCommand command,
            NextPageDelegate nextPage,
            int pageSize,
            CancellationToken cancellationToken,
            params IDisposable[] disposable)
        {
            _scope = scope;
            _dialect = dialect;
            _command = command;
            _nextPage = nextPage;
            _pageSize = pageSize;
            _cancellationToken = cancellationToken;
            _disposable = disposable ?? _disposable;
        }

        public virtual IEnumerator<IDataRecord> GetEnumerator()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(Messages.ObjectAlreadyDisposed);
            }

            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        bool IEnumerator.MoveNext()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(Messages.ObjectAlreadyDisposed);
            }

            if (MoveToNextRecord())
            {
                return true;
            }

            Logger.Verbose(Messages.QueryCompleted);
            return false;
        }

        public virtual void Reset()
        {
            throw new NotSupportedException("Forward-only readers.");
        }

        public virtual IDataRecord Current
        {
            get
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(Messages.ObjectAlreadyDisposed);
                }

                return _current = _reader;
            }
        }

        object IEnumerator.Current => ((IEnumerator<IDataRecord>) this).Current;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            _disposed = true;
            _position = 0;
            _current = null;

            _reader?.Dispose();

            _reader = null;

            _command?.Dispose();

            // queries do not modify state and thus calling Complete() on a so-called 'failed' query only
            // allows any outer transaction scope to decide the fate of the transaction
            _scope?.Complete(); // caller will dispose scope.

            foreach (var dispose in _disposable)
            {
                dispose.Dispose();
            }
        }

        private bool MoveToNextRecord()
        {
            if (_pageSize > 0 && _position >= _pageSize)
            {
                _command.SetParameter(_dialect.Skip, _position);
                _nextPage(_command, _current);
            }

            _reader = _reader ?? OpenNextPage();

            if (_reader.Read())
            {
                return IncrementPosition();
            }

            if (!PagingEnabled())
            {
                return false;
            }

            if (!PageCompletelyEnumerated())
            {
                return false;
            }

            Logger.Verbose(Messages.EnumeratedRowCount, _position);
            _reader.Dispose();
            _reader = OpenNextPage();

            if (_reader.Read())
            {
                return IncrementPosition();
            }

            return false;
        }

        private bool IncrementPosition()
        {
            _position++;
            return true;
        }

        private bool PagingEnabled()
        {
            return _pageSize > 0;
        }

        private bool PageCompletelyEnumerated()
        {
            return _position > 0 && 0 == _position%_pageSize;
        }

        private IDataReader OpenNextPage()
        {
            try
            {
                return _command.ExecuteReader();
            }
            catch (Exception e)
            {
                Logger.Debug(Messages.EnumerationThrewException, e.GetType());
                throw new StorageUnavailableException(e.Message, e);
            }
        }
    }
}