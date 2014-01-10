using NEventStore.Logging;
using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace NEventStore.Persistence.AzureBlob
{
	public class AzureBlobPersistenceWireup : PersistenceWireup
	{
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof (AzureBlobPersistenceWireup));

		public AzureBlobPersistenceWireup(Wireup inner, string connectionString, ISerialize serializer, AzureBlobPersistenceOptions persistenceOptions)
			: base(inner)
		{
			Logger.Debug("Configuring Azure blob persistence engine.");

			var options = Container.Resolve<TransactionScopeOption>();
			if (options != TransactionScopeOption.Suppress)
			{ Logger.Warn(Messages.TransactionScopeNotSupportedSettingIgnored); }

			Container.Register(c => new AzureBlobPersistenceFactory(connectionString, serializer, persistenceOptions).Build());
		}
	}
}
