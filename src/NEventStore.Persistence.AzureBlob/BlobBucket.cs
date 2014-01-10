using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// This holds the serialized data from commits
	/// </summary>
	[Serializable]
	public class BlobBucket
	{
		/// <summary>
		/// The value which identifies bucket to which the the stream and the the commit belongs.
		/// </summary>
		public string BucketId
		{ get; set; }

		/// <summary>
		/// The value which uniquely identifies the stream in a bucket to which the commit belongs.
		/// </summary>
		public string StreamId
		{ get; set; }

		/// <summary>
		/// The value which indicates the revision of the most recent event in the stream to which this commit applies.
		/// </summary>
		public int StreamRevision
		{ get; set; }

		/// <summary>
		///  Number of events attached to this commit.
		/// </summary>
		public int Items
		{ get; set; }

		/// <summary>
		/// The value which uniquely identifies the commit within the stream.
		/// </summary>
		public Guid CommitId
		{ get; set; }

		/// <summary>
		/// The value which indicates the sequence (or position) in the stream to which this commit applies.
		/// </summary>
		public int CommitSequence
		{ get; set; }

		/// <summary>
		/// The point in time at which the commit was persisted.
		/// </summary>
		public DateTime CommitStamp
		{ get; set; }

		/// <summary>
		/// The metadata which provides additional, unstructured information about this commit.
		/// </summary>
		public IDictionary<string, object> Headers
		{ get; set; }

		/// <summary>
		/// The collection of event messages to be committed as a single unit.
		/// </summary>
		public List<EventMessage> Events
		{ get; set; }

		/// <summary>
		/// Whether the commit has been dispatched.
		/// </summary>
		public bool Dispatched
		{ get; set; }
	}
}
