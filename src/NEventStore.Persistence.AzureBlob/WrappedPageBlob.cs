using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NEventStore.Logging;
using AzureStorage = Microsoft.WindowsAzure.Storage;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// A WrappedPageBlob
	/// </summary>
	public class WrappedPageBlob
	{
		private const int _blobPageSize = 512;
		private readonly CloudPageBlob _pageBlob;
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof(WrappedPageBlob));

		#region construction

		/// <summary>
		/// Create a new wrapped page blob
		/// </summary>
		/// <param name="pageBlob"></param>
		private WrappedPageBlob(CloudPageBlob pageBlob)
		{
			if (pageBlob == null)
			{ throw new ArgumentNullException("pageBlob"); }

			_pageBlob = pageBlob;
		}

		/// <summary>
		/// Creates a new page blob if it does not already exist.
		/// </summary>
		/// <param name="blobContainer">the container that owns the blob</param>
		/// <param name="blobId">the id of the blob</param>
		/// <returns>the already existing or newly created page blob</returns>
		/// <remarks>This call should only be used when uncertain if the blob already exists.  It costs an extra API call</remarks>
		public static WrappedPageBlob CreateNewIfNotExists(CloudBlobContainer blobContainer, string blobId)
		{
			var pageBlob = GetAssumingExists(blobContainer, blobId);
			return pageBlob ?? CreateNew(blobContainer, blobId);
		}

		/// <summary>
		/// Gets all wrapped page blobs matching the blob id prefix
		/// </summary>
		/// <param name="blobContainer"></param>
		/// <param name="blobId"></param>
		/// <returns></returns>
		public static IEnumerable<WrappedPageBlob> GetAllMatchinPrefix(CloudBlobContainer blobContainer, string prefix)
		{
			var pageBlobs = blobContainer
				.ListBlobs(prefix, true, BlobListingDetails.Metadata).OfType<CloudPageBlob>();

			foreach (var pageBlob in pageBlobs)
			{ yield return new WrappedPageBlob(pageBlob); }
		}

		/// <summary>
		/// Gets a wrapped page blob.
		/// </summary>
		/// <param name="blobContainer">the container that owns the blob</param>
		/// <param name="blobId">the id of the blob</param>
		/// <returns></returns>
		public static WrappedPageBlob GetAssumingExists(CloudBlobContainer blobContainer, string blobId)
		{
			var pageBlob = blobContainer
				.ListBlobs(blobId, true, BlobListingDetails.Metadata).OfType<CloudPageBlob>()
				.SingleOrDefault();

			return (pageBlob == null) ? null : new WrappedPageBlob(pageBlob);
		}

		/// <summary>
		/// Creates a new wrapped page blob.  If it exists is will throw, this call assumes it does not exist
		/// </summary>
		/// <param name="blobContainer">the container that owns the blob</param>
		/// <param name="blobId">the id of the blob</param>
		/// <returns></returns>
		public static WrappedPageBlob CreateNew(CloudBlobContainer blobContainer, string blobId)
		{
			var pageBlob = blobContainer.GetPageBlobReference(blobId);
			pageBlob.Create((long)512);
			pageBlob.FetchAttributes();
			return new WrappedPageBlob(pageBlob);
		}

		#endregion

		#region casts

		/// <summary>
		/// Implicit cast to get to the page blob
		/// </summary>
		/// <param name="wrapped"></param>
		/// <returns></returns>
		public static implicit operator CloudPageBlob(WrappedPageBlob wrapped)
		{ return wrapped.PageBlob; }

		#endregion

		/// <summary>
		/// returns the wrapped page blob
		/// </summary>
		private CloudPageBlob PageBlob
		{ get { return _pageBlob; } }

		/// <summary>
		/// Gets the actual page blob metadata.
		/// </summary>
		/// <remarks>Anything added to this dictionary will be submitted during a call to set metadata</remarks>
		public IDictionary<string, string> Metadata
		{
			get
			{ return _pageBlob.Metadata; }
		}

		/// <summary>
		/// Gets the actual properties of the page blob
		/// </summary>
		public BlobProperties Properties
		{
			get
			{ return _pageBlob.Properties; }
		}

		/// <summary>
		/// refetches the blob attributes.  this only needs to be done when fresher attributes
		/// than fetched when the wrapped page was first created
		/// </summary>
		/// <param name="accessCondition">the access condition</param>
		public void RefetchAttributes(AccessCondition accessCondition = null)
		{
			try
			{ _pageBlob.FetchAttributes(accessCondition); }
			catch (AzureStorage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Sets the metadata that is currently set
		/// </summary>
		/// <param name="accessCondition"></param>
		public void SetMetadata(AccessCondition accessCondition = null)
		{
			Logger.Verbose("Setting metadata");

			try
			{ _pageBlob.SetMetadata(accessCondition); }
			catch (AzureStorage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Download the page range specified
		/// </summary>
		/// <param name="startPage">start page</param>
		/// <param name="endPage">end page</param>
		/// <param name="accessCondition">access conditions</param>
		/// <returns></returns>
		public byte[] DownloadBytes(int startIndex, int endIndex, AccessCondition accessCondition = null)
		{
			try
			{
				var data = new byte[endIndex - startIndex];
				Logger.Verbose("Downloading [{0}] bytes", data.Length);
				_pageBlob.DownloadRangeToByteArray(data, 0, startIndex, data.Length, accessCondition);
				return data;
			}
			catch (AzureStorage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Writes to the page blob
		/// </summary>
		/// <param name="pageData">must be page size</param>
		/// <param name="startOffset">must be page aligned</param>
		/// <param name="accessCondition"></param>
		public void Write(Stream pageData, int startOffset, AccessCondition accessCondition = null)
		{
			Logger.Verbose("Writing [{0}] bytes", pageData.Length);
			try
			{ _pageBlob.WritePages(pageData, startOffset, accessCondition: accessCondition); }
			catch (AzureStorage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		public void Resize(int neededSize)
		{
			Logger.Verbose("Resizing page blob");
			try
			{
				var currentSize = _pageBlob.Properties.Length;
				var newSize = GetPageAlignedSize(neededSize);
				_pageBlob.Resize(newSize, AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag));
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Get page aligned number of bytes from a non page aligned number
		/// </summary>
		/// <param name="nonAligned"></param>
		/// <returns></returns>
		private int GetPageAlignedSize(int nonAligned)
		{
			var remainder = nonAligned % _blobPageSize;
			return (remainder == 0) ? nonAligned : nonAligned + (_blobPageSize - remainder);
		}

		/// <summary>
		/// Helper to remat exceptions
		/// </summary>
		/// <param name="ex"></param>
		/// <returns></returns>
		private static Exception HandleAndRemapCommonExceptions(Microsoft.WindowsAzure.Storage.StorageException ex)
		{
			if (ex.Message.Contains("412"))
			{ return new ConcurrencyException("concurrency exception in markcommitasdispachted", ex); }
			else
			{ return ex; }
		}
	}
}
