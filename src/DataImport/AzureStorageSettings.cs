namespace TaleLearnCode.LEGOMaster.DataImport
{
	/// <summary>
	/// Represents the settings for connecting to the Twitch Command Azure Storage settings.
	/// </summary>
	public class AzureStorageSettings
	{

		/// <summary>
		/// Gets or sets the URL for connecting to the Azure Storage account.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the Azure Storage account URL.
		/// </value>
		public string Url { get; set; }

		/// <summary>
		/// Gets or sets the name of the Azure Storage account used by Twitch Commander.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the Azure Storage account name.
		/// </value>
		public string AccountName { get; set; }

		/// <summary>
		/// Gets or sets the key of the Azure Storage account used by Twitch Commander.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the Azure Storage account key.
		/// </value>
		public string AccountKey { get; set; }

		public string RebrickableTableName { get; set; }

	}

}