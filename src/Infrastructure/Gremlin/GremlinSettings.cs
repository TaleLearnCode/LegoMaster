namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin
{

	/// <summary>
	/// Represents the settings necessary to connect to a Cosmos DB Gremlin database.
	/// </summary>
	public class GremlinSettings
	{

		/// <summary>
		/// Gets or sets the host address for the Cosmos DB account.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the Cosmos DB account host address.
		/// </value>
		public string Host { get; set; }

		/// <summary>
		/// Gets or sets the key for the Cosmos DB account.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the Cosmos DB account key.
		/// </value>
		public string AccountKey { get; set; }

		/// <summary>
		/// Gets or sets the name of the database to connect to.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the database name.
		/// </value>
		public string DatabaseName { get; set; }

		/// <summary>
		/// Gets or sets the name of the container (Gremlin graph) to connect to.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the container name.
		/// </value>
		public string ContainerName { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether to connect to the Cosmos DB using SSL.
		/// </summary>
		/// <value>
		///   <c>true</c> if SSL should be enabled; otherwise, <c>false</c>.
		/// </value>
		public bool EnableSSL { get; set; } = true;

		/// <summary>
		/// Gets or sets the port hosting the Azure Cosmos DB account.
		/// </summary>
		/// <value>
		/// A <c>int</c> representing the Azure Cosmos DB account port.
		/// </value>
		public int Port { get; set; } = 443;


	}

}