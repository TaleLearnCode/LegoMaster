using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin
{

	/// <summary>
	/// Handles connections to a Cosmos DB Gremlin database.
	/// </summary>
	/// <seealso cref="IDisposable" />
	public class GremlinHandler : IDisposable
	{

		/// <summary>
		/// Initializes a new instance of the <see cref="GremlinHandler"/> class.
		/// </summary>
		/// <param name="gremlinSettings">The settings for connecting to the Cosmos DB Gremlin graph.</param>
		public GremlinHandler(GremlinSettings gremlinSettings)
		{

			GremlinServer gremlinServer = new(
				gremlinSettings.Host,
				gremlinSettings.Port,
				enableSsl: gremlinSettings.EnableSSL,
				username: $"/dbs/{gremlinSettings.DatabaseName}/colls/{gremlinSettings.ContainerName}",
				password: gremlinSettings.AccountKey);

			ConnectionPoolSettings connectionPoolSettings = new()
			{
				MaxInProcessPerConnection = 10,
				PoolSize = 30,
				ReconnectionAttempts = 3,
				ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
			};

			Action<ClientWebSocketOptions> webSocketConfiguration =
					new(options =>
					{
						options.KeepAliveInterval = TimeSpan.FromSeconds(10);
					});

			GremlinClient = new(
					gremlinServer,
					new GraphSON2Reader(),
					new GraphSON2Writer(),
					GremlinClient.GraphSON2MimeType,
					connectionPoolSettings,
					webSocketConfiguration);

		}

		/// <summary>
		/// Gets or sets a mechanism for submitting Gremlin requests to the Cosmos DB Gremlin graph.
		/// </summary>
		/// <value>
		/// A <see cref="GremlinClient"/> which provides a mechanism for submitting Gremlin requests.
		/// </value>
		public GremlinClient GremlinClient { get; init; }

		/// <summary>
		/// Releases unmanaged and - optionally - managed resources.
		/// </summary>
		public void Dispose()
		{
			if (GremlinClient != null) GremlinClient.Dispose();
			GC.SuppressFinalize(this);
		}

		public T GetFirstOrDefault<T>(string query) where T : class
		{
			ResultSet<dynamic> resultSet = SubmitRequest(query).Result;
			if (resultSet.Any())
			{
				foreach (var result in resultSet)
				{
					string json = JsonSerializer.Serialize(result);
					GremlinObjectBase baseObject = JsonSerializer.Deserialize<GremlinObjectBase>(json);
					if (baseObject != null)
					{
						return baseObject.Label switch
						{
							GremlinLabels.Category => CategoryVertex.DeserializeAsCategory(json) as T,
							GremlinLabels.Color => ColorVertex.DeserializeAsColor(json) as T,
							GremlinLabels.Part => PartVertex.DeserializeAsPart(json) as T,
							GremlinLabels.Set => SetVertex.DeserializeAsSet(json) as T,
							GremlinLabels.Theme => ThemeVertex.DeserializeAsTheme(json) as T,
							_ => default,
						};
					}
				}
			}
			return default;
		}

		public List<T> GetList<T>(string query) where T : class
		{
			List<T> results = new();
			ResultSet<dynamic> resultSet = SubmitRequest(query).Result;
			if (resultSet.Any())
				foreach (var result in resultSet)
				{
					string json = JsonSerializer.Serialize(result);
					GremlinObjectBase baseObject = JsonSerializer.Deserialize<GremlinObjectBase>(json);
					if (baseObject != null)
						switch (baseObject.Label)
						{
							case GremlinLabels.Category:
								results.Add(CategoryVertex.DeserializeAsCategory(json) as T);
								break;
							case GremlinLabels.Color:
								results.Add(ColorVertex.DeserializeAsColor(json) as T);
								break;
							case GremlinLabels.Part:
								results.Add(PartVertex.DeserializeAsPart(json) as T);
								break;
							case GremlinLabels.Set:
								results.Add(SetVertex.DeserializeAsSet(json) as T);
								break;
							case GremlinLabels.Theme:
								results.Add(ThemeVertex.DeserializeAsTheme(json) as T);
								break;
						}
				}

			return results;
		}

		private Task<ResultSet<dynamic>> SubmitRequest(string query)
		{
			return GremlinClient.SubmitAsync<dynamic>(query);
		}


	}

}



