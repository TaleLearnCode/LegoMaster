using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using TinyCsvParser;

namespace DataLoad
{
	class Program
	{


		static void Main()
		{


			CsvParserOptions csvParserOptions = new CsvParserOptions(true, ',');
			CsvColorMapping csvMapper = new CsvColorMapping();
			CsvParser<Color> csvParser = new CsvParser<Color>(csvParserOptions, csvMapper);

			var result = csvParser
					.ReadFromFile(@"C:\Users\chadg\Downloads\Lego Database\colors.csv", Encoding.ASCII)
					.ToList();

			string containerLink = $"/dbs/{Settings.Database}/colls/{Settings.Container}";
			Console.WriteLine($"Connecting to: host: {Settings.Host}, port: {Settings.Port}, container: {containerLink}, ssl: {Settings.EnableSSL}");
			var gremlinServer = new GremlinServer(Settings.Host, Settings.Port, enableSsl: Settings.EnableSSL,
																							username: containerLink,
																							password: Settings.PrimaryKey);

			ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings()
			{
				MaxInProcessPerConnection = 10,
				PoolSize = 30,
				ReconnectionAttempts = 3,
				ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
			};

			var webSocketConfiguration =
					new Action<ClientWebSocketOptions>(options =>
					{
						options.KeepAliveInterval = TimeSpan.FromSeconds(10);
					});


			using (var gremlinClient = new GremlinClient(
					gremlinServer,
					new GraphSON2Reader(),
					new GraphSON2Writer(),
					GremlinClient.GraphSON2MimeType,
					connectionPoolSettings,
					webSocketConfiguration))
			{

				foreach (var color in result)
				{
					Console.WriteLine($"{color.Result.Name}");
					string query = $"g.addV('color').property('userId', 'catalog').property('id', 'color_{color.Result.Id}').property('name', '{color.Result.Name}').property('rgb', '{color.Result.RGB}').property('isTranslucent', '{color.Result.IsTranslucent}')";
					var resultSet = SubmitRequest(gremlinClient, query).Result;
					if (resultSet.Count > 0)
					{
						Console.WriteLine("\tResult:");
						foreach (var resultValue in resultSet)
						{
							// The vertex results are formed as Dictionaries with a nested dictionary for their properties
							string output = JsonConvert.SerializeObject(resultValue);
							Console.WriteLine($"\t{output}");
						}
						Console.WriteLine();
					}



				}




			}

		}


		private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, string query)
		{
			try
			{
				return gremlinClient.SubmitAsync<dynamic>(query);
			}
			catch (ResponseException e)
			{
				Console.WriteLine("\tRequest Error!");

				// Print the Gremlin status code.
				Console.WriteLine($"\tStatusCode: {e.StatusCode}");

				throw;
			}
		}

	}
}