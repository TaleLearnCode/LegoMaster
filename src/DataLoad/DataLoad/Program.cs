using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using TinyCsvParser;

namespace DataLoad
{
	class Program
	{


		static async Task Main()
		{
			string containerLink = $"/dbs/{Settings.Database}/colls/{Settings.Container}";
			Console.WriteLine($"Connecting to: host: {Settings.Host}, port: {Settings.Port}, container: {containerLink}, ssl: {Settings.EnableSSL}");
			var gremlinServer = new GremlinServer(Settings.Host, Settings.Port, enableSsl: Settings.EnableSSL,
																							username: containerLink,
																							password: Settings.PrimaryKey);

			ConnectionPoolSettings connectionPoolSettings = new()
			{
				MaxInProcessPerConnection = 10,
				PoolSize = 30,
				ReconnectionAttempts = 3,
				ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
			};

			var webSocketConfiguration = new Action<ClientWebSocketOptions>(options =>
			{
				options.KeepAliveInterval = TimeSpan.FromSeconds(10);
			});

			using var gremlinClient = new GremlinClient(
					gremlinServer,
					new GraphSON2Reader(),
					new GraphSON2Writer(),
					GremlinClient.GraphSON2MimeType,
					connectionPoolSettings,
					webSocketConfiguration);

			await ImportColorsAsync(gremlinClient, @"C:\Users\chadg\Downloads\Lego Database\");
			await ImportCategoriesAsync(gremlinClient, @"C:\Users\chadg\Downloads\Lego Database\");
			await ImportThemesAsync(gremlinClient, @"C:\Users\chadg\Downloads\Lego Database\");

		}

		private static void SubmitRequest(GremlinClient gremlinClient, string query)
		{
			try
			{
				gremlinClient.SubmitAsync<dynamic>(query);
			}
			catch (ResponseException e)
			{
				ConsoleColor backgroundColor = Console.BackgroundColor;
				ConsoleColor foregroundColor = Console.ForegroundColor;
				Console.BackgroundColor = ConsoleColor.Red;
				Console.ForegroundColor = ConsoleColor.White;
				Console.WriteLine($"\tCosmos query failed — {e.StatusCode}");
				Console.BackgroundColor = backgroundColor;
				Console.ForegroundColor = foregroundColor;
				throw;
			}
		}

		private static CsvParserOptions GetCsvParserOptions()
		{
			return new CsvParserOptions(true, ',');
		}

		private static async Task ImportColorsAsync(GremlinClient gremlinClient, string filePath = @"C:\Users\chadg\Downloads\Lego Database\")
		{

			CsvColorMapping csvMapper = new();
			CsvParser<Color> csvParser = new(GetCsvParserOptions(), csvMapper);

			var result = csvParser
					.ReadFromFile(@$"{filePath}colors.csv", Encoding.ASCII)
					.ToList();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('color').drop()");
			foreach (var color in result)
			{
				Console.WriteLine($"{color.Result.Name}");
				string query = $"g.addV('color').property('userId', 'catalog').property('name', '{color.Result.Name}').property('rgb', '{color.Result.RGB}').property('isTranslucent', '{color.Result.IsTranslucent}').property('rebrickableId', '{color.Result.Id}')";
				SubmitRequest(gremlinClient, query);
			}

		}

		private static async Task ImportCategoriesAsync(GremlinClient gremlinClient, string filePath = @"C:\Users\chadg\Downloads\Lego Database\")
		{

			CsvCategoryMapping csvMapper = new();
			CsvParser<Category> csvParser = new(GetCsvParserOptions(), csvMapper);

			var result = csvParser
					.ReadFromFile(@$"{filePath}part_categories.csv", Encoding.ASCII)
					.ToList();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('category').drop()");
			foreach (var category in result)
			{
				Console.WriteLine($"{category.Result.Name}");
				string query = $"g.addV('category').property('userId', 'catalog').property('name', '{category.Result.Name}').property('rebrickableId', '{category.Result.Id}')";
				SubmitRequest(gremlinClient, query);
			}

		}

		private static async Task ImportThemesAsync(GremlinClient gremlinClient, string filePath = @"C:\Users\chadg\Downloads\Lego Database\")
		{

			CsvThemeMapping csvMapper = new();
			CsvParser<Theme> csvParser = new(GetCsvParserOptions(), csvMapper);

			var result = csvParser
					.ReadFromFile(@$"{filePath}themes.csv", Encoding.ASCII)
					.ToList();

			Dictionary<string, Theme> themes = new();
			Console.WriteLine("Reading in CSV file...");
			foreach (var category in result)
			{
				if (!themes.ContainsKey(category.Result.RebrickableId))
				{
					Theme parentTheme = category.Result;
					parentTheme.Id = Guid.NewGuid().ToString();
					parentTheme.Name = parentTheme.Name.Replace("\'", "\\\'");
					themes.Add(category.Result.RebrickableId, category.Result);
				}
			}

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('theme').drop()");
			List<string> edges = new();
			string vertexQuery;
			string vertexId;
			foreach (Theme theme in themes.Values)
			{
				Console.WriteLine($"{theme.Name}");
				vertexId = Guid.NewGuid().ToString();
				if (string.IsNullOrWhiteSpace(theme.ParentId))
				{
					vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
				}
				else
				{
					if (themes.ContainsKey(theme.ParentId))
					{
						vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{vertexId}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}').property('parentId', 'theme_{theme.ParentId}').property('parentName', '{themes[theme.ParentId]}')";
						edges.Add($"g.V('{vertexId}').addE('isChildOf').to(g.V('{themes[theme.ParentId].Id}'))");
					}
					else
					{
						vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{vertexId}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
					}
				}
				await gremlinClient.SubmitAsync<dynamic>(vertexQuery);
			}

			Console.WriteLine("Adding the theme edges...");
			foreach (string edge in edges)
				SubmitRequest(gremlinClient, edge);

		}

	}
}