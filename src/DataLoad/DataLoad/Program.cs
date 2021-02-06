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

			string filePath = @"C:\Users\chadg\Downloads\Lego Database\";

			//Dictionary<string, Color> colors = await ImportColorsAsync(gremlinClient, filePath);
			//Dictionary<string, Category> categories = await ImportCategoriesAsync(gremlinClient, filePath);
			Dictionary<string, Theme> themes = await ImportThemesAsync(gremlinClient, filePath);
			//Dictionary<string, Part> parts = await ImportPartsAsync(gremlinClient, filePath, categories);
			Dictionary<string, Set> sets = await ImportSetsAsync(gremlinClient, filePath, themes);

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

		private static async Task<Dictionary<string, Color>> ImportColorsAsync(GremlinClient gremlinClient, string filePath)
		{

			CsvColorMapping csvMapper = new();
			CsvParser<Color> csvParser = new(GetCsvParserOptions(), csvMapper);

			var parseResults = csvParser
					.ReadFromFile(@$"{filePath}colors.csv", Encoding.ASCII)
					.ToList();

			Dictionary<string, Color> results = new();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('color').drop()");
			foreach (var color in parseResults)
			{
				Console.WriteLine($"{color.Result.Name}");
				color.Result.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('color').property('userId', 'catalog').property('id', '{color.Result.Id}').property('name', '{color.Result.Name}').property('rgb', '{color.Result.RGB}').property('isTranslucent', '{color.Result.IsTranslucent}').property('rebrickableId', '{color.Result.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(color.Result.RebrickableId, color.Result);
			}

			return results;
		}

		private static async Task<Dictionary<string, Category>> ImportCategoriesAsync(GremlinClient gremlinClient, string filePath)
		{

			CsvCategoryMapping csvMapper = new();
			CsvParser<Category> csvParser = new(GetCsvParserOptions(), csvMapper);

			var categories = csvParser
					.ReadFromFile(@$"{filePath}part_categories.csv", Encoding.ASCII)
					.ToList();

			Dictionary<string, Category> results = new();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('category').drop()");
			string vertexId;
			foreach (var category in categories)
			{
				Console.WriteLine($"{category.Result.Name}");
				category.Result.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('category').property('id', '{category.Result.Id}').property('userId', 'catalog').property('name', '{category.Result.Name}').property('rebrickableId', '{category.Result.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(category.Result.RebrickableId, category.Result);
			}

			return results;

		}

		private static async Task<Dictionary<string, Theme>> ImportThemesAsync(GremlinClient gremlinClient, string filePath)
		{

			CsvThemeMapping csvMapper = new();
			CsvParser<Theme> csvParser = new(GetCsvParserOptions(), csvMapper);

			var parseResults = csvParser
					.ReadFromFile(@$"{filePath}themes.csv", Encoding.ASCII)
					.ToList();

			Dictionary<string, Theme> themes = new();
			Console.WriteLine("Reading in CSV file...");
			foreach (var category in parseResults)
			{
				if (!themes.ContainsKey(category.Result.RebrickableId))
				{
					Theme theme = category.Result;
					theme.Id = Guid.NewGuid().ToString();
					theme.Name = theme.Name.Replace("\'", "\\\'");
					themes.Add(category.Result.RebrickableId, category.Result);
				}
			}



			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('theme').drop()");
			List<string> edges = new();
			string vertexQuery;
			Dictionary<string, Theme> results = new();
			foreach (Theme theme in themes.Values)
			{
				Console.WriteLine($"{theme.Name}");
				if (string.IsNullOrWhiteSpace(theme.ParentId))
				{
					vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
				}
				else
				{
					if (themes.ContainsKey(theme.ParentId))
					{
						vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}').property('parentId', 'theme_{theme.ParentId}').property('parentName', '{themes[theme.ParentId]}')";
						edges.Add($"g.V('{theme.Id}').addE('isChildOf').to(g.V('{themes[theme.ParentId].Id}'))");
					}
					else
					{
						vertexQuery = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
					}
				}
				await gremlinClient.SubmitAsync<dynamic>(vertexQuery);
				results.Add(theme.RebrickableId, theme);
			}

			Console.WriteLine("Adding the theme edges...");
			foreach (string edge in edges)
				SubmitRequest(gremlinClient, edge);

			return results;

		}

		private static async Task<Dictionary<string, Part>> ImportPartsAsync(GremlinClient gremlinClient, string filePath, Dictionary<string, Category> categories)
		{

			CsvPartMapping csvMapper = new();
			CsvParser<Part> csvParser = new(GetCsvParserOptions(), csvMapper);

			var parts = csvParser
					.ReadFromFile(@$"{filePath}parts.csv", Encoding.ASCII)
					.ToList();


			Dictionary<string, Part> results = new();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('part').drop()");
			StringBuilder query;
			string edge;
			foreach (var part in parts)
			{
				Console.WriteLine($"{part.Result.Name}");
				part.Result.Id = Guid.NewGuid().ToString();
				part.Result.Name = part.Result.Name.Replace("\'", "\\\'");
				query = new($"g.addV('part').property('id', '{part.Result.Id}').property('userId', 'catalog').property('name', '{part.Result.Name}').property('partNumber', '{part.Result.PartNumber}').property('categoryId', '{part.Result.CategoryId}')");
				if (categories.ContainsKey(part.Result.CategoryId))
				{
					query.Append($".property('categoryName','{categories[part.Result.CategoryId].Name}')");
					edge = $"g.V('{part.Result.Id}').addE('isOf').to(g.V('{categories[part.Result.CategoryId].Id}'))";
				}
				else
				{
					edge = string.Empty;
				}
				try
				{
					await gremlinClient.SubmitAsync<dynamic>(query.ToString());
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
				try
				{
					if (!string.IsNullOrWhiteSpace(edge)) await gremlinClient.SubmitAsync<dynamic>(edge);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
				results.Add(part.Result.PartNumber, part.Result);
			}

			return results;
		}

		private static async Task<Dictionary<string, Set>> ImportSetsAsync(GremlinClient gremlinClient, string filePath, Dictionary<string, Theme> themes)
		{

			CsvSetMapping csvMapper = new();
			CsvParser<Set> csvParser = new(GetCsvParserOptions(), csvMapper);

			var sets = csvParser
					.ReadFromFile(@$"{filePath}sets.csv", Encoding.ASCII)
					.ToList();


			Dictionary<string, Set> results = new();

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('set').drop()");
			StringBuilder query;
			string edge;
			foreach (var set in sets)
			{
				Console.WriteLine($"{set.Result.Name}");
				set.Result.Id = Guid.NewGuid().ToString();
				set.Result.Name = set.Result.Name.Replace("\'", "\\\'");
				query = new($"g.addV('set').property('id', '{set.Result.Id}').property('userId', 'catalog').property('name', '{set.Result.Name}').property('setNumber', '{set.Result.SetNumber}').property('year', {set.Result.Year}).property('themeId', '{set.Result.ThemeId}').property('partCount', '{set.Result.PartCount}')");
				if (themes.ContainsKey(set.Result.ThemeId))
				{
					query.Append($".property('themeName','{themes[set.Result.ThemeId].Name}')");
					edge = $"g.V('{set.Result.Id}').addE('isOf').to(g.V('{themes[set.Result.ThemeId].Id}'))";
				}
				else
				{
					edge = string.Empty;
				}
				await gremlinClient.SubmitAsync<dynamic>(query.ToString());
				if (!string.IsNullOrWhiteSpace(edge)) await gremlinClient.SubmitAsync<dynamic>(edge);
				results.Add(set.Result.SetNumber, set.Result);
			}

			return results;
		}

		private static async Task ImportSetInventoriesAsync(GremlinClient gremlinClient, string filePath, Dictionary<string, Set> sets, Dictionary<string, Part> parts)
		{

			Console.WriteLine("Reading in the inventories.csv file...");
			CsvInventoryMapping csvInventoryMapper = new();
			CsvParser<Inventory> csvInventoryParser = new(GetCsvParserOptions(), csvInventoryMapper);
			var parsedInventories = csvInventoryParser
					.ReadFromFile(@$"{filePath}inventories.csv", Encoding.ASCII)
					.ToList();

			Console.WriteLine("Reading in the inventory_parts.csv file...");
			CsvInventoryPartMapping csvInventoryPartMapping = new();
			CsvParser<InventoryPart> csvInventoryPartParser = new(GetCsvParserOptions(), csvInventoryPartMapping);
			var parsedInventoryParts = csvInventoryPartParser
					.ReadFromFile(@$"{filePath}inventory_parts.csv", Encoding.ASCII)
					.ToList();

			Console.WriteLine("Cleaning out older versions of inventories...");
			Dictionary<string, Inventory> rawInventories = new();
			foreach (var parsedInventory in parsedInventories)
			{
				if (rawInventories.ContainsKey(parsedInventory.Result.SetNumber))
					if (rawInventories[parsedInventory.Result.SetNumber].Version < parsedInventory.Result.Version)
						rawInventories[parsedInventory.Result.SetNumber].Id = parsedInventory.Result.Id;
					else
						rawInventories.Add(parsedInventory.Result.SetNumber, parsedInventory.Result);
			}






		}


	}
}