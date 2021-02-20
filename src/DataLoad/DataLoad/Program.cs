﻿using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using ShellProgressBar;
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

			const int overallSteps = 6;
			using ProgressBar progressBar = new(overallSteps, "Colors", MainProgressBarOptions());

			Dictionary<string, Color> colors = await ImportColorsAsync(gremlinClient, filePath, progressBar);

			progressBar.Tick("Categories");
			Dictionary<string, Category> categories = await ImportCategoriesAsync(gremlinClient, filePath, progressBar);

			progressBar.Tick("Themes");
			Dictionary<string, Theme> themes = await ImportThemesAsync(gremlinClient, filePath, progressBar);

			progressBar.Tick("Parts");
			Dictionary<string, Part> parts = await ImportPartsAsync(gremlinClient, filePath, categories, progressBar);

			progressBar.Tick("Sets");
			Dictionary<string, Set> sets = await ImportSetsAsync(gremlinClient, filePath, themes, progressBar);

			progressBar.Tick("Inventories");
			ImportSetInventories(gremlinClient, filePath, sets, parts, colors, progressBar);

			progressBar.Tick();

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

		private static async Task<Dictionary<string, Color>> ImportColorsAsync(GremlinClient gremlinClient, string filePath, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing colors.csv", StepProgressBarOptions());

			CsvColorMapping csvMapper = new();
			CsvParser<Color> csvParser = new(GetCsvParserOptions(), csvMapper);
			var parsedResults = csvParser
					.ReadFromFile(@$"{filePath}colors.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Dropping Existing Vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('color').drop()");

			progressBar.Tick("Creating Vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", ProcessProgressBarOptions());
			Dictionary<string, Color> results = new();
			foreach (var parsedResult in parsedResults)
			{
				Color color = parsedResult.Result;
				color.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('color').property('userId', 'catalog').property('id', '{color.Id}').property('name', '{color.Name}').property('rgb', '{color.RGB}').property('isTranslucent', '{color.IsTranslucent}').property('rebrickableId', '{color.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(color.RebrickableId, color);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedResults.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private static async Task<Dictionary<string, Category>> ImportCategoriesAsync(GremlinClient gremlinClient, string filePath, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing part_categories.csv", StepProgressBarOptions());

			CsvCategoryMapping csvMapper = new();
			CsvParser<Category> csvParser = new(GetCsvParserOptions(), csvMapper);
			var parsedResults = csvParser
					.ReadFromFile(@$"{filePath}part_categories.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Dropping existing vertices");
			try
			{
				await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('category').drop()");
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
			}


			progressBar.Tick("Creating vertices");
			Dictionary<string, Category> results = new();
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", ProcessProgressBarOptions());
			foreach (var parsedResult in parsedResults)
			{
				Category category = parsedResult.Result;
				category.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('category').property('id', '{category.Id}').property('userId', 'catalog').property('name', '{category.Name}').property('rebrickableId', '{category.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(category.RebrickableId, category);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedResults.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private static async Task<Dictionary<string, Theme>> ImportThemesAsync(GremlinClient gremlinClient, string filePath, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 5;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing themes.csv", StepProgressBarOptions());

			CsvThemeMapping csvMapper = new();
			CsvParser<Theme> csvParser = new(GetCsvParserOptions(), csvMapper);
			var parsedResults = csvParser
					.ReadFromFile(@$"{filePath}themes.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Formating Theme Data");
			int counter = 0;
			using ChildProgressBar formattingProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", ProcessProgressBarOptions());
			Dictionary<string, Theme> themes = new();
			foreach (var category in parsedResults)
			{
				if (!themes.ContainsKey(category.Result.RebrickableId))
				{
					Theme theme = category.Result;
					theme.Id = Guid.NewGuid().ToString();
					theme.Name = StringCleanup(theme.Name);
					themes.Add(category.Result.RebrickableId, category.Result);
				}
				counter++;
				formattingProgressBar.Tick($"{counter} of {parsedResults.Count}");
			}

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('theme').drop()");

			progressBar.Tick("Creating vertices");
			using ChildProgressBar vertexProgressBar = progressBar.Spawn(themes.Count, $"{counter} of {themes.Count}", ProcessProgressBarOptions());
			counter = 0;
			List<string> edges = new();
			string vertexQuery;
			Dictionary<string, Theme> results = new();
			foreach (Theme theme in themes.Values)
			{
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
				counter++;
				vertexProgressBar.Tick($"{counter} of {themes.Count}");
			}

			progressBar.Tick("Creating edges");
			counter = 0;
			using ChildProgressBar edgeProgressBar = progressBar.Spawn(edges.Count, $"{counter} of {edges.Count}", ProcessProgressBarOptions());
			foreach (string edge in edges)
			{
				SubmitRequest(gremlinClient, edge);
				counter++;
				vertexProgressBar.Tick($"{counter} of {edges.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private static async Task<Dictionary<string, Part>> ImportPartsAsync(GremlinClient gremlinClient, string filePath, Dictionary<string, Category> categories, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing parts.csv", StepProgressBarOptions());

			CsvPartMapping csvMapper = new();
			CsvParser<Part> csvParser = new(GetCsvParserOptions(), csvMapper);
			var parsedResults = csvParser
					.ReadFromFile(@$"{filePath}parts.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('part').drop()");

			progressBar.Tick("Creating vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", ProcessProgressBarOptions());
			StringBuilder query;
			string edge;
			Dictionary<string, Part> results = new();
			foreach (var parsedResult in parsedResults)
			{
				Part part = parsedResult.Result;
				part.Id = Guid.NewGuid().ToString();
				part.Name = StringCleanup(part.Name);
				query = new($"g.addV('part').property('id', '{part.Id}').property('userId', 'catalog').property('name', '{part.Name}').property('partNumber', '{part.PartNumber}').property('categoryId', '{part.CategoryId}')");
				if (categories.TryGetValue(part.CategoryId, out Category category))
				{
					query.Append($".property('categoryName','{category.Name}')");
					edge = $"g.V('{parsedResult.Result.Id}').addE('isOf').to(g.V('{category.Id}'))";
				}
				else
				{
					edge = string.Empty;
				}
				await gremlinClient.SubmitAsync<dynamic>(query.ToString());
				if (!string.IsNullOrWhiteSpace(edge)) await gremlinClient.SubmitAsync<dynamic>(edge);
				results.Add(parsedResult.Result.PartNumber, part);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedResults.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private static async Task<Dictionary<string, Set>> ImportSetsAsync(GremlinClient gremlinClient, string filePath, Dictionary<string, Theme> themes, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing parts.csv", StepProgressBarOptions());

			CsvSetMapping csvMapper = new();
			CsvParser<Set> csvParser = new(GetCsvParserOptions(), csvMapper);
			var parsedResults = csvParser
					.ReadFromFile(@$"{filePath}sets.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('set').drop()");

			Dictionary<string, Set> results = new();
			StringBuilder query;
			string edge;
			int counter = 0;
			using ChildProgressBar childProgressBar = mainProgressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", StepProgressBarOptions());
			foreach (var parsedResult in parsedResults)
			{
				Set set = parsedResult.Result;
				set.Id = Guid.NewGuid().ToString();
				set.Name = StringCleanup(set.Name);
				query = new($"g.addV('set').property('id', '{set.Id}').property('userId', 'catalog').property('name', '{set.Name}').property('setNumber', '{set.SetNumber}').property('year', {set.Year}).property('themeId', '{set.ThemeId}').property('partCount', '{set.PartCount}')");
				if (themes.TryGetValue(set.ThemeId, out Theme theme))
				{
					query.Append($".property('themeName','{theme.Name}')");
					edge = $"g.V('{set.Id}').addE('isOf').to(g.V('{theme.Id}'))";
				}
				else
				{
					edge = string.Empty;
				}
				await gremlinClient.SubmitAsync<dynamic>(query.ToString());
				if (!string.IsNullOrWhiteSpace(edge)) await gremlinClient.SubmitAsync<dynamic>(edge);
				results.Add(parsedResult.Result.SetNumber, set);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedResults.Count}");
			}

			progressBar.Tick();
			return results;
		}

		private static void ImportSetInventories(GremlinClient gremlinClient, string filePath, Dictionary<string, Set> sets, Dictionary<string, Part> parts, Dictionary<string, Color> colors, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 4;
			ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Parsing inventories.csv", StepProgressBarOptions());

			CsvInventoryMapping csvInventoryMapper = new();
			CsvParser<Inventory> csvInventoryParser = new(GetCsvParserOptions(), csvInventoryMapper);
			var parsedInventories = csvInventoryParser
					.ReadFromFile(@$"{filePath}inventories.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Parsing inventories_parts.csv");
			CsvInventoryPartMapping csvInventoryPartMapping = new();
			CsvParser<InventoryPart> csvInventoryPartParser = new(GetCsvParserOptions(), csvInventoryPartMapping);
			var parsedInventoryParts = csvInventoryPartParser
					.ReadFromFile(@$"{filePath}inventory_parts.csv", Encoding.ASCII)
					.ToList();

			progressBar.Tick("Removing older versions of inventories");
			int counter = 0;
			using ChildProgressBar cleanupProgressBar = mainProgressBar.Spawn(parsedInventories.Count, $"{counter} of {parsedInventories.Count}", StepProgressBarOptions());
			Dictionary<string, Inventory> inventories = new();
			foreach (var parsedInventory in parsedInventories)
			{
				if (inventories.ContainsKey(parsedInventory.Result.SetNumber))
					if (inventories[parsedInventory.Result.SetNumber].Version < parsedInventory.Result.Version)
						inventories[parsedInventory.Result.SetNumber].Id = parsedInventory.Result.Id;
					else
						inventories.Add(parsedInventory.Result.SetNumber, parsedInventory.Result);
				counter++;
				cleanupProgressBar.Tick($"{counter} of {parsedInventories.Count}");
			}

			progressBar.Tick("Creating edges");
			counter = 0;
			using ChildProgressBar edgeProgressBar = mainProgressBar.Spawn(parsedInventoryParts.Count, $"{counter} of {parsedInventoryParts.Count}", StepProgressBarOptions());
			foreach (var parsedInventoryPart in parsedInventoryParts)
			{
				InventoryPart inventoryPart = parsedInventoryPart.Result;
				if (inventories.TryGetValue(inventoryPart.InventoryId, out Inventory inventory))
					if (parts.TryGetValue(inventoryPart.PartNumber, out Part part))
						if (sets.TryGetValue(inventory.SetNumber, out Set set))
						{
							StringBuilder edgeGremlin = new($"g.V('{set.Id}').addE('has').to(g.V('{part.Id}')).property('colorRebrickableId', '{inventoryPart.ColorId}').property('isSpare', '{inventoryPart.IsSpare}')");
							if (colors.TryGetValue(inventoryPart.ColorId, out Color color))
								edgeGremlin.Append($".property('colorId', '{color.Id}').property('colorName', '{color.Name}')");
							SubmitRequest(gremlinClient, edgeGremlin.ToString());
						}
				counter++;
				edgeProgressBar.Tick($"{counter} of {parsedInventoryParts.Count}");
			}

			progressBar.Tick();

		}

		private static ProgressBarOptions MainProgressBarOptions()
		{
			return new ProgressBarOptions()
			{
				ForegroundColor = ConsoleColor.Yellow,
				BackgroundColor = ConsoleColor.DarkYellow,
				ProgressCharacter = '─'
			};
		}

		private static ProgressBarOptions StepProgressBarOptions()
		{
			return new ProgressBarOptions()
			{
				ForegroundColor = ConsoleColor.Green,
				BackgroundColor = ConsoleColor.DarkGreen,
				ProgressCharacter = '─'
			};
		}

		private static ProgressBarOptions ProcessProgressBarOptions()
		{
			return new ProgressBarOptions()
			{
				ForegroundColor = ConsoleColor.Cyan,
				BackgroundColor = ConsoleColor.DarkCyan,
				ProgressCharacter = '─'
			};
		}

		private static string StringCleanup(string input)
		{
			string output = input.Replace("\\", "\\\\");
			output = output.Replace("\'", "\\\'");
			return output;
		}

	}

}