using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using ICSharpCode.SharpZipLib.GZip;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
{

	public class LoadData
	{

		private readonly HttpClient _httpClient = new();
		private readonly ProgressBar _progressBar;

		private const string _colorUrl = "https://cdn.rebrickable.com/media/downloads/colors.csv.gz";
		private const string _categoryUrl = "https://cdn.rebrickable.com/media/downloads/part_categories.csv.gz";
		private const string _themesUrl = "https://cdn.rebrickable.com/media/downloads/themes.csv.gz";
		private const string _partsUrl = "https://cdn.rebrickable.com/media/downloads/parts.csv.gz";
		private const string _setsUrl = "https://cdn.rebrickable.com/media/downloads/sets.csv.gz";
		private const string _inventoriesUrl = "https://cdn.rebrickable.com/media/downloads/inventories.csv.gz";
		private const string _inventoryPartsUrl = "https://cdn.rebrickable.com/media/downloads/inventory_parts.csv.gz";

		public LoadData(ProgressBar progressBar)
		{
			_progressBar = progressBar;
		}

		public async Task BulkImportAsync()
		{

			#region Create GremlinClient

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

			#endregion

			// Colors
			Stream colorStream = await DownloadFileAsStream(_colorUrl);
			List<CsvMappingResult<Color>> parsedColors = (List<CsvMappingResult<Color>>)ExtractAndParseStream<Color, ColorMapper>(colorStream);
			Dictionary<string, Color> colors = await ImportColorsAsync(gremlinClient, parsedColors);

			// Categories
			_progressBar.Tick("Categories");
			Stream categoryStream = await DownloadFileAsStream(_categoryUrl);
			List<CsvMappingResult<Category>> parsedCategories = (List<CsvMappingResult<Category>>)ExtractAndParseStream<Category, CategoryMapper>(categoryStream);
			Dictionary<string, Category> categories = await ImportCategoriesAsync(gremlinClient, parsedCategories);

			// Themes
			_progressBar.Tick("Themes");
			Stream themeStream = await DownloadFileAsStream(_themesUrl);
			List<CsvMappingResult<Theme>> parsedThemes = (List<CsvMappingResult<Theme>>)ExtractAndParseStream<Theme, ThemeMapper>(themeStream);
			Dictionary<string, Theme> themes = await ImportThemesAsync(gremlinClient, parsedThemes);

			// Parts
			_progressBar.Tick("Parts");
			Stream partsStream = await DownloadFileAsStream(_partsUrl);
			List<CsvMappingResult<Part>> parsedParts = (List<CsvMappingResult<Part>>)ExtractAndParseStream<Part, PartMapper>(partsStream);
			Dictionary<string, Part> parts = await ImportPartsAsync(gremlinClient, parsedParts, categories);

			// Sets
			_progressBar.Tick("Sets");
			Stream setsStream = await DownloadFileAsStream(_setsUrl);
			List<CsvMappingResult<Set>> parsedSets = (List<CsvMappingResult<Set>>)ExtractAndParseStream<Set, SetMapper>(setsStream);
			Dictionary<string, Set> sets = await ImportSetsAsync(gremlinClient, parsedSets, themes);

			// Set Inventories
			_progressBar.Tick("Set Inventories");
			Stream inventoriesStream = await DownloadFileAsStream(_inventoriesUrl);
			List<CsvMappingResult<Inventory>> parsedInventories = (List<CsvMappingResult<Inventory>>)ExtractAndParseStream<Inventory, InventoryMapper>(inventoriesStream);
			Stream inventoryPartsStream = await DownloadFileAsStream(_inventoryPartsUrl);
			List<CsvMappingResult<InventoryPart>> parsedInventoryParts = (List<CsvMappingResult<InventoryPart>>)ExtractAndParseStream<InventoryPart, InventoryPartMapper>(inventoryPartsStream);
			ImportSetInventories(gremlinClient, parsedInventories, parsedInventoryParts, sets, parts, colors);

		}

		private async Task<Stream> DownloadFileAsStream(string url)
		{
			try
			{
				HttpResponseMessage responseMessage = await _httpClient.GetAsync(url);
				responseMessage.EnsureSuccessStatusCode();
				return await responseMessage.Content.ReadAsStreamAsync();
			}
			catch (HttpRequestException)
			{
				return null;
			}
		}

		private static IList<CsvMappingResult<TEntity>> ExtractAndParseStream<TEntity, TMapper>(Stream stream) where TMapper : ICsvMapping<TEntity>, new()
		{
			using GZipInputStream gzipStream = new(stream);
			TMapper mapper = new();
			CsvParser<TEntity> csvParser = new(GetCsvParserOptions(), mapper);
			return csvParser
				.ReadFromStream(gzipStream, Encoding.ASCII)
				.ToList();
		}

		private static CsvParserOptions GetCsvParserOptions()
		{
			return new CsvParserOptions(true, ',');
		}

		private async Task<Dictionary<string, Color>> ImportColorsAsync(GremlinClient gremlinClient, IList<CsvMappingResult<Color>> colors)
		{

			const int currentStepProcesses = 2;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Dropping Existing Vertices", StepProgressBarOptions());

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('color').drop()");

			progressBar.Tick("Creating Vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(colors.Count, $"{counter} of {colors.Count}", ProcessProgressBarOptions());
			Dictionary<string, Color> results = new();
			foreach (var parsedResult in colors)
			{
				Color color = parsedResult.Result;
				color.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('color').property('userId', 'catalog').property('id', '{color.Id}').property('name', '{StringCleanup(color.Name)}').property('rgb', '{StringCleanup(color.RGB)}').property('isTranslucent', '{color.IsTranslucent}').property('rebrickableId', '{color.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(color.RebrickableId, color);
				counter++;
				childProgressBar.Tick($"{counter} of {colors.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private async Task<Dictionary<string, Category>> ImportCategoriesAsync(GremlinClient gremlinClient, IList<CsvMappingResult<Category>> categories)
		{

			const int currentStepProcesses = 2;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Dropping existing vertices", StepProgressBarOptions());

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('category').drop()");

			progressBar.Tick("Creating vertices");
			Dictionary<string, Category> results = new();
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(categories.Count, $"{counter} of {categories.Count}", ProcessProgressBarOptions());
			foreach (var parsedResult in categories)
			{
				Category category = parsedResult.Result;
				category.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('category').property('id', '{category.Id}').property('userId', 'catalog').property('name', '{StringCleanup(category.Name)}').property('rebrickableId', '{category.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				results.Add(category.RebrickableId, category);
				counter++;
				childProgressBar.Tick($"{counter} of {categories.Count}");
			}

			progressBar.Tick();
			return results;

		}

		private async Task<Dictionary<string, Theme>> ImportThemesAsync(GremlinClient gremlinClient, IList<CsvMappingResult<Theme>> parsedResults)
		{

			const int currentStepProcesses = 5;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Formating Theme Data", StepProgressBarOptions());

			int counter = 0;
			using ChildProgressBar formattingProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", ProcessProgressBarOptions());
			Dictionary<string, Theme> themes = new();
			foreach (var parsedTheme in parsedResults)
			{
				if (!themes.ContainsKey(parsedTheme.Result.RebrickableId))
				{
					Theme theme = parsedTheme.Result;
					theme.Id = Guid.NewGuid().ToString();
					theme.Name = StringCleanup(theme.Name);
					themes.Add(parsedTheme.Result.RebrickableId, parsedTheme.Result);
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

		private async Task<Dictionary<string, Part>> ImportPartsAsync(GremlinClient gremlinClient, IList<CsvMappingResult<Part>> parsedResults, Dictionary<string, Category> categories)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Dropping existing vertices", StepProgressBarOptions());

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

		private async Task<Dictionary<string, Set>> ImportSetsAsync(GremlinClient gremlinClient, IList<CsvMappingResult<Set>> parsedResults, Dictionary<string, Theme> themes)
		{

			const int currentStepProcesses = 3;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Dropping existing vertices", StepProgressBarOptions());

			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('set').drop()");

			Dictionary<string, Set> results = new();
			StringBuilder query;
			string edge;
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedResults.Count, $"{counter} of {parsedResults.Count}", StepProgressBarOptions());
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

		private void ImportSetInventories(GremlinClient gremlinClient, IList<CsvMappingResult<Inventory>> parsedInventories, IList<CsvMappingResult<InventoryPart>> parsedInventoryParts, Dictionary<string, Set> sets, Dictionary<string, Part> parts, Dictionary<string, Color> colors)
		{

			const int currentStepProcesses = 4;
			ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Removing older versions of inventories", StepProgressBarOptions());

			int counter = 0;
			using ChildProgressBar cleanupProgressBar = progressBar.Spawn(parsedInventories.Count, $"{counter} of {parsedInventories.Count}", StepProgressBarOptions());
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
			using ChildProgressBar edgeProgressBar = progressBar.Spawn(parsedInventoryParts.Count, $"{counter} of {parsedInventoryParts.Count}", StepProgressBarOptions());
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