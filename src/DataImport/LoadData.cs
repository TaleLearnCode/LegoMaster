using Azure.Data.Tables;
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
using TaleLearnCode.LEGOMaster.DataImport.TableEntities;
using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
{

	public class LoadData
	{

		private readonly HttpClient _httpClient = new();
		private readonly ProgressBar _progressBar;
		private readonly RebrickableUrls _rebrickableUrls;
		private readonly CosmosDBSettings _cosmosDbSettings;
		private readonly AzureStorageSettings _azureStorageSettings;
		private TableClient _tableClient;

		private readonly Dictionary<string, Color> _colors = new();
		private readonly Dictionary<string, Category> _categories = new();
		private readonly Dictionary<string, Theme> _themes = new();
		private readonly Dictionary<string, Part> _parts = new();
		private readonly Dictionary<string, Set> _sets = new();

		public LoadData(
			ProgressBar progressBar,
			RebrickableUrls rebrickableUrls,
			CosmosDBSettings cosmosDBSettings,
			AzureStorageSettings azureStorageSettings)
		{
			_progressBar = progressBar;
			_rebrickableUrls = rebrickableUrls;
			_cosmosDbSettings = cosmosDBSettings;
			_azureStorageSettings = azureStorageSettings;
		}

		#region Bulk Import

		public async Task BulkImportAsync()
		{

			#region Create GremlinClient

			string containerLink = $"/dbs/{_cosmosDbSettings.DatabaseName}/colls/{_cosmosDbSettings.ContainerName}";
			Console.WriteLine($"Connecting to: host: {_cosmosDbSettings.Host}, port: {_cosmosDbSettings.Port}, container: {containerLink}, ssl: {_cosmosDbSettings.EnableSSL}");
			var gremlinServer = new GremlinServer(_cosmosDbSettings.Host, _cosmosDbSettings.Port, enableSsl: _cosmosDbSettings.EnableSSL,
																							username: containerLink,
																							password: _cosmosDbSettings.AccountKey);

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

			_tableClient = GetTableClient();

			await ImportColorsAsync(gremlinClient);
			_progressBar.Tick("Categories");
			await ImportCategoriesAsync(gremlinClient);

			// Themes
			_progressBar.Tick("Themes");
			await ImportThemesAsync(gremlinClient);

			// Parts
			_progressBar.Tick("Parts");
			await ImportPartsAsync(gremlinClient);

			// Sets
			_progressBar.Tick("Sets");
			await ImportSetsAsync(gremlinClient);

			// Set Inventories
			_progressBar.Tick("Set Inventories");
			await ImportSetInventoriesAsync(gremlinClient);

		}

		private async Task ImportColorsAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 4;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Downloading Rebrickable file", StepProgressBarOptions());

			Stream colorStream = await DownloadFileAsStream(_rebrickableUrls.ColorsUrl);

			progressBar.Tick("Extracting and parsing Rebrickable file");
			List<CsvMappingResult<Color>> parsedColors = (List<CsvMappingResult<Color>>)ExtractAndParseStream<Color, ColorMapper>(colorStream);

			progressBar.Tick("Dropping Existing Vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('color').drop()");
			List<ColorTableEntity> colorTableEntities = _tableClient.Query<ColorTableEntity>(t => t.PartitionKey == "color").ToList();
			foreach (ColorTableEntity colorTableEntity in colorTableEntities)
			{
				_tableClient.DeleteEntity(colorTableEntity.PartitionKey, colorTableEntity.RowKey);
			}

			progressBar.Tick("Creating Vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedColors.Count, $"{counter} of {parsedColors.Count}", ProcessProgressBarOptions());
			foreach (var parsedResult in parsedColors)
			{
				Color color = parsedResult.Result;
				color.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('color').property('userId', 'catalog').property('id', '{color.Id}').property('name', '{StringCleanup(color.Name)}').property('rgb', '{StringCleanup(color.RGB)}').property('isTranslucent', '{color.IsTranslucent}').property('rebrickableId', '{color.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				_tableClient.AddEntity<ColorTableEntity>(new(color));
				_colors.Add(color.RebrickableId, color);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedColors.Count}");
			}

			progressBar.Tick();

		}

		private async Task ImportCategoriesAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 4;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Downloading Rebrickable file", StepProgressBarOptions());

			Stream categoryStream = await DownloadFileAsStream(_rebrickableUrls.CategoriesUrl);
			progressBar.Tick("Extracting and parsing Rebrickable file");
			List<CsvMappingResult<Category>> parsedCategories = (List<CsvMappingResult<Category>>)ExtractAndParseStream<Category, CategoryMapper>(categoryStream);

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('category').drop()");

			progressBar.Tick("Creating vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedCategories.Count, $"{counter} of {parsedCategories.Count}", ProcessProgressBarOptions());
			foreach (var parsedResult in parsedCategories)
			{
				Category category = parsedResult.Result;
				category.Id = Guid.NewGuid().ToString();
				string query = $"g.addV('category').property('id', '{category.Id}').property('userId', 'catalog').property('name', '{StringCleanup(category.Name)}').property('rebrickableId', '{category.RebrickableId}')";
				SubmitRequest(gremlinClient, query);
				_tableClient.AddEntity<CategoryTableEntity>(new(category));
				_categories.Add(category.RebrickableId, category);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedCategories.Count}");
			}

			progressBar.Tick();

		}

		private async Task ImportThemesAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 7;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Downloading Rebrickable file", StepProgressBarOptions());

			Stream themeStream = await DownloadFileAsStream(_rebrickableUrls.ThemesUrl);

			progressBar.Tick("Extracting and parsing Rebrickable file");
			List<CsvMappingResult<Theme>> parsedThemes = (List<CsvMappingResult<Theme>>)ExtractAndParseStream<Theme, ThemeMapper>(themeStream);

			progressBar.Tick("Formating Theme Data");
			int counter = 0;
			using ChildProgressBar formattingProgressBar = progressBar.Spawn(parsedThemes.Count, $"{counter} of {parsedThemes.Count}", ProcessProgressBarOptions());
			Dictionary<string, Theme> themes = new();
			foreach (var parsedTheme in parsedThemes)
			{
				if (!themes.ContainsKey(parsedTheme.Result.RebrickableId))
				{
					Theme theme = parsedTheme.Result;
					theme.Id = Guid.NewGuid().ToString();
					theme.Name = StringCleanup(theme.Name);
					themes.Add(parsedTheme.Result.RebrickableId, parsedTheme.Result);
				}
				counter++;
				formattingProgressBar.Tick($"{counter} of {parsedThemes.Count}");
			}

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('theme').drop()");

			progressBar.Tick("Creating vertices");
			using ChildProgressBar vertexProgressBar = progressBar.Spawn(themes.Count, $"{counter} of {themes.Count}", ProcessProgressBarOptions());
			counter = 0;
			List<string> edges = new();
			string vertexQuery;
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

				_tableClient.AddEntity<ThemeTableEntity>(new(theme));
				_themes.Add(theme.RebrickableId, theme);
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

		}

		private async Task ImportPartsAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 5;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Downloading Rebrickable file", StepProgressBarOptions());

			Stream partsStream = await DownloadFileAsStream(_rebrickableUrls.PartsUrl);

			progressBar.Tick("Extracting and parsing Rebrickable file");
			List<CsvMappingResult<Part>> parsedParts = (List<CsvMappingResult<Part>>)ExtractAndParseStream<Part, PartMapper>(partsStream);

			progressBar.Tick("Dropping existing vertices");
			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('part').drop()");

			progressBar.Tick("Creating vertices");
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedParts.Count, $"{counter} of {parsedParts.Count}", ProcessProgressBarOptions());
			StringBuilder query;
			string edge;
			foreach (var parsedResult in parsedParts)
			{
				Part part = parsedResult.Result;
				part.Id = Guid.NewGuid().ToString();
				part.Name = StringCleanup(part.Name);
				query = new($"g.addV('part').property('id', '{part.Id}').property('userId', 'catalog').property('name', '{part.Name}').property('partNumber', '{part.PartNumber}').property('categoryId', '{part.CategoryId}')");
				if (_categories.TryGetValue(part.CategoryId, out Category category))
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
				_tableClient.AddEntity<PartTableEntity>(new(part));
				_parts.Add(parsedResult.Result.PartNumber, part);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedParts.Count}");
			}

			progressBar.Tick();

		}

		private async Task ImportSetsAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 5;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Dropping existing vertices", StepProgressBarOptions());

			Stream setsStream = await DownloadFileAsStream(_rebrickableUrls.SetsUrl);
			List<CsvMappingResult<Set>> parsedSets = (List<CsvMappingResult<Set>>)ExtractAndParseStream<Set, SetMapper>(setsStream);


			await gremlinClient.SubmitAsync<dynamic>("g.V().has('userId', 'catalog').hasLabel('set').drop()");

			StringBuilder query;
			string edge;
			int counter = 0;
			using ChildProgressBar childProgressBar = progressBar.Spawn(parsedSets.Count, $"{counter} of {parsedSets.Count}", StepProgressBarOptions());
			foreach (var parsedResult in parsedSets)
			{
				Set set = parsedResult.Result;
				set.Id = Guid.NewGuid().ToString();
				set.Name = StringCleanup(set.Name);
				query = new($"g.addV('set').property('id', '{set.Id}').property('userId', 'catalog').property('name', '{set.Name}').property('setNumber', '{set.SetNumber}').property('year', {set.Year}).property('themeId', '{set.ThemeId}').property('partCount', '{set.PartCount}')");
				if (_themes.TryGetValue(set.ThemeId, out Theme theme))
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
				_tableClient.AddEntity<SetTableEntity>(new(set));
				_sets.Add(parsedResult.Result.SetNumber, set);
				counter++;
				childProgressBar.Tick($"{counter} of {parsedSets.Count}");
			}

			progressBar.Tick();

		}

		private async Task ImportSetInventoriesAsync(GremlinClient gremlinClient)
		{

			const int currentStepProcesses = 6;
			using ChildProgressBar progressBar = _progressBar.Spawn(currentStepProcesses, "Downloading Rebrickable inventories file", StepProgressBarOptions());

			Stream inventoriesStream = await DownloadFileAsStream(_rebrickableUrls.InventoriesUrl);

			progressBar.Tick("Extracting and parsing Rebrickable inventories file");
			List<CsvMappingResult<Inventory>> parsedInventories = (List<CsvMappingResult<Inventory>>)ExtractAndParseStream<Inventory, InventoryMapper>(inventoriesStream);

			progressBar.Tick("Downloading Rebrickable inventory parts file");
			Stream inventoryPartsStream = await DownloadFileAsStream(_rebrickableUrls.InventoryPartsUrl);

			progressBar.Tick("Extracting and parsing Rebrickable inventory parts file");
			List<CsvMappingResult<InventoryPart>> parsedInventoryParts = (List<CsvMappingResult<InventoryPart>>)ExtractAndParseStream<InventoryPart, InventoryPartMapper>(inventoryPartsStream);

			progressBar.Tick("Building inventory list");
			int buildInventoryListCounter = 1;
			using ChildProgressBar buildInventoryListProgress = progressBar.Spawn(parsedInventories.Count, $"Inventory {buildInventoryListCounter} of {parsedInventories.Count}", ProcessProgressBarOptions());
			Dictionary<string, Inventory> inventories = new();
			foreach (var parsedInventory in parsedInventories)
			{
				if (inventories.TryGetValue(parsedInventory.Result.Id, out Inventory inventory))
					if (inventory.Version < parsedInventory.Result.Version)
						inventories.Remove(inventory.Id);
				inventories.Add(parsedInventory.Result.Id, parsedInventory.Result);
				buildInventoryListCounter++;
				buildInventoryListProgress.Tick($"Inventory {buildInventoryListCounter} of {parsedInventories.Count}");
			}
			progressBar.Tick();

			int createEdgeCounter = 1;
			using ChildProgressBar createEdgesProgress = progressBar.Spawn(parsedInventoryParts.Count, $"Inventory Part {createEdgeCounter} of {parsedInventoryParts.Count}", ProcessProgressBarOptions());
			foreach (var parsedInventoryPart in parsedInventoryParts)
			{
				InventoryPart inventoryPart = parsedInventoryPart.Result;
				if (inventories.TryGetValue(inventoryPart.InventoryId, out Inventory inventory))
				{
					if (_parts.TryGetValue(inventoryPart.PartNumber, out Part part))
					{
						if (_sets.TryGetValue(inventory.SetNumber, out Set set))
						{
							StringBuilder edgeGremlin = new($"g.V('{set.Id}').addE('has').to(g.V('{part.Id}')).property('colorRebrickableId', '{inventoryPart.ColorId}').property('isSpare', '{inventoryPart.IsSpare}')");
							if (_colors.TryGetValue(inventoryPart.ColorId, out Color color))
								edgeGremlin.Append($".property('colorId', '{color.Id}').property('colorName', '{color.Name}')");
							SubmitRequest(gremlinClient, edgeGremlin.ToString());
						}
					}
				}
				createEdgeCounter++;
				createEdgesProgress.Tick($"Inventory Part {createEdgeCounter} of {parsedInventoryParts.Count}");
			}
			progressBar.Tick();

		}

		#endregion

		#region Daily Update

		public async Task DailyUpdateAsync()
		{

			const int progressSteps = 6;
			using ProgressBar progressBar = new(progressSteps, "Connecting to Cosmos", MainProgressBarOptions());

			#region Create GremlinClient

			string containerLink = $"/dbs/{_cosmosDbSettings.DatabaseName}/colls/{_cosmosDbSettings.ContainerName}";
			Console.WriteLine($"Connecting to: host: {_cosmosDbSettings.Host}, port: {_cosmosDbSettings.Port}, container: {containerLink}, ssl: {_cosmosDbSettings.EnableSSL}");
			var gremlinServer = new GremlinServer(_cosmosDbSettings.Host, _cosmosDbSettings.Port, enableSsl: _cosmosDbSettings.EnableSSL,
																							username: containerLink,
																							password: _cosmosDbSettings.AccountKey);

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

			progressBar.Tick("Connecting to Azure Storage");
			_tableClient = GetTableClient();

			progressBar.Tick("Processing color updates");
			List<Color> rebrickableColors = await GetRebrickableColors();
			Dictionary<string, Color> colors = new();
			using ChildProgressBar colorProgressBar = progressBar.Spawn(rebrickableColors.Count, $"Processing color {colors.Count} of {rebrickableColors.Count}", StepProgressBarOptions());
			foreach (Color rebrickableColor in rebrickableColors)
			{
				Color processedColor = UpdateColor(gremlinClient, rebrickableColor);
				colors.Add(processedColor.RebrickableId, processedColor);
				colorProgressBar.Tick($"Processing color {colors.Count} of {rebrickableColors.Count}");
			}

			progressBar.Tick("Processing category updates");
			List<Category> rebrickableCategories = await GetRebrickableCategories();
			Dictionary<string, Category> categories = new();
			using ChildProgressBar categoryProgressBar = progressBar.Spawn(rebrickableCategories.Count, $"Processing category {categories.Count} of {rebrickableCategories.Count}", StepProgressBarOptions());
			foreach (Category rebrickableCategory in rebrickableCategories)
			{
				Category processedCategory = UpdateCategory(gremlinClient, rebrickableCategory);
				categories.Add(processedCategory.RebrickableId, processedCategory);
				categoryProgressBar.Tick($"Processing category {categories.Count} of {rebrickableCategories.Count}");
			}

			progressBar.Tick("Processing theme updates");
			Dictionary<string, Theme> rebrickableThemes = await GetRebrickableThemes();
			Dictionary<string, Theme> themes = new();
			using ChildProgressBar themeProgressBar = progressBar.Spawn(rebrickableThemes.Count, $"Processing theme {themes.Count} of {rebrickableThemes.Count}", StepProgressBarOptions());
			foreach (Theme rebrickableTheme in rebrickableThemes.Values)
			{
				Theme processedTheme = await UpdateThemeAsync(gremlinClient, rebrickableTheme, rebrickableThemes);
				themes.Add(processedTheme.RebrickableId, processedTheme);
				themeProgressBar.Tick($"Processing theme {themes.Count} of {rebrickableThemes.Count}");
			}

			progressBar.Tick("Processing part updates");
			List<Part> rebrickableParts = await GetRebrickableParts();

		}

		public async Task<List<Color>> GetRebrickableColors()
		{
			Stream colorStream = await DownloadFileAsStream(_rebrickableUrls.ColorsUrl);
			List<CsvMappingResult<Color>> parsedColors = (List<CsvMappingResult<Color>>)ExtractAndParseStream<Color, ColorMapper>(colorStream);
			List<Color> colors = new();
			foreach (var parsedColor in parsedColors)
				colors.Add(parsedColor.Result);
			return colors;
		}

		public Color UpdateColor(GremlinClient gremlinClient, Color color)
		{

			int updatedRecords = 0;
			string query = string.Empty;
			ColorTableEntity colorTableEntity = _tableClient.Query<ColorTableEntity>(t => t.PartitionKey == "color" && t.RowKey == color.RebrickableId).FirstOrDefault();
			if (colorTableEntity == null)
			{
				color.Id = Guid.NewGuid().ToString();
				query = $"g.addV('color').property('userId', 'catalog').property('id', '{color.Id}').property('name', '{StringCleanup(color.Name)}').property('rgb', '{StringCleanup(color.RGB)}').property('isTranslucent', '{color.IsTranslucent}').property('rebrickableId', '{color.RebrickableId}')";
			}
			else if (color.Name != colorTableEntity.Name || color.RGB != colorTableEntity.RGB || color.IsTranslucent != colorTableEntity.IsTranslucent)
			{
				color.Id = colorTableEntity.Id;
				query = $"g.V('{colorTableEntity.Id}').property('userId', 'catalog').property('id', '{color.Id}').property('name', '{StringCleanup(color.Name)}').property('rgb', '{StringCleanup(color.RGB)}').property('isTranslucent', '{color.IsTranslucent}').property('rebrickableId', '{color.RebrickableId}')";
			}
			if (!string.IsNullOrWhiteSpace(query))
			{
				updatedRecords++;
				SubmitRequest(gremlinClient, query);
				_tableClient.UpsertEntity<ColorTableEntity>(new(color));
			}
			return color;
		}

		public async Task<List<Category>> GetRebrickableCategories()
		{
			Stream categoriesStream = await DownloadFileAsStream(_rebrickableUrls.CategoriesUrl);
			List<CsvMappingResult<Category>> parsedCategories = (List<CsvMappingResult<Category>>)ExtractAndParseStream<Category, CategoryMapper>(categoriesStream);
			List<Category> categories = new();
			foreach (var parsedColor in parsedCategories)
				categories.Add(parsedColor.Result);
			return categories;
		}

		public Category UpdateCategory(GremlinClient gremlinClient, Category category)
		{

			string query = string.Empty;
			CategoryTableEntity categoryTableEntity = _tableClient.Query<CategoryTableEntity>(t => t.PartitionKey == "category" && t.RowKey == category.RebrickableId).FirstOrDefault();

			if (categoryTableEntity == null)
			{
				category.Id = Guid.NewGuid().ToString();
				query = $"g.addV('category').property('id', '{category.Id}').property('userId', 'catalog').property('name', '{StringCleanup(category.Name)}').property('rebrickableId', '{category.RebrickableId}')";
			}
			else
			{
				category.Id = categoryTableEntity.Id;
				if (!categoryTableEntity.EqualsCategory(category))
					query = $"g.V('{category.Id}').property('id', '{category.Id}').property('userId', 'catalog').property('name', '{StringCleanup(category.Name)}').property('rebrickableId', '{category.RebrickableId}')";
			}

			if (!string.IsNullOrWhiteSpace(query))
			{
				SubmitRequest(gremlinClient, query);
				_tableClient.UpsertEntity<CategoryTableEntity>(new(category));
			}

			return category;

		}

		public async Task<Dictionary<string, Theme>> GetRebrickableThemes()
		{
			Stream themesStream = await DownloadFileAsStream(_rebrickableUrls.CategoriesUrl);
			List<CsvMappingResult<Theme>> parsedThemes = (List<CsvMappingResult<Theme>>)ExtractAndParseStream<Theme, ThemeMapper>(themesStream);

			Dictionary<string, Theme> themes = new();
			foreach (var parsedTheme in parsedThemes)
			{
				if (!themes.ContainsKey(parsedTheme.Result.RebrickableId))
				{
					Theme theme = parsedTheme.Result;
					theme.Id = Guid.NewGuid().ToString();
					theme.Name = StringCleanup(theme.Name);
					themes.Add(parsedTheme.Result.RebrickableId, parsedTheme.Result);
				}
			}

			return themes;

		}

		public async Task<Theme> UpdateThemeAsync(GremlinClient gremlinClient, Theme theme, Dictionary<string, Theme> themes)
		{

			string query = string.Empty;
			List<string> edges = new();
			(string query, List<string> edges) gremlin = new();
			ThemeTableEntity themeTableEntity = _tableClient.Query<ThemeTableEntity>(t => t.PartitionKey == "theme" && t.RowKey == theme.RebrickableId).FirstOrDefault();

			if (themeTableEntity == null)
			{
				theme.Id = Guid.NewGuid().ToString();
				gremlin = BuildThemeGremlin(theme, themes);
			}
			else
			{
				theme.Id = themeTableEntity.Id;
				if (!themeTableEntity.EqualsTheme(theme))
					gremlin = BuildThemeGremlin(theme, themes);
			}

			if (!string.IsNullOrWhiteSpace(gremlin.query))
			{
				await gremlinClient.SubmitAsync<dynamic>(gremlin.query);
				foreach (string edge in gremlin.edges)
					SubmitRequest(gremlinClient, edge);
				_tableClient.UpsertEntity<ThemeTableEntity>(new(theme));
			}

			return theme;

		}

		private (string query, List<string> edges) BuildThemeGremlin(Theme theme, Dictionary<string, Theme> themes)
		{

			string query = string.Empty;
			List<string> edges = new();

			if (string.IsNullOrWhiteSpace(theme.ParentId))
			{
				query = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
			}
			else
			{
				if (themes.ContainsKey(theme.ParentId))
				{
					query = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}').property('parentId', 'theme_{theme.ParentId}').property('parentName', '{themes[theme.ParentId]}')";
					edges.Add($"g.V('{theme.Id}').addE('isChildOf').to(g.V('{themes[theme.ParentId].Id}'))");
				}
				else
				{
					query = $"g.addV('theme').property('userId', 'catalog').property('id', '{theme.Id}').property('name', '{theme.Name}').property('rebrickableId', '{theme.RebrickableId}')";
				}
			}

			return (query, edges);

		}

		public async Task<List<Part>> GetRebrickableParts()
		{
			Stream partsStream = await DownloadFileAsStream(_rebrickableUrls.PartsUrl);
			List<CsvMappingResult<Part>> parsedParts = (List<CsvMappingResult<Part>>)ExtractAndParseStream<Part, PartMapper>(partsStream);
			List<Part> parts = new();
			foreach (var parsedPart in parsedParts)
				parts.Add(parsedPart.Result);
			return parts;
		}

		public async Task<Part> UpdatePartAsync(GremlinClient gremlinClient, Part part, Dictionary<string, Category> categories)
		{

			StringBuilder query = new();
			string edge = string.Empty;
			PartTableEntity partTableEntity = _tableClient.Query<PartTableEntity>(t => t.PartitionKey == "part" && t.RowKey == part.PartNumber).FirstOrDefault();

			if (partTableEntity == null)
			{
				part.Id = Guid.NewGuid().ToString();
				query = new($"g.addV('part').property('id', '{part.Id}').property('userId', 'catalog').property('name', '{part.Name}').property('partNumber', '{part.PartNumber}').property('categoryId', '{part.CategoryId}').property('propertyMaterial', '{part.PartMaterial}')");
				if (categories.TryGetValue(part.CategoryId, out Category category))
				{
					query.Append($".property('categoryName','{category.Name}')");
					edge = $"g.V('{part.Id}').addE('isOf').to(g.V('{category.Id}'))";
				}
				else
				{
					edge = string.Empty;
				}

			}
			else
			{
				part.Id = partTableEntity.Id;
				if (!partTableEntity.EqualsPart(part))
					query.Append($"g.V('{part.Id}').property('id', '{part.Id}').property('userId', 'catalog').property('name', '{StringCleanup(part.Name)}').property('partNumber', '{part.PartNumber}').property('categoryId', '{part.CategoryId}').property('propertyMaterial', '{part.PartMaterial}')");
			}

			if (query.Length > 0)
			{
				await gremlinClient.SubmitAsync<dynamic>(query.ToString());
				_tableClient.UpsertEntity<PartTableEntity>(new(part));
				if (!string.IsNullOrWhiteSpace(edge)) await gremlinClient.SubmitAsync<dynamic>(edge);

			}

			return part;

		}


		#endregion

		#region Private Utility Methods

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

		private TableClient GetTableClient()
		{
			return new TableClient(
				new Uri(_azureStorageSettings.Url),
				_azureStorageSettings.RebrickableTableName,
				new TableSharedKeyCredential(_azureStorageSettings.AccountName, _azureStorageSettings.AccountKey));
		}

		#endregion

	}

}