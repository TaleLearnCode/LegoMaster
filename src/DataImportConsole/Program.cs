using ShellProgressBar;
using System;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.DataImport;

namespace TaleLearnCode.LEGOMaster.DataImportConsole
{
	class Program
	{

		private const string _colorUrl = "https://cdn.rebrickable.com/media/downloads/colors.csv.gz";
		private const string _categoryUrl = "https://cdn.rebrickable.com/media/downloads/part_categories.csv.gz";
		private const string _themesUrl = "https://cdn.rebrickable.com/media/downloads/themes.csv.gz";
		private const string _partsUrl = "https://cdn.rebrickable.com/media/downloads/parts.csv.gz";
		private const string _setsUrl = "https://cdn.rebrickable.com/media/downloads/sets.csv.gz";
		private const string _inventoriesUrl = "https://cdn.rebrickable.com/media/downloads/inventories.csv.gz";
		private const string _inventoryPartsUrl = "https://cdn.rebrickable.com/media/downloads/inventory_parts.csv.gz";

		static async Task Main()
		{

			RebrickableUrls rebrickableUrls = new()
			{
				ColorsUrl = _colorUrl,
				CategoriesUrl = _categoryUrl,
				ThemesUrl = _themesUrl,
				PartsUrl = _partsUrl,
				SetsUrl = _setsUrl,
				InventoriesUrl = _inventoriesUrl,
				InventoryPartsUrl = _inventoryPartsUrl
			};

			CosmosDBSettings cosmosDBSettings = new()
			{
				Host = Settings.Host,
				Port = Settings.Port,
				EnableSSL = Settings.EnableSSL,
				AccountKey = Settings.PrimaryKey,
				DatabaseName = Settings.Database,
				ContainerName = Settings.Container
			};

			AzureStorageSettings azureStorageSettings = new()
			{
				AccountKey = Settings.StorageAccountKey,
				AccountName = Settings.StorageAccountName,
				Url = Settings.StorageUrl,
				RebrickableTableName = "Rebrickable"
			};

			const int overallSteps = 6;
			using ProgressBar progressBar = new(overallSteps, "Colors", MainProgressBarOptions());

			LoadData loadData = new(progressBar, rebrickableUrls, cosmosDBSettings, azureStorageSettings);
			await loadData.BulkImportAsync();
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


	}

}
