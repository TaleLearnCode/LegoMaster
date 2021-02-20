using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using TaleLearnCode.LegoMaster.CSVData.Mappers;
using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LegoMaster.CSVData
{

	public class LoadData
	{

		public readonly CosmosSettings _cosmosSettings;
		public string _existingDataFilePath;
		public string _newDataFilePath;

		public LoadData(CosmosSettings cosmosSettings)
		{
			_cosmosSettings = cosmosSettings;
		}

		public async Task LoadDailyUpdatesAsync(string existingDataFilePath, string newDataFilePath)
		{

			_existingDataFilePath = existingDataFilePath;
			_newDataFilePath = newDataFilePath;

			GremlinClient gremlinClient = GetGremlinClient();

			const int overallSteps = 6;
			using ProgressBar progressBar = new(overallSteps, "Colors", MainProgressBarOptions());

			Dictionary<string, Color> colors = await UpdateColorsAsync(gremlinClient, progressBar);

		}

		private async Task<Dictionary<string, Color>> UpdateColorsAsync(GremlinClient gremlinClient, ProgressBar mainProgressBar)
		{

			const int currentStepProcesses = 3;
			using ChildProgressBar progressBar = mainProgressBar.Spawn(currentStepProcesses, "Retrieving existing data...", StepProgressBarOptions());

			//ColorMapper csvMapper = new();

			List<CsvMappingResult<Color>> existingDataList = GetCSVData(new OldColorMapper(), $@"{_existingDataFilePath}colors.csv");
			Dictionary<string, CsvMappingResult<Color>> existingData = existingDataList.ToDictionary(x => x.Result.RebrickableId);
			progressBar.Tick("Retrieving new data...");
			List<CsvMappingResult<Color>> newData = GetCSVData(new NewColorMapper(), $@"{_newDataFilePath}colors.csv");

			Dictionary<string, Color> returnValue = new();
			if (newData.Any() && existingDataList.Any())
			{
				progressBar.Tick("Comparing new and old data");
				int counter = 1;
				using ChildProgressBar parseProgressBar = progressBar.Spawn(newData.Count, $"{counter} of {newData.Count()}", ProcessProgressBarOptions());
				foreach (var colorResult in newData)
				{
					string query = string.Empty;
					Color newColor = colorResult.Result;
					if (existingData.TryGetValue(newColor.RebrickableId, out CsvMappingResult<Color> existingColorResult))
					{
						Color existingColor = existingColorResult.Result;
						if (newColor.Name != existingColor.Name
							|| newColor.RGB != existingColor.RGB
							|| newColor.IsTranslucent != existingColor.IsTranslucent)
							query = $"g.V('{existingColor.Id}').property('userId', 'catalog').property('name', '{StringCleanup(newColor.Name)}').property('rgb', '{newColor.RGB}').property('isTranslucent', '{newColor.IsTranslucent}')";
						newColor.Id = existingColor.Id;
					}
					else
					{
						newColor.Id = Guid.NewGuid().ToString();
						query = $"g.AddV('color').property('userId', 'catalog').property('id', '{newColor.Id}').property('name', '{StringCleanup(newColor.Name)}').property('rgb', '{newColor.RGB}').property('isTranslucent', '{newColor.IsTranslucent}')";
					}
					if (!string.IsNullOrWhiteSpace(query))
						await gremlinClient.SubmitAsync<dynamic>(query);
					returnValue.Add(newColor.Id, newColor);
					counter++;
					parseProgressBar.Tick($"{counter} of {newData.Count}");
				}
			}

			return returnValue;

		}


		private GremlinClient GetGremlinClient()
		{

			string containerLink = $"/dbs/{_cosmosSettings.Database}/colls/{_cosmosSettings.Container}";
			var gremlinServer = new GremlinServer(_cosmosSettings.Host, _cosmosSettings.Port, enableSsl: _cosmosSettings.EnableSSL,
																							username: containerLink,
																							password: _cosmosSettings.AccountKey);

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

			return gremlinClient;

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

		private static CsvParserOptions GetCsvParserOptions()
		{
			return new CsvParserOptions(true, ',');
		}

		private List<CsvMappingResult<TEntity>> GetCSVData<TEntity>(ICsvMapping<TEntity> csvMapper, string csvFileName)
		{
			CsvParser<TEntity> csvParser = new(GetCsvParserOptions(), csvMapper);
			return csvParser
				.ReadFromFile(csvFileName, Encoding.ASCII)
				.ToList();
		}

		private static string StringCleanup(string input)
		{
			string output = input.Replace("\\", "\\\\");
			output = output.Replace("\'", "\\\'");
			return output;
		}


	}

}