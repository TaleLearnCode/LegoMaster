using DataLoad;
using System;
using System.Threading.Tasks;
using TaleLearnCode.LegoMaster.CSVData;

namespace DailyDataLoad
{

	class Program
	{
		static async Task Main(string[] args)
		{
			CosmosSettings cosmosSettings = new()
			{
				Host = Settings.Host,
				Port = Settings.Port,
				EnableSSL = Settings.EnableSSL,
				AccountKey = Settings.PrimaryKey,
				Database = Settings.Database,
				Container = Settings.Container
			};

			LoadData loadData = new(cosmosSettings);
			await loadData.LoadDailyUpdatesAsync(@"C:\Users\chadg\Downloads\Lego Database\", @"C:\Users\chadg\Downloads\New Lego Database\");


		}
	}

}