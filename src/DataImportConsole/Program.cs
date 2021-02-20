using ShellProgressBar;
using System;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.DataImport;

namespace TaleLearnCode.LEGOMaster.DataImportConsole
{
	class Program
	{
		static async Task Main()
		{

			const int overallSteps = 6;
			using ProgressBar progressBar = new(overallSteps, "Colors", MainProgressBarOptions());

			LoadData loadData = new(progressBar);
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
