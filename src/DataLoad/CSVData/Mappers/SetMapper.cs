using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
{
	public class SetMapper : CsvMapping<Set>
	{

		public SetMapper()
		{
			MapProperty(0, x => x.SetNumber);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.Year);
			MapProperty(3, x => x.ThemeId);
			MapProperty(4, x => x.PartCount);
		}

	}
}
