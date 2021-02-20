using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
{
	public class ThemeMapper : CsvMapping<Theme>
	{

		public ThemeMapper()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.ParentId);
		}

	}
}