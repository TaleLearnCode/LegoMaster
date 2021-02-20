using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
{
	public class CategoryMapper : CsvMapping<Category>
	{

		public CategoryMapper()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
		}

	}
}
