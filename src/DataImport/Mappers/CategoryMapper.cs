using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
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
