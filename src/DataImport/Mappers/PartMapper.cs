using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
{
	public class PartMapper : CsvMapping<Part>
	{

		public PartMapper()
		{
			MapProperty(0, x => x.PartNumber);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.CategoryId);
			MapProperty(3, x => x.PartMaterial);
		}

	}
}