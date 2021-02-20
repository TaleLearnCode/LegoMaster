using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
{
	public class InventoryMapper : CsvMapping<Inventory>
	{

		public InventoryMapper()
		{
			MapProperty(0, x => x.Id);
			MapProperty(1, x => x.Version);
			MapProperty(2, x => x.SetNumber);
		}

	}
}