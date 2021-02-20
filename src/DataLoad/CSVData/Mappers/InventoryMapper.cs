using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
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