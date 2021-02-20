using System;
using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
{

	public class OldColorMapper : CsvMapping<Color>
	{

		public OldColorMapper() : base()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.RGB);
			MapProperty(3, x => x.IsTranslucent, new BoolConverter("1", "0", StringComparison.InvariantCulture));
		}

	}

	public class NewColorMapper : CsvMapping<Color>
	{

		public NewColorMapper() : base()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.RGB);
			MapProperty(3, x => x.IsTranslucent, new BoolConverter("t", "f", StringComparison.InvariantCulture));
		}

	}


}
