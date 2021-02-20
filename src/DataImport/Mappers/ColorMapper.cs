using System;
using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace TaleLearnCode.LEGOMaster.DataImport
{

	public class ColorMapper : CsvMapping<Color>
	{

		public ColorMapper() : base()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.RGB);
			MapProperty(3, x => x.IsTranslucent, new BoolConverter("t", "f", StringComparison.InvariantCulture));
		}

	}


}
