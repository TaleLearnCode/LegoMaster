using Azure;
using Azure.Data.Tables;
using System;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.DataImport.TableEntities
{

	public class ColorTableEntity : Color, ITableEntity
	{

		public string PartitionKey { get; set; }
		public string RowKey { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }

		public ColorTableEntity() { }

		public ColorTableEntity(Color color)
		{
			PartitionKey = "color";
			RowKey = color.RebrickableId;
			Id = color.Id;
			Name = color.Name;
			RebrickableId = color.RebrickableId;
			RGB = color.RGB;
		}
	}
}