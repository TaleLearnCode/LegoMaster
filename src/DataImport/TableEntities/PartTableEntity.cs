using Azure;
using Azure.Data.Tables;
using System;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.DataImport.TableEntities
{

	public class PartTableEntity : Part, ITableEntity
	{

		public string PartitionKey { get; set; }
		public string RowKey { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }

		public PartTableEntity() { }

		public PartTableEntity(Part part)
		{
			PartitionKey = "part";
			RowKey = part.PartNumber;
			Id = part.Id;
			PartNumber = part.PartNumber;
			Name = part.Name;
			CategoryId = part.CategoryId;
			CategoryName = part.CategoryName;
			PartMaterial = part.PartMaterial;
		}

		public bool EqualsPart(Part part)
		{
			if (
				part.Id != Id
				|| part.PartNumber != PartNumber
				|| part.Name != Name
				|| part.CategoryId != CategoryId
				|| part.CategoryName != CategoryName
				|| part.PartMaterial != PartMaterial)
				return false;
			else
				return true;
		}

	}
}