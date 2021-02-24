using Azure;
using Azure.Data.Tables;
using System;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.DataImport.TableEntities
{

	public class CategoryTableEntity : Category, ITableEntity
	{
		public string PartitionKey { get; set; }
		public string RowKey { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }

		public CategoryTableEntity() { }

		public CategoryTableEntity(Category category)
		{
			PartitionKey = "category";
			RowKey = category.RebrickableId;
			Id = category.Id;
			Name = category.Name;
			RebrickableId = category.RebrickableId;
		}

		public bool EqualsCategory(Category category)
		{
			if (
				category.Id != Id
				|| category.RebrickableId != RebrickableId
				|| category.Name != Name)
				return false;
			else
				return true;
		}

	}
}