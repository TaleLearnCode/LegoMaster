namespace TaleLearnCode.LEGOMaster.Domain
{
	public class Part : IEntity
	{

		public string Id { get; set; }

		public string UserId { get; set; }

		public string Discriminator => Discriminators.Part;

		public string PartNumber { get; set; }

		public string Name { get; set; }

		public string CategoryId { get; set; }

		public string CategoryName { get; set; }

		public string PartMaterial { get; set; }

	}
}
