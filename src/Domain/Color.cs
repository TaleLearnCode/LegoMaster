namespace TaleLearnCode.LEGOMaster.Domain
{
	public class Color : IEntity
	{

		public string Id { get; set; }

		public string UserId { get; set; }

		public string Discriminator => Discriminators.Color;

		public string RebrickableId { get; set; }

		public string Name { get; set; }

		public string RGB { get; set; }

		public bool IsTranslucent { get; set; }

	}

}