namespace TaleLearnCode.LEGOMaster.Domain
{
	public class Set
	{

		public string Id { get; set; }

		public string SetNumber { get; set; }

		public string Name { get; set; }

		public int Year { get; set; }

		public string ThemeId { get; set; }

		// TODO: Themes can be multi-tiered
		public string ThemeName { get; set; }

		public int PartCount { get; set; }

	}
}
