using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.Domain;
using TaleLearnCode.LEGOMaster.Infrastructure.Gremlin;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Services
{
	
	public class SearchService
	{

		GremlinHandler _gremlinHandler;

		public SearchService(GremlinHandler gremlinHandler)
		{
			_gremlinHandler = gremlinHandler;
		}

		public List<Part> SearchParts(string searchTerm)
		{
			string query = $"g.V().hasLabel('{Discriminators.Part}').has('userId', 'catalog').or(has('name', containing('{searchTerm}')),has('partNumber', containing('{searchTerm}')))";
			return _gremlinHandler.GetList<Part>(query);
		}

	}

}