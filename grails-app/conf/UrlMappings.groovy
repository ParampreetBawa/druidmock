class UrlMappings {

	static mappings = {
		"/$controller/$action?/$id?"{
			constraints {
				// apply constraints here
			}
		}

		"/druid/v2"(controller: "/druid/index")
		"500"(view:'/error')
	}
}
