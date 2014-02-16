package com.rc.gds;

import java.util.HashSet;
import java.util.Set;

public class ESMapCreator {
	
	GDSImpl gds;
	Set<String> createdIndexes = new HashSet<>();
	
	public ESMapCreator(GDSImpl gds) {
		this.gds = gds;
	}
	
	public void ensureIndexCreated(String index) {
		if (createdIndexes.add(index)) {
			gds.getClient().admin().indices().prepareCreate(index).execute();
		}
	}
	
}
