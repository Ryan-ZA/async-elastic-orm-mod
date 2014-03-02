package com.rc.gds;

import com.rc.gds.annotation.AlwaysPersist;
import com.rc.gds.annotation.ID;
import com.rc.gds.interfaces.GDSManual;

@AlwaysPersist
public class TestManual {

	@ID
	String id;
	
	String name;
	
	GDSManual manualChild;

}
