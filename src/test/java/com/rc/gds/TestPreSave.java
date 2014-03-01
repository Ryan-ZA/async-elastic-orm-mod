package com.rc.gds;

import com.rc.gds.annotation.PreSave;
import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSResult;

public class TestPreSave extends TestParent {
	
	public static TestChild staticTestChild;
	
	@PreSave
	GDSResult<?> preSave(GDS gds) {
		staticTestChild = new TestChild();
		return gds.save(staticTestChild);
	}
	
}
