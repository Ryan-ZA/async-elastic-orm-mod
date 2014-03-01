package com.rc.gds;

import com.rc.gds.annotation.PostSave;
import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSResult;

public class TestPostSave extends TestParent {
	
	public static TestChild staticTestChild;
	
	@PostSave
	GDSResult<?> preSave(GDS gds) {
		staticTestChild = new TestChild();
		return gds.save(staticTestChild);
	}
	
}
