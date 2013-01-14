package play.modules.export;

import play.PlayPlugin;


/**
 * @author Elhajdi
 * 
 */
public class ExportPlugin extends PlayPlugin {
    public void onApplicationStart() {
	System.out.println("---------------> export started");
    }
}
