/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package core.prepr;

import java.util.Comparator;

public class IncreaseFreqComparator implements Comparator<Selector>{

	public int compare(Selector sel1, Selector sel2) {
		return sel1.frequency - sel2.frequency;
	}

}
