package org.apache.hyracks.dataflow.std.sjoin;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A simple interface that is called whenever a notifier needs to notify another
 * class.
 * @author Ahmed Eldawy
 *
 */
public interface Notifiable {
	/**
	 * Notify the calee that something happened. 
	 * @param notifier The caller object of this method
	 * @throws HyracksDataException 
	 */
	public void notify(Object notified) throws HyracksDataException;
}
