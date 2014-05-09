/**
 * @author wlapka
 *
 * @created May 9, 2014 5:30:00 PM
 */
package net.thoiry.lapka.correlationidentifier;

/**
 * @author wlapka
 *
 */
public interface ReplyChannel<E, T> {
	
	T put (E messageId, T message);
	
	T remove(E messageId);
}
