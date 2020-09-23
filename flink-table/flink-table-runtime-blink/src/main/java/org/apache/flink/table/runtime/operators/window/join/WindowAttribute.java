package org.apache.flink.table.runtime.operators.window.join;

/** Enumerations to indicate the output of window
 * attributes of the window join side(LHS and RHS). **/
public enum WindowAttribute {
	/** Does not output any window attributes. */
	NONE,
	/** Outputs the window_start. */
	START,
	/** Outputs the window_end. */
	END,
	/** Outputs the window_start and window_end. */
	START_END
}
